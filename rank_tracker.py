#!/usr/bin/env python3
"""Shopify bestseller rank tracker, alles in één bestand.

  python rank_tracker.py run        meten en rapport bouwen   <- de RUN knop
  python rank_tracker.py snapshot   alleen posities vastleggen
  python rank_tracker.py report     alleen het rapport opnieuw bouwen
  python rank_tracker.py stores     overzicht van wat gevolgd wordt
  python rank_tracker.py forget     vergeet welke producten al gemeld zijn
  python rank_tracker.py simulate   testdata om het dashboard te bekijken

Dit bestand is samengesteld uit de losse modules; pas liever die aan en draai
build_single.py opnieuw. Instellingen staan in config.yml (optioneel), stores
in stores.txt.
"""
from __future__ import annotations

import argparse
import base64
import csv
import gzip
import html
import html as htmllib
import io
import json
import math
import os
import random
import re
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

ROOT = Path(__file__).resolve().parent


# ==========================================================================
# config
# ==========================================================================

# Configuratie inlezen en store-lijst parsen.




DEFAULTS = {
    "max_products": 1000,
    "lookback_days": 3,
    "extra_lookbacks": [7, 14],
    "top_n": 5,
    "min_current_rank": 300,
    "min_climb": 10,
    "new_entrant_max_rank": 150,
    "concurrency": 4,
    "per_store_delay": 1.5,
    "timeout": 25,
    "retries": 3,
    "user_agent": "RankTracker/1.0",
    "keep_days": 240,
    "suppress_repeats": True,
    "repeat_cooldown_days": 0,
    "export_status": "draft",
    "export_price_multiplier": 1.0,
    "export_inventory_qty": 100,
    "export_handle_prefix": "",
    "my_store": "",
    "my_store_similarity": 0.8,
}


def scheme_for(domain: str) -> str:
    """http alleen voor de lokale testserver; alle echte stores draaien https."""
    return "http" if domain.startswith(("localhost", "127.0.0.1")) else "https"


@dataclass
class Store:
    domain: str
    label: str

    @property
    def base(self) -> str:
        return f"{scheme_for(self.domain)}://{self.domain}"


@dataclass
class Config:
    max_products: int = 1000
    lookback_days: int = 3
    extra_lookbacks: list = field(default_factory=lambda: [7, 14])
    top_n: int = 5
    min_current_rank: int = 300
    min_climb: int = 10
    new_entrant_max_rank: int = 150
    concurrency: int = 4
    per_store_delay: float = 1.5
    timeout: int = 25
    retries: int = 3
    user_agent: str = "RankTracker/1.0"
    keep_days: int = 240
    suppress_repeats: bool = True
    repeat_cooldown_days: int = 0
    export_status: str = "draft"
    export_price_multiplier: float = 1.0
    export_inventory_qty: int = 100
    export_handle_prefix: str = ""
    my_store: str = ""
    my_store_similarity: float = 0.8

    @classmethod
    def load(cls, path: Path | None = None) -> "Config":
        path = path or ROOT / "config.yml"
        data = dict(DEFAULTS)
        if path.exists():
            loaded = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
            data.update({k: v for k, v in loaded.items() if v is not None})
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in data.items() if k in known})


_DOMAIN_RE = re.compile(r"^(?:https?://)?(?:www\.)?([^/\s|]+)")


def normalise_domain(raw: str) -> str:
    m = _DOMAIN_RE.match(raw.strip())
    return m.group(1).lower().rstrip("/") if m else raw.strip().lower()


def load_stores(path: Path | None = None) -> list[Store]:
    path = path or ROOT / "stores.txt"
    stores: list[Store] = []
    seen: set[str] = set()
    if not path.exists():
        return stores
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line:
            raw, label = line.split("|", 1)
            label = label.strip()
        else:
            raw, label = line, ""
        domain = normalise_domain(raw)
        if not domain or domain in seen:
            continue
        seen.add(domain)
        stores.append(Store(domain=domain, label=label or domain))
    return stores


# ==========================================================================
# db
# ==========================================================================

# SQLite opslag voor dagelijkse rank-snapshots.



DB_PATH = ROOT / "data" / "tracker.db"

SCHEMA = """
PRAGMA journal_mode = DELETE;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS stores (
    id        INTEGER PRIMARY KEY,
    domain    TEXT UNIQUE NOT NULL,
    label     TEXT,
    strategy  TEXT,               -- 'json' of 'html'
    page_size INTEGER,            -- producten per HTML-pagina
    checked_at TEXT               -- wanneer de strategie voor het laatst gevalideerd is
);

CREATE TABLE IF NOT EXISTS products (
    store_id   INTEGER NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    handle     TEXT NOT NULL,
    title      TEXT,
    price      REAL,
    currency   TEXT,
    image      TEXT,
    vendor     TEXT,
    product_id TEXT,
    created_at TEXT,              -- publicatiedatum bij de store
    first_seen TEXT,
    last_seen  TEXT,
    PRIMARY KEY (store_id, handle)
);

CREATE TABLE IF NOT EXISTS snapshots (
    id        INTEGER PRIMARY KEY,
    store_id  INTEGER NOT NULL REFERENCES stores(id) ON DELETE CASCADE,
    day       TEXT NOT NULL,      -- YYYY-MM-DD
    taken_at  TEXT NOT NULL,      -- volledige timestamp
    total     INTEGER NOT NULL,   -- aantal producten in deze snapshot
    strategy  TEXT,
    UNIQUE (store_id, day)
);

CREATE TABLE IF NOT EXISTS ranks (
    snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
    handle      TEXT NOT NULL,
    rank        INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, handle)
);

CREATE INDEX IF NOT EXISTS idx_ranks_handle ON ranks(handle);
CREATE INDEX IF NOT EXISTS idx_snapshots_store_day ON snapshots(store_id, day);

CREATE TABLE IF NOT EXISTS runs (
    id       INTEGER PRIMARY KEY,
    started  TEXT,
    finished TEXT,
    ok       INTEGER,
    failed   INTEGER,
    note     TEXT
);
"""


def connect(path: Path | None = None) -> sqlite3.Connection:
    path = Path(path or DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.executescript(SCHEMA)
    return con


@contextmanager
def session(path: Path | None = None):
    con = connect(path)
    try:
        yield con
        con.commit()
    finally:
        con.close()


def upsert_store(con: sqlite3.Connection, domain: str, label: str) -> int:
    con.execute(
        "INSERT INTO stores (domain, label) VALUES (?, ?) "
        "ON CONFLICT(domain) DO UPDATE SET label = excluded.label",
        (domain, label),
    )
    return con.execute("SELECT id FROM stores WHERE domain = ?", (domain,)).fetchone()["id"]


def get_strategy(con: sqlite3.Connection, store_id: int) -> tuple[str | None, int | None, str | None]:
    row = con.execute(
        "SELECT strategy, page_size, checked_at FROM stores WHERE id = ?", (store_id,)
    ).fetchone()
    if not row:
        return None, None, None
    return row["strategy"], row["page_size"], row["checked_at"]


def save_strategy(con: sqlite3.Connection, store_id: int, strategy: str,
                  page_size: int | None, checked_at: str) -> None:
    con.execute(
        "UPDATE stores SET strategy = ?, page_size = ?, checked_at = ? WHERE id = ?",
        (strategy, page_size, checked_at, store_id),
    )


def upsert_products(con: sqlite3.Connection, store_id: int, catalog: dict, today: str) -> None:
    rows = []
    for handle, meta in catalog.items():
        rows.append((
            store_id, handle, meta.get("title"), meta.get("price"), meta.get("currency"),
            meta.get("image"), meta.get("vendor"), str(meta.get("product_id") or ""),
            meta.get("created_at"), today, today,
        ))
    con.executemany(
        """
        INSERT INTO products
            (store_id, handle, title, price, currency, image, vendor, product_id,
             created_at, first_seen, last_seen)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(store_id, handle) DO UPDATE SET
            title      = COALESCE(excluded.title, products.title),
            price      = COALESCE(excluded.price, products.price),
            currency   = COALESCE(excluded.currency, products.currency),
            image      = COALESCE(excluded.image, products.image),
            vendor     = COALESCE(excluded.vendor, products.vendor),
            product_id = COALESCE(NULLIF(excluded.product_id,''), products.product_id),
            created_at = COALESCE(excluded.created_at, products.created_at),
            last_seen  = excluded.last_seen
        """,
        rows,
    )


def save_snapshot(con: sqlite3.Connection, store_id: int, day: str, taken_at: str,
                  handles: list[str], strategy: str) -> int:
    """Schrijft de ranking van vandaag weg. Draai je twee keer op een dag,
    dan overschrijft de nieuwste run de oude (idempotent)."""
    con.execute("DELETE FROM snapshots WHERE store_id = ? AND day = ?", (store_id, day))
    cur = con.execute(
        "INSERT INTO snapshots (store_id, day, taken_at, total, strategy) VALUES (?,?,?,?,?)",
        (store_id, day, taken_at, len(handles), strategy),
    )
    snap_id = cur.lastrowid
    con.executemany(
        "INSERT OR REPLACE INTO ranks (snapshot_id, handle, rank) VALUES (?,?,?)",
        [(snap_id, h, i + 1) for i, h in enumerate(handles)],
    )
    return snap_id


def snapshot_days(con: sqlite3.Connection, store_id: int) -> list[str]:
    return [r["day"] for r in con.execute(
        "SELECT day FROM snapshots WHERE store_id = ? ORDER BY day", (store_id,))]


def ranking_for_day(con: sqlite3.Connection, store_id: int, day: str) -> dict[str, int]:
    rows = con.execute(
        "SELECT r.handle, r.rank FROM ranks r "
        "JOIN snapshots s ON s.id = r.snapshot_id "
        "WHERE s.store_id = ? AND s.day = ?",
        (store_id, day),
    )
    return {r["handle"]: r["rank"] for r in rows}


def snapshot_total(con: sqlite3.Connection, store_id: int, day: str) -> int:
    row = con.execute(
        "SELECT total FROM snapshots WHERE store_id = ? AND day = ?", (store_id, day)
    ).fetchone()
    return row["total"] if row else 0


def product_meta(con: sqlite3.Connection, store_id: int) -> dict[str, dict]:
    rows = con.execute("SELECT * FROM products WHERE store_id = ?", (store_id,))
    return {r["handle"]: dict(r) for r in rows}


def all_stores(con: sqlite3.Connection) -> list[dict]:
    return [dict(r) for r in con.execute("SELECT * FROM stores ORDER BY label")]


def today_str() -> str:
    return date.today().isoformat()


# ==========================================================================
# storage
# ==========================================================================

# Snapshots op schijf: de bestanden zijn de bron van waarheid, de SQLite
# database is alleen een werkindex die elke run opnieuw wordt opgebouwd.
# 
# Waarom: een database-bestand verandert elke dag helemaal en zou de git-repo
# laten ontploffen. Losse gzip-bestandjes per store per dag zijn klein
# (ongeveer 8 kB voor 1000 producten) en worden nooit meer aangeraakt.
# 
#     data/snapshots/<domein>/2026-08-21.csv.gz   handle,rank
#     data/catalog/<domein>.json                  titel, prijs, foto per handle
# 



SNAP_DIR = ROOT / "data" / "snapshots"
CAT_DIR = ROOT / "data" / "catalog"
DAY_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\.csv\.gz$")


def _safe(domain: str) -> str:
    return re.sub(r"[^a-z0-9.\-_]", "_", domain.lower())


# ------------------------------------------------------------------ schrijven
def write_snapshot(domain: str, day: str, handles: list[str],
                   strategy: str, taken_at: str) -> Path:
    d = SNAP_DIR / _safe(domain)
    d.mkdir(parents=True, exist_ok=True)
    path = d / f"{day}.csv.gz"
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["# strategy", strategy, "taken_at", taken_at])
    w.writerow(["handle", "rank"])
    for i, h in enumerate(handles, 1):
        w.writerow([h, i])
    path.write_bytes(gzip.compress(buf.getvalue().encode("utf-8"), mtime=0))
    return path


def merge_catalog(domain: str, catalog: dict, day: str) -> Path:
    CAT_DIR.mkdir(parents=True, exist_ok=True)
    path = CAT_DIR / f"{_safe(domain)}.json"
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except ValueError:
            existing = {}
    for handle, meta in catalog.items():
        prev = existing.get(handle, {})
        merged = {**prev, **{k: v for k, v in meta.items() if v is not None}}
        merged.setdefault("first_seen", day)
        merged["last_seen"] = day
        existing[handle] = merged
    path.write_text(json.dumps(existing, ensure_ascii=False, sort_keys=True, indent=0),
                    encoding="utf-8")
    return path


def write_state(domain: str, strategy: str, page_size: int | None, checked_at: str) -> None:
    CAT_DIR.mkdir(parents=True, exist_ok=True)
    path = CAT_DIR / f"{_safe(domain)}.state.json"
    path.write_text(json.dumps(
        {"strategy": strategy, "page_size": page_size, "checked_at": checked_at}),
        encoding="utf-8")


def read_state(domain: str) -> dict:
    path = CAT_DIR / f"{_safe(domain)}.state.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return {}


# --------------------------------------------------------------------- lezen
def snapshot_files(domain: str) -> list[tuple[str, Path]]:
    d = SNAP_DIR / _safe(domain)
    if not d.exists():
        return []
    out = []
    for f in d.iterdir():
        m = DAY_RE.match(f.name)
        if m:
            out.append((m.group(1), f))
    return sorted(out)


def read_snapshot(path: Path) -> tuple[list[str], str]:
    text = gzip.decompress(path.read_bytes()).decode("utf-8")
    rows = list(csv.reader(io.StringIO(text)))
    strategy = ""
    handles: list[tuple[int, str]] = []
    for row in rows:
        if not row:
            continue
        if row[0] == "# strategy":
            strategy = row[1] if len(row) > 1 else ""
            continue
        if row[0] == "handle":
            continue
        try:
            handles.append((int(row[1]), row[0]))
        except (IndexError, ValueError):
            continue
    handles.sort()
    return [h for _, h in handles], strategy


def read_catalog(domain: str) -> dict:
    path = CAT_DIR / f"{_safe(domain)}.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except ValueError:
        return {}


def prune(domain: str, keep_days: int) -> int:
    if keep_days <= 0:
        return 0
    cutoff = (date.today() - timedelta(days=keep_days)).isoformat()
    removed = 0
    for day, path in snapshot_files(domain):
        if day < cutoff:
            path.unlink()
            removed += 1
    return removed


# ------------------------------------------------------- index (her)opbouwen
def rebuild_index(con, stores) -> None:
    """Leest alle snapshotbestanden in een verse SQLite index."""

    # ook de storetabel legen: haal je een store uit stores.txt, dan hoort hij
    # niet meer in het rapport op te duiken
    con.executescript("DELETE FROM ranks; DELETE FROM snapshots; "
                      "DELETE FROM products; DELETE FROM stores;")
    for st in stores:
        sid = db.upsert_store(con, st.domain, st.label)
        catalog = read_catalog(st.domain)
        if catalog:
            db.upsert_products(con, sid, catalog, date.today().isoformat())
        for day, path in snapshot_files(st.domain):
            handles, strategy = read_snapshot(path)
            if handles:
                db.save_snapshot(con, sid, day, f"{day}T00:00:00", handles, strategy)
        state = read_state(st.domain)
        if state.get("strategy"):
            db.save_strategy(con, sid, state["strategy"], state.get("page_size"),
                             state.get("checked_at") or "")


# ----------------------------------------------------- al gerapporteerd
REPORTED = ROOT / "data" / "reported.json"


def read_reported() -> dict:
    """{domein: {handle: {"first_reported": "YYYY-MM-DD", "title": ...}}}"""
    if not REPORTED.exists():
        return {}
    try:
        return json.loads(REPORTED.read_text(encoding="utf-8"))
    except ValueError:
        return {}


def write_reported(data: dict) -> None:
    REPORTED.parent.mkdir(parents=True, exist_ok=True)
    REPORTED.write_text(json.dumps(data, ensure_ascii=False, sort_keys=True, indent=1),
                        encoding="utf-8")


def suppressed_handles(domain: str, day: str, cooldown_days: int = 0) -> set[str]:
    """Handles die al eerder gerapporteerd zijn en dus niet opnieuw mogen.

    Rapporten van vandaag tellen niet mee, zodat opnieuw draaien op dezelfde
    dag exact dezelfde vijf producten oplevert in plaats van vijf nieuwe.
    """
    entries = read_reported().get(domain, {})
    out = set()
    for handle, info in entries.items():
        first = info.get("first_reported", "")
        if not first or first >= day:
            continue
        if cooldown_days > 0:
            try:
                age = (datetime.strptime(day, "%Y-%m-%d").date()
                       - datetime.strptime(first, "%Y-%m-%d").date()).days
            except ValueError:
                age = 0
            if age >= cooldown_days:
                continue
        out.add(handle)
    return out


def record_reported(domain: str, day: str, items: list[dict]) -> int:
    data = read_reported()
    entries = data.setdefault(domain, {})
    # eerdere registraties van vandaag weggooien: een herhaalde run op
    # dezelfde dag moet hetzelfde resultaat geven, niet stapelen
    for handle in [h for h, v in entries.items() if v.get("first_reported") == day]:
        entries.pop(handle, None)
    added = 0
    for it in items:
        h = it.get("handle")
        if not h or h in entries:
            continue
        entries[h] = {"first_reported": day, "title": it.get("title"),
                      "rank": it.get("rank_now"), "url": it.get("url")}
        added += 1
    write_reported(data)
    return added


def forget_reported(domain: str | None = None) -> None:
    """Wist de geschiedenis, zodat producten weer mogen terugkomen."""
    data = read_reported()
    if domain:
        data.pop(domain, None)
    else:
        data = {}
    write_reported(data)


# ==========================================================================
# fetch
# ==========================================================================

# Haalt de best-selling volgorde van een Shopify store op.
# 
# Twee strategieen:
# 
#   json  -> /collections/all/products.json?sort_by=best-selling&limit=250&page=N
#            Snel: 4 requests voor 1000 producten, inclusief titel/prijs/foto.
# 
#   html  -> /collections/all?sort_by=best-selling&page=N
#            Fallback voor stores waar de JSON-endpoint de sortering negeert.
#            Metadata wordt dan alsnog uit de (ongesorteerde) products.json gehaald.
# 
# Welke van de twee klopt, verschilt per store en per theme. Daarom valideert
# het script dat zelf: hij vergelijkt de eerste producten uit de JSON met de
# eerste producten uit de HTML en kiest de methode die daadwerkelijk
# best-selling teruggeeft. Het resultaat wordt in de database onthouden en
# elke 7 dagen opnieuw gecontroleerd.
# 




PRODUCT_HREF_RE = re.compile(r'href="([^"]*?/products/[^"?#]+)', re.I)
JSON_LIMIT = 250


@dataclass
class FetchResult:
    domain: str
    handles: list[str] = field(default_factory=list)
    catalog: dict = field(default_factory=dict)
    strategy: str = ""
    page_size: int | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None and len(self.handles) > 0


class StoreClient:
    def __init__(self, store: Store, cfg: Config):
        self.store = store
        self.cfg = cfg
        self.base = store.base
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": cfg.user_agent,
            "Accept-Language": "de,en;q=0.8,nl;q=0.6",
        })
        self._last_request = 0.0

    # -- laag niveau ------------------------------------------------------
    def _sleep(self) -> None:
        wait = self.cfg.per_store_delay - (time.time() - self._last_request)
        if wait > 0:
            time.sleep(wait)

    def get(self, path: str, **params) -> requests.Response:
        url = urljoin(self.base + "/", path.lstrip("/"))
        last_exc = None
        for attempt in range(self.cfg.retries):
            self._sleep()
            try:
                resp = self.session.get(url, params=params or None,
                                        timeout=self.cfg.timeout, allow_redirects=True)
                self._last_request = time.time()
                if resp.status_code == 429 or resp.status_code >= 500:
                    time.sleep(2 ** attempt * 2)
                    last_exc = RuntimeError(f"HTTP {resp.status_code} op {url}")
                    continue
                resp.raise_for_status()
                # store kan naar een ander domein redirecten (rebrand, locale)
                final = urlparse(resp.url)
                if final.netloc and final.netloc not in self.base:
                    self.base = f"{final.scheme}://{final.netloc}"
                return resp
            except requests.RequestException as exc:  # noqa: PERF203
                last_exc = exc
                self._last_request = time.time()
                time.sleep(2 ** attempt)
        raise last_exc or RuntimeError(f"kon {url} niet ophalen")

    # -- JSON -------------------------------------------------------------
    def json_page(self, page: int, sorted_by_bestselling: bool = True) -> list[dict]:
        params = {"limit": JSON_LIMIT, "page": page}
        if sorted_by_bestselling:
            params["sort_by"] = "best-selling"
        resp = self.get("/collections/all/products.json", **params)
        try:
            return resp.json().get("products", []) or []
        except ValueError:
            return []

    def json_catalog(self, sorted_by_bestselling: bool) -> tuple[list[str], dict]:
        """Loopt door de products.json pagina's. Geeft (volgorde, metadata)."""
        handles: list[str] = []
        catalog: dict = {}
        pages = max(1, -(-self.cfg.max_products // JSON_LIMIT))
        for page in range(1, pages + 1):
            products = self.json_page(page, sorted_by_bestselling)
            if not products:
                break
            for p in products:
                handle = p.get("handle")
                if not handle or handle in catalog:
                    continue
                handles.append(handle)
                catalog[handle] = _meta_from_json(p)
            if len(products) < JSON_LIMIT:
                break
            if len(handles) >= self.cfg.max_products:
                break
        return handles[: self.cfg.max_products], catalog

    # -- HTML -------------------------------------------------------------
    def html_page(self, page: int) -> list[str]:
        resp = self.get("/collections/all", sort_by="best-selling", page=page)
        return _handles_from_html(resp.text)

    def html_ranking(self) -> tuple[list[str], int]:
        handles: list[str] = []
        seen: set[str] = set()
        page_size = 0
        page = 1
        max_pages = 120
        while page <= max_pages and len(handles) < self.cfg.max_products:
            found = self.html_page(page)
            if page == 1:
                page_size = len(found)
            fresh = [h for h in found if h not in seen]
            if not fresh:
                break
            for h in fresh:
                seen.add(h)
                handles.append(h)
            if len(found) < max(1, page_size):
                break
            page += 1
        return handles[: self.cfg.max_products], page_size or len(handles)


def _meta_from_json(p: dict) -> dict:
    variants = p.get("variants") or []
    prices = []
    for v in variants:
        try:
            prices.append(float(v.get("price")))
        except (TypeError, ValueError):
            continue
    images = p.get("images") or []
    image = None
    if images:
        image = images[0].get("src")
    elif p.get("featured_image"):
        image = p["featured_image"]
    return {
        "title": p.get("title"),
        "price": min(prices) if prices else None,
        "currency": None,
        "image": image,
        "vendor": p.get("vendor"),
        "product_id": p.get("id"),
        "created_at": (p.get("published_at") or p.get("created_at") or "")[:10] or None,
        "available": any(v.get("available") for v in variants) if variants else None,
    }


def _handles_from_html(text: str) -> list[str]:
    """Haalt producthandles in DOM-volgorde uit een collectiepagina.

    Alle /products/<handle> links worden gepakt, ontdubbeld met behoud van
    volgorde. Links uit menus of 'recently viewed' blokken zijn zeldzaam op
    collectiepagina's en vallen weg door de ontdubbeling zodra ze ook in het
    grid staan.
    """
    handles: list[str] = []
    seen: set[str] = set()
    for raw in PRODUCT_HREF_RE.findall(text):
        href = htmllib.unescape(raw)
        handle = href.rsplit("/products/", 1)[-1].strip("/")
        if not handle or handle in seen:
            continue
        seen.add(handle)
        handles.append(handle)
    return handles


def order_similarity(a: list[str], b: list[str], depth: int = 12) -> float:
    """Hoeveel van de eerste `depth` items staan op dezelfde plek."""
    a, b = a[:depth], b[:depth]
    if not a or not b:
        return 0.0
    n = min(len(a), len(b))
    return sum(1 for i in range(n) if a[i] == b[i]) / n


def detect_strategy(client: StoreClient) -> tuple[str, int | None]:
    """Bepaalt of products.json de best-selling sortering respecteert."""
    html_first = client.html_page(1)
    json_first = [p.get("handle") for p in client.json_page(1, True) if p.get("handle")]
    if not html_first:
        # geen bruikbare HTML om tegen te ijken: vertrouw op JSON
        return ("json", None)
    if order_similarity(json_first, html_first) >= 0.7:
        return ("json", len(html_first))
    return ("html", len(html_first))


def fetch_store(store: Store, cfg: Config, strategy: str | None = None) -> FetchResult:
    client = StoreClient(store, cfg)
    result = FetchResult(domain=store.domain)
    try:
        page_size = None
        if strategy not in ("json", "html"):
            strategy, page_size = detect_strategy(client)
        result.strategy = strategy
        result.page_size = page_size

        if strategy == "json":
            handles, catalog = client.json_catalog(sorted_by_bestselling=True)
            result.handles = handles
            result.catalog = catalog
        else:
            handles, page_size = client.html_ranking()
            result.handles = handles
            result.page_size = page_size
            # metadata los ophalen (volgorde doet er hier niet toe)
            _, catalog = client.json_catalog(sorted_by_bestselling=False)
            result.catalog = catalog

        if not result.handles:
            result.error = "geen producten gevonden"
    except Exception as exc:  # noqa: BLE001
        result.error = f"{type(exc).__name__}: {exc}"
    return result


# ==========================================================================
# lists
# ==========================================================================

# Handmatige uitsluitlijsten.
# 
#   uploaded.txt    producten die je al geupload hebt of bewust niet wilt
#   my_products.txt titels of handles die al in je eigen store staan
# 
# Beide bestanden zijn gewone tekstbestanden. Eén regel per product. Een regel
# mag een handle zijn, een volledige product-URL, of een titel. Met een streepje
# beperk je hem tot één store:
# 
#     redlich-becker.de | dit-product-handle
#     zomaar-een-handle
#     https://drune.de/products/nog-een-product
#     Orthopädische Sandalen
# 



UPLOADED = ROOT / "uploaded.txt"
MINE = ROOT / "my_products.txt"


def _norm_title(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def parse(path: Path) -> tuple[dict[str, set[str]], set[str], set[str]]:
    """Geeft (per store handles, handles voor alle stores, genormaliseerde titels)."""
    per_store: dict[str, set[str]] = {}
    global_handles: set[str] = set()
    titles: set[str] = set()
    if not path.exists():
        return per_store, global_handles, titles

    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        domain = None
        if "|" in line:
            left, line = line.split("|", 1)
            domain = left.strip().lower().replace("https://", "").replace("http://", "").strip("/")
            line = line.strip()
        if "/products/" in line:
            m = re.search(r"https?://([^/]+)/products/([^/?#]+)", line)
            if m:
                domain = domain or m.group(1).lower()
                line = m.group(2)
        looks_like_handle = bool(re.fullmatch(r"[a-z0-9][a-z0-9\-_%]*", line))
        if looks_like_handle:
            if domain:
                per_store.setdefault(domain, set()).add(line)
            else:
                global_handles.add(line)
        else:
            titles.add(_norm_title(line))
    return per_store, global_handles, titles


def load_manual_exclusions() -> tuple[dict[str, set[str]], set[str], set[str]]:
    per_store: dict[str, set[str]] = {}
    handles: set[str] = set()
    titles: set[str] = set()
    for path in (UPLOADED, MINE):
        ps, gh, ti = parse(path)
        for d, hs in ps.items():
            per_store.setdefault(d, set()).update(hs)
        handles |= gh
        titles |= ti
    return per_store, handles, titles


# ------------------------------------------------------- eigen catalogus
def _tokens(text: str) -> set[str]:
    words = re.findall(r"[a-z0-9]+", (text or "").lower())
    return {w for w in words if len(w) > 2}


def similar(a: str, b_tokens: set[str], threshold: float) -> bool:
    a_tokens = _tokens(a)
    if not a_tokens or not b_tokens:
        return False
    overlap = len(a_tokens & b_tokens)
    return overlap / min(len(a_tokens), len(b_tokens)) >= threshold


def fetch_own_catalog(domain: str, cfg) -> list[str]:
    """Titels van je eigen store, zodat je niets krijgt aangeraden wat je al hebt.

    Gebruikt dezelfde publieke catalogus-endpoint als voor de andere stores,
    dus zonder inloggegevens en altijd actueel.
    """


    titles: list[str] = []
    base = f"{scheme_for(domain)}://{domain}/collections/all/products.json"
    for page in range(1, 21):
        try:
            r = requests.get(base, params={"limit": 250, "page": page},
                             timeout=cfg.timeout, headers={"User-Agent": cfg.user_agent})
            r.raise_for_status()
            products = (r.json() or {}).get("products") or []
        except Exception:  # noqa: BLE001
            break
        if not products:
            break
        titles.extend(p.get("title") or "" for p in products)
        if len(products) < 250:
            break
    return [t for t in titles if t]


# ==========================================================================
# analyze
# ==========================================================================

# Vergelijkt snapshots en bepaalt de hardste stijgers per store.




def _norm(text) -> str:
    """Titel normaliseren zodat kleine verschillen in spelling niet uitmaken."""
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def _matches_own(title, own_titles: list[set], threshold: float) -> bool:

    return any(similar(title, tokens, threshold) for tokens in own_titles)


def _d(day: str) -> date:
    return datetime.strptime(day, "%Y-%m-%d").date()


def pick_baseline(days: list[str], current: str, lookback: int) -> str | None:
    """Kies de snapshotdag die het dichtst bij `current - lookback` ligt.

    Voorkeur voor een dag die minstens `lookback` dagen terug ligt. Is die er
    niet (nog te weinig historie), dan de oudste dag die we hebben, zodat het
    rapport ook in week 1 al iets zinnigs laat zien.
    """
    others = [d for d in days if d < current]
    if not others:
        return None
    target = _d(current) - timedelta(days=lookback)
    at_or_before = [d for d in others if _d(d) <= target]
    if at_or_before:
        return max(at_or_before)
    return min(others)


def rank_history(con: sqlite3.Connection, store_id: int, handle: str,
                 days: list[str], current: str, span: int = 10) -> list[tuple[str, int | None]]:
    """Rank per snapshotdag voor dit product, oudste eerst. None = stond er niet in."""
    ordered = [d for d in days if d <= current][-span:]
    out: list[tuple[str, int | None]] = []
    for d in ordered:
        row = con.execute(
            "SELECT r.rank FROM ranks r JOIN snapshots s ON s.id = r.snapshot_id "
            "WHERE s.store_id = ? AND s.day = ? AND r.handle = ?",
            (store_id, d, handle),
        ).fetchone()
        out.append((d, row["rank"] if row else None))
    return out


def climb_streak(history: list[tuple[str, int | None]]) -> int:
    """Aantal opeenvolgende snapshots waarin dit product omhoog ging."""
    ranks = [r for _, r in history]
    streak = 0
    for i in range(len(ranks) - 1, 0, -1):
        a, b = ranks[i - 1], ranks[i]
        if a is None or b is None or b >= a:
            break
        streak += 1
    return streak


def analyse_store(con: sqlite3.Connection, store: dict, cfg: Config,
                  current: str | None = None, exclude: set[str] | None = None,
                  exclude_titles: set[str] | None = None,
                  own_titles: list[set] | None = None,
                  similarity: float = 0.0) -> dict:
    store_id = store["id"]
    exclude = exclude or set()
    exclude_titles = exclude_titles or set()
    own_titles = own_titles or []
    days = db.snapshot_days(con, store_id)
    out = {
        "store_id": store_id,
        "domain": store["domain"],
        "label": store["label"] or store["domain"],
        "days_tracked": len(days),
        "current_day": None,
        "baseline_day": None,
        "baseline_gap": None,
        "total_products": 0,
        "risers": [],
        "runners_up": [],
        "new_entrants": [],
        "skipped_known": 0,
        "note": None,
    }
    if not days:
        out["note"] = "nog geen snapshots"
        return out

    current = current or days[-1]
    out["current_day"] = current
    now = db.ranking_for_day(con, store_id, current)
    out["total_products"] = len(now)
    meta = db.product_meta(con, store_id)

    baseline = pick_baseline(days, current, cfg.lookback_days)
    if baseline is None:
        out["note"] = ("eerste meting: er is nog geen vorige dag om mee te "
                       "vergelijken. Vanaf de tweede run verschijnen hier stijgers.")
        return out
    out["baseline_day"] = baseline
    out["baseline_gap"] = (_d(current) - _d(baseline)).days
    before = db.ranking_for_day(con, store_id, baseline)
    before_total = db.snapshot_total(con, store_id, baseline) or len(before)

    extra = {}
    for lb in cfg.extra_lookbacks:
        d = pick_baseline(days, current, lb)
        if d and d != baseline:
            extra[lb] = (d, db.ranking_for_day(con, store_id, d))

    candidates = []
    skipped = 0
    for handle, new_rank in now.items():
        old_rank = before.get(handle)
        if old_rank is None:
            continue
        delta = old_rank - new_rank
        if delta < cfg.min_climb or new_rank > cfg.min_current_rank:
            continue
        title = meta.get(handle, {}).get("title")
        if handle in exclude or _norm(title) in exclude_titles:
            # al eerder aan je gemeld of al geupload: nooit twee keer
            skipped += 1
            continue
        if own_titles and _matches_own(title, own_titles, similarity):
            # lijkt te sterk op iets dat al in je eigen store staat
            skipped += 1
            continue
        m = meta.get(handle, {})
        hist = rank_history(con, store_id, handle, days, current)
        row = {
            "handle": handle,
            "title": m.get("title") or handle.replace("-", " ").title(),
            "url": f"https://{store['domain']}/products/{handle}",
            "image": m.get("image"),
            "price": m.get("price"),
            "vendor": m.get("vendor"),
            "created_at": m.get("created_at"),
            "rank_now": new_rank,
            "rank_before": old_rank,
            "delta": delta,
            "pct": round(delta / old_rank * 100, 1) if old_rank else None,
            # zwaartepunt naar de top: 40 plekken winnen op plek 20 telt zwaarder
            # dan 40 plekken winnen op plek 500
            "weighted": round(delta / math.sqrt(new_rank), 2),
            "streak": climb_streak(hist),
            "spark": [r for _, r in hist],
            "spark_days": [d for d, _ in hist],
            "history": {},
        }
        for lb, (d, ranking) in extra.items():
            prev = ranking.get(handle)
            row["history"][lb] = None if prev is None else prev - new_rank
        candidates.append(row)

    out["skipped_known"] = skipped
    candidates.sort(key=lambda r: (-r["delta"], r["rank_now"]))
    out["risers"] = candidates[: cfg.top_n]
    out["runners_up"] = candidates[cfg.top_n : cfg.top_n + 15]

    entrants = []
    for handle, new_rank in now.items():
        if handle in before or new_rank > cfg.new_entrant_max_rank or handle in exclude:
            continue
        if _norm(meta.get(handle, {}).get("title")) in exclude_titles:
            continue
        m = meta.get(handle, {})
        entrants.append({
            "handle": handle,
            "title": m.get("title") or handle.replace("-", " ").title(),
            "url": f"https://{store['domain']}/products/{handle}",
            "image": m.get("image"),
            "price": m.get("price"),
            "rank_now": new_rank,
            "rank_before": None,
            "delta": None,
            "created_at": m.get("created_at"),
            "was_in_catalog": handle in meta,
            "baseline_total": before_total,
        })
    entrants.sort(key=lambda r: r["rank_now"])
    out["new_entrants"] = entrants[: cfg.top_n]

    if not out["risers"]:
        out["note"] = (f"geen product voldeed aan de drempel (minstens "
                       f"{cfg.min_climb} plekken gestegen en nu binnen de "
                       f"top {cfg.min_current_rank}).")
    return out


def analyse_all(con: sqlite3.Connection, cfg: Config,
                excludes: dict[str, set[str]] | None = None,
                exclude_titles: set[str] | None = None,
                own_titles: list[set] | None = None) -> list[dict]:
    excludes = excludes or {}
    return [analyse_store(con, s, cfg, exclude=excludes.get(s["domain"]),
                          exclude_titles=exclude_titles, own_titles=own_titles,
                          similarity=cfg.my_store_similarity)
            for s in db.all_stores(con)]


# ==========================================================================
# report
# ==========================================================================

# Bouwt het HTML dashboard en de CSV export.



REPORTS = ROOT / "reports"

CSV_COLUMNS = [
    "run_date", "store", "domain", "type", "title", "handle", "url",
    "rank_now", "rank_before", "delta", "pct_gain", "weighted_score",
    "climb_streak_days", "price", "vendor", "product_created", "image",
]


# ---------------------------------------------------------------- helpers
def _e(v) -> str:
    return html.escape(str(v)) if v is not None else ""


def _money(v, currency: str | None = None) -> str:
    if v is None:
        return "n.v.t."
    sym = {"EUR": "€", "USD": "$", "GBP": "£"}.get(currency or "", "€")
    return f"{sym}{v:,.2f}".replace(",", " ")


def sparkline(ranks: list[int | None], width: int = 132, height: int = 34) -> str:
    """Eén serie: rangverloop over de laatste snapshots.

    De y-as is omgekeerd, want een lager rangnummer is beter. De lijn loopt dus
    omhoog als het product stijgt. Geen legenda nodig bij één serie; het
    eindpunt krijgt een marker en het getal staat naast de grafiek.
    """
    pts = [(i, r) for i, r in enumerate(ranks) if r is not None]
    if len(pts) < 2:
        return ('<svg class="spark" width="%d" height="%d" aria-hidden="true">'
                '<line x1="2" y1="%d" x2="%d" y2="%d" class="spark-flat"/></svg>'
                % (width, height, height // 2, width - 2, height // 2))
    xs = [p[0] for p in pts]
    ys = [p[1] for p in pts]
    x0, x1 = min(xs), max(xs)
    lo, hi = min(ys), max(ys)
    pad = 6
    span = (hi - lo) or 1
    xspan = (x1 - x0) or 1

    def px(i):
        return pad + (i - x0) / xspan * (width - 2 * pad)

    def py(r):
        # omgekeerd: kleinste rang (beste) bovenaan
        return pad + (r - lo) / span * (height - 2 * pad)

    d = " ".join(("M" if k == 0 else "L") + f"{px(i):.1f},{py(r):.1f}"
                 for k, (i, r) in enumerate(pts))
    ex, ey = px(pts[-1][0]), py(pts[-1][1])
    label = " → ".join(str(r) for r in ys)
    return (
        f'<svg class="spark" width="{width}" height="{height}" role="img" '
        f'aria-label="Rangverloop: {label}">'
        f'<path d="{d}" class="spark-line"/>'
        f'<circle cx="{ex:.1f}" cy="{ey:.1f}" r="4" class="spark-end"/>'
        f"</svg>"
    )


def _delta_badge(delta: int | None) -> str:
    if delta is None:
        return '<span class="badge badge-new">✦ nieuw</span>'
    if delta > 0:
        return f'<span class="badge badge-up">▲ +{delta}</span>'
    if delta < 0:
        return f'<span class="badge badge-down">▼ {delta}</span>'
    return '<span class="badge badge-flat">= 0</span>'


def _card(item: dict, is_new: bool = False) -> str:
    img = item.get("image")
    thumb = (f'<img src="{_e(img)}" alt="" loading="lazy" '
             f'onerror="this.closest(\'.thumb\').classList.add(\'no-img\')">' if img else "")
    rank_line = (f'#{item["rank_before"]} → <strong>#{item["rank_now"]}</strong>'
                 if item.get("rank_before") else f'nieuw op <strong>#{item["rank_now"]}</strong>')
    streak = item.get("streak") or 0
    chips = []
    if item.get("pct"):
        chips.append(f'{item["pct"]}% van vorige positie')
    if streak >= 2:
        chips.append(f"{streak} dagen op rij omhoog")
    if item.get("weighted"):
        chips.append(f'score {item["weighted"]}')
    if item.get("created_at"):
        chips.append(f'sinds {item["created_at"]}')
    hist = item.get("history") or {}
    for lb, val in sorted(hist.items()):
        if val is not None:
            sign = "+" if val > 0 else ""
            chips.append(f"{sign}{val} over {lb}d")
    spark = sparkline(item.get("spark") or []) if not is_new else ""
    return f"""
      <article class="card{' card-new' if is_new else ''}">
        <div class="thumb">{thumb}</div>
        <div class="card-body">
          <div class="card-top">
            {_delta_badge(item.get("delta"))}
            <span class="rank">{rank_line}</span>
          </div>
          <h4 class="card-title"><a href="{_e(item['url'])}" target="_blank" rel="noopener">{_e(item['title'])}</a></h4>
          <div class="card-meta">{_e(_money(item.get('price')))}</div>
          {spark}
          <div class="chips">{''.join(f'<span class="chip">{_e(c)}</span>' for c in chips)}</div>
        </div>
      </article>"""


def _table(rows: list[dict]) -> str:
    if not rows:
        return "<p class='muted'>Geen rijen.</p>"
    body = "".join(
        f"<tr><td>{i+1}</td><td>{_e(r['title'])}</td>"
        f"<td class='num'>{r.get('rank_before') or '-'}</td>"
        f"<td class='num'>{r['rank_now']}</td>"
        f"<td class='num'>{'+' + str(r['delta']) if r.get('delta') else 'nieuw'}</td>"
        f"<td class='num'>{_e(_money(r.get('price')))}</td>"
        f"<td><a href=\"{_e(r['url'])}\" target='_blank' rel='noopener'>open</a></td></tr>"
        for i, r in enumerate(rows))
    return ("<div class='table-wrap'><table><thead><tr><th>#</th><th>Product</th>"
            "<th class='num'>Was</th><th class='num'>Nu</th><th class='num'>Delta</th>"
            f"<th class='num'>Prijs</th><th>Link</th></tr></thead><tbody>{body}</tbody></table></div>")


def _store_section(s: dict, cfg: Config) -> str:
    risers = s.get("risers") or []
    entrants = s.get("new_entrants") or []
    runners = s.get("runners_up") or []

    head_bits = [f"{s['total_products']} producten gevolgd",
                 f"{s['days_tracked']} snapshots"]
    if s.get("baseline_day"):
        head_bits.append(f"vergeleken met {s['baseline_day']} ({s['baseline_gap']} dagen terug)")
    note = f"<p class='note'>{_e(s['note'])}</p>" if s.get("note") else ""

    cards = "".join(_card(r) for r in risers) or "<p class='muted'>Geen stijgers boven de drempel.</p>"
    entrant_html = ""
    if entrants:
        entrant_html = f"""
        <h4 class="sub">Nieuw binnengekomen in de top {cfg.new_entrant_max_rank}</h4>
        <div class="grid">{''.join(_card(e, is_new=True) for e in entrants)}</div>"""

    return f"""
    <section class="store" id="store-{_e(s['domain'])}">
      <div class="store-head">
        <div>
          <h3>{_e(s['label'])}</h3>
          <a class="domain" href="https://{_e(s['domain'])}/collections/all?sort_by=best-selling"
             target="_blank" rel="noopener">{_e(s['domain'])}</a>
        </div>
        <div class="store-meta">{' &middot; '.join(_e(b) for b in head_bits)}</div>
      </div>
      {note}
      <div class="grid">{cards}</div>
      {entrant_html}
      <details class="more">
        <summary>Tabelweergave en volgende {len(runners)} stijgers</summary>
        {_table(risers + runners)}
      </details>
    </section>"""


# ---------------------------------------------------------------- dashboard
def build_html(results: list[dict], cfg: Config, run_at: str) -> str:
    ok = [r for r in results if r.get("current_day")]
    total_products = sum(r["total_products"] for r in ok)
    all_risers = [(r, s) for s in ok for r in (s.get("risers") or [])]
    best = max(all_risers, key=lambda t: t[0]["delta"], default=None)
    n_entrants = sum(len(s.get("new_entrants") or []) for s in ok)
    n_skipped = sum(s.get("skipped_known", 0) for s in ok)
    max_hist = max((s["days_tracked"] for s in ok), default=0)

    tiles = [
        ("Stores gevolgd", str(len(ok)), "actief in deze run"),
        ("Producten in beeld", f"{total_products:,}".replace(",", " "), "posities vastgelegd"),
        ("Historie", f"{max_hist}", "snapshots diep"),
        ("Nieuwe binnenkomers", str(n_entrants), f"top {cfg.new_entrant_max_rank}"),
        ("Al eerder gehad", str(n_skipped), "overgeslagen, nooit dubbel"),
    ]
    if best:
        tiles.append(("Grootste sprong", f"+{best[0]['delta']}",
                      f"{best[0]['title'][:38]} bij {best[1]['label']}"))

    tile_html = "".join(
        f"<div class='tile'><div class='tile-label'>{_e(l)}</div>"
        f"<div class='tile-value'>{_e(v)}</div>"
        f"<div class='tile-sub'>{_e(sub)}</div></div>"
        for l, v, sub in tiles)

    nav = "".join(
        f"<a href='#store-{_e(s['domain'])}'>{_e(s['label'])}</a>" for s in ok)

    sections = "".join(_store_section(s, cfg) for s in results)
    failed = [r for r in results if not r.get("current_day")]
    fail_html = ""
    if failed:
        fail_html = ("<section class='store'><h3>Zonder data</h3><p class='muted'>"
                     + ", ".join(_e(f["domain"]) for f in failed) + "</p></section>")

    return f"""<!doctype html>
<html lang="nl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Snelste stijgers</title>
<style>
:root {{
  color-scheme: light;
  --surface-0: #f4f3f0;
  --surface-1: #fcfcfb;
  --surface-2: #eeede9;
  --border:    #dedcd6;
  --text-primary:   #0b0b0b;
  --text-secondary: #52514e;
  --text-muted:     #7d7c76;
  --series-1: #2a78d6;
  --good:     #0ca30c;
  --critical: #d03b3b;
  --warning:  #fab219;
  --accent-soft: #e8f0fc;
}}
@media (prefers-color-scheme: dark) {{
  :root:not([data-theme="light"]) {{
    color-scheme: dark;
    --surface-0: #121211;
    --surface-1: #1a1a19;
    --surface-2: #232322;
    --border:    #34342f;
    --text-primary:   #ffffff;
    --text-secondary: #c3c2b7;
    --text-muted:     #8d8c83;
    --series-1: #3987e5;
    --accent-soft: #16263a;
  }}
}}
:root[data-theme="dark"] {{
  color-scheme: dark;
  --surface-0: #121211; --surface-1: #1a1a19; --surface-2: #232322;
  --border: #34342f; --text-primary: #ffffff; --text-secondary: #c3c2b7;
  --text-muted: #8d8c83; --series-1: #3987e5; --accent-soft: #16263a;
}}
* {{ box-sizing: border-box; }}
body {{
  margin: 0; background: var(--surface-0); color: var(--text-primary);
  font: 15px/1.5 ui-sans-serif, -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
}}
.wrap {{ max-width: 1180px; margin: 0 auto; padding: 32px 20px 72px; }}
header h1 {{ font-size: 26px; margin: 0 0 4px; letter-spacing: -0.01em; }}
header .lede {{ color: var(--text-secondary); margin: 0 0 24px; font-size: 14px; }}
.tiles {{ display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(168px, 1fr)); margin-bottom: 26px; }}
.tile {{ background: var(--surface-1); border: 1px solid var(--border); border-radius: 12px; padding: 14px 16px; }}
.tile-label {{ font-size: 11px; text-transform: uppercase; letter-spacing: .07em; color: var(--text-muted); }}
.tile-value {{ font-size: 26px; font-weight: 650; margin: 4px 0 2px; letter-spacing: -0.02em; }}
.tile-sub {{ font-size: 12px; color: var(--text-secondary); }}
nav {{ display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 26px; }}
nav a {{ font-size: 13px; padding: 5px 11px; border-radius: 999px; background: var(--surface-2);
        color: var(--text-secondary); text-decoration: none; border: 1px solid var(--border); }}
nav a:hover {{ color: var(--text-primary); }}
.store {{ background: var(--surface-1); border: 1px solid var(--border); border-radius: 14px;
          padding: 20px; margin-bottom: 22px; }}
.store-head {{ display: flex; flex-wrap: wrap; gap: 10px; justify-content: space-between;
               align-items: baseline; margin-bottom: 14px; }}
.store-head h3 {{ margin: 0; font-size: 18px; letter-spacing: -0.01em; }}
.domain {{ font-size: 13px; color: var(--series-1); text-decoration: none; }}
.store-meta {{ font-size: 12.5px; color: var(--text-muted); }}
.sub {{ font-size: 13px; text-transform: uppercase; letter-spacing: .06em;
        color: var(--text-muted); margin: 22px 0 10px; font-weight: 600; }}
.grid {{ display: grid; gap: 12px; grid-template-columns: repeat(auto-fill, minmax(198px, 1fr)); }}
.card {{ background: var(--surface-2); border: 1px solid var(--border); border-radius: 12px;
         overflow: hidden; display: flex; flex-direction: column; }}
.card-new {{ border-style: dashed; }}
.thumb {{ aspect-ratio: 1/1; background: var(--surface-1); display: flex; align-items: center; justify-content: center; }}
.thumb img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
.thumb.no-img::after {{ content: "geen foto"; font-size: 12px; color: var(--text-muted); }}
.card-body {{ padding: 11px 12px 13px; display: flex; flex-direction: column; gap: 7px; }}
.card-top {{ display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }}
.badge {{ font-size: 12px; font-weight: 650; padding: 2px 8px; border-radius: 6px; white-space: nowrap; }}
.badge-up {{ background: color-mix(in srgb, var(--good) 16%, transparent); color: var(--good); }}
.badge-down {{ background: color-mix(in srgb, var(--critical) 16%, transparent); color: var(--critical); }}
.badge-new {{ background: var(--accent-soft); color: var(--series-1); }}
.badge-flat {{ background: var(--surface-1); color: var(--text-muted); }}
.rank {{ font-size: 12.5px; color: var(--text-secondary); font-variant-numeric: tabular-nums; }}
.card-title {{ margin: 0; font-size: 14px; line-height: 1.35; font-weight: 600; }}
.card-title a {{ color: var(--text-primary); text-decoration: none; }}
.card-title a:hover {{ text-decoration: underline; }}
.card-meta {{ font-size: 13px; color: var(--text-secondary); font-variant-numeric: tabular-nums; }}
.spark {{ display: block; overflow: visible; }}
.spark-line {{ fill: none; stroke: var(--series-1); stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }}
.spark-end {{ fill: var(--series-1); stroke: var(--surface-2); stroke-width: 2; }}
.spark-flat {{ stroke: var(--border); stroke-width: 2; stroke-dasharray: 3 4; }}
.chips {{ display: flex; flex-wrap: wrap; gap: 5px; }}
.chip {{ font-size: 11px; color: var(--text-muted); background: var(--surface-1);
         border: 1px solid var(--border); border-radius: 5px; padding: 1px 6px; }}
.note {{ font-size: 13px; color: var(--text-secondary); background: var(--surface-2);
         border-left: 3px solid var(--warning); padding: 9px 12px; border-radius: 0 8px 8px 0; margin: 0 0 14px; }}
.muted {{ color: var(--text-muted); font-size: 13.5px; }}
.more {{ margin-top: 16px; }}
.more summary {{ cursor: pointer; font-size: 13px; color: var(--text-secondary); }}
.table-wrap {{ overflow-x: auto; margin-top: 12px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
th, td {{ text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--border); white-space: nowrap; }}
th {{ color: var(--text-muted); font-weight: 600; font-size: 11.5px;
      text-transform: uppercase; letter-spacing: .05em; }}
td.num, th.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
td a {{ color: var(--series-1); }}
footer {{ margin-top: 30px; font-size: 12px; color: var(--text-muted); line-height: 1.7; }}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Snelste stijgers</h1>
    <p class="lede">Run van {_e(run_at)} &middot; stijging gemeten over {cfg.lookback_days} dagen &middot;
       top {cfg.top_n} per store &middot; drempel: minstens {cfg.min_climb} plekken omhoog en nu binnen de top {cfg.min_current_rank}</p>
  </header>
  <div class="tiles">{tile_html}</div>
  <nav>{nav}</nav>
  {sections}
  {fail_html}
  <footer>
    Positie 1 is de bestverkopende. Een sprong van #340 naar #45 telt als +295.<br>
    De sparkline toont het rangverloop over de laatste snapshots; omhoog betekent stijgen.<br>
    Score = plekken gestegen gedeeld door de wortel van de huidige positie, zodat winst dicht bij de top zwaarder weegt.<br>
    Elk product hier is nieuw voor jou: alles wat al eens in een rapport stond wordt automatisch overgeslagen.<br>
    Naast dit dashboard staat <strong>shopify_import.csv</strong>, klaar voor Producten &rarr; Importeren in je eigen store.
  </footer>
</div>
</body>
</html>"""


# ---------------------------------------------------------------- outputs
def write_reports(results: list[dict], cfg: Config, outdir: Path | None = None) -> dict:
    outdir = Path(outdir or REPORTS)
    (outdir / "history").mkdir(parents=True, exist_ok=True)
    run_at = datetime.now().strftime("%Y-%m-%d %H:%M")
    stamp = datetime.now().strftime("%Y-%m-%d")

    html_doc = build_html(results, cfg, run_at)
    index = outdir / "index.html"
    index.write_text(html_doc, encoding="utf-8")
    (outdir / "history" / f"{stamp}.html").write_text(html_doc, encoding="utf-8")

    rows = []
    for s in results:
        for kind, items in (("stijger", s.get("risers") or []),
                            ("nieuw", s.get("new_entrants") or [])):
            for r in items:
                rows.append({
                    "run_date": stamp, "store": s["label"], "domain": s["domain"],
                    "type": kind, "title": r.get("title"), "handle": r.get("handle"),
                    "url": r.get("url"), "rank_now": r.get("rank_now"),
                    "rank_before": r.get("rank_before"), "delta": r.get("delta"),
                    "pct_gain": r.get("pct"), "weighted_score": r.get("weighted"),
                    "climb_streak_days": r.get("streak"), "price": r.get("price"),
                    "vendor": r.get("vendor"), "product_created": r.get("created_at"),
                    "image": r.get("image"),
                })
    csv_path = outdir / "risers_latest.csv"
    with csv_path.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        w.writeheader()
        w.writerows(rows)
    (outdir / "history" / f"{stamp}.csv").write_text(
        csv_path.read_text(encoding="utf-8-sig"), encoding="utf-8-sig")

    (outdir / "summary.md").write_text(build_markdown(results, cfg, run_at), encoding="utf-8")

    (outdir / "risers_latest.json").write_text(
        json.dumps({"run_at": run_at, "config": cfg.__dict__, "stores": results},
                   ensure_ascii=False, indent=2), encoding="utf-8")

    return {"html": index, "csv": csv_path, "md": outdir / "summary.md", "rows": len(rows)}


def build_markdown(results: list[dict], cfg: Config, run_at: str) -> str:
    """Korte samenvatting voor de GitHub Actions run-pagina."""
    total_skipped = sum(s.get("skipped_known", 0) for s in results)
    lines = [f"## Snelste stijgers - {run_at}", "",
             f"Venster: {cfg.lookback_days} dagen | top {cfg.top_n} per store | "
             f"{total_skipped} producten overgeslagen omdat je ze al eerder kreeg", "",
             "Download onderaan deze pagina bij **Artifacts**: `shopify_import.csv` "
             "kun je direct in Shopify importeren via Producten, Importeren.", ""]
    for s in results:
        label = s["label"]
        if not s.get("current_day"):
            lines += [f"### {label}", "", "_geen data opgehaald_", ""]
            continue
        head = (f"### {label} "
                f"({s['total_products']} producten, {s['days_tracked']} snapshots)")
        lines += [head, ""]
        risers = s.get("risers") or []
        if risers:
            lines += ["| # | Product | Was | Nu | Stijging | Prijs | Link |",
                      "|---|---------|----:|---:|---------:|------:|------|"]
            for i, r in enumerate(risers, 1):
                price = "" if r.get("price") is None else f"{r['price']:.2f}"
                lines.append(
                    f"| {i} | {r['title'][:60]} | {r['rank_before']} | {r['rank_now']} "
                    f"| +{r['delta']} | {price} | [open]({r['url']}) |")
        else:
            lines.append(f"_{s.get('note') or 'geen stijgers boven de drempel'}_")
        entrants = s.get("new_entrants") or []
        if entrants:
            lines += ["", "Nieuw binnengekomen: " + ", ".join(
                f"[{e['title'][:40]}]({e['url']}) (#{e['rank_now']})" for e in entrants)]
        lines.append("")
    return "\n".join(lines)


# ==========================================================================
# export
# ==========================================================================

# Bouwt een CSV die je rechtstreeks in Shopify kunt importeren.
# 
# Van elk gekozen product wordt de volledige productdata opgehaald
# (`/products/<handle>.json`): beschrijving, alle afbeeldingen, alle varianten
# met hun opties en prijzen. Dat gebeurt alleen voor de handvol producten die
# in het rapport staan, niet voor de hele catalogus.
# 
# De rijen volgen het importformaat van Shopify: Producten -> Importeren -> CSV.
# Alles komt binnen als concept (Status = draft), zodat er nooit per ongeluk
# iets live gaat voordat jij het hebt nagekeken.
# 




COLUMNS = [
    "Handle", "Title", "Body (HTML)", "Vendor", "Product Category", "Type", "Tags",
    "Published", "Option1 Name", "Option1 Value", "Option2 Name", "Option2 Value",
    "Option3 Name", "Option3 Value", "Variant SKU", "Variant Grams",
    "Variant Inventory Tracker", "Variant Inventory Qty", "Variant Inventory Policy",
    "Variant Fulfillment Service", "Variant Price", "Variant Compare At Price",
    "Variant Requires Shipping", "Variant Taxable", "Variant Barcode",
    "Image Src", "Image Position", "Image Alt Text", "Gift Card",
    "SEO Title", "SEO Description", "Status",
    # eigen kolommen, Shopify negeert deze bij import
    "Bron store", "Bron URL", "Positie nu", "Positie toen", "Stijging",
]


def money(value, multiplier: float) -> str:
    """Prijs omrekenen met correcte afronding op centen.

    Bewust Decimal en niet float: 16.95 maal 1.5 is exact 25.425 en moet
    25.43 worden, terwijl gewone floats daar 25.42 van maken.
    """
    if value in (None, ""):
        return ""
    try:
        d = Decimal(str(value)) * Decimal(str(multiplier))
    except Exception:  # noqa: BLE001
        return ""
    return str(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def normalise_title(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def fetch_product(domain: str, handle: str, cfg: Config) -> dict | None:
    url = f"{scheme_for(domain)}://{domain}/products/{handle}.json"
    try:
        r = requests.get(url, timeout=cfg.timeout,
                         headers={"User-Agent": cfg.user_agent})
        if r.status_code != 200:
            return None
        return (r.json() or {}).get("product")
    except (requests.RequestException, ValueError):
        return None


def _rows_for_product(p: dict, item: dict, store_label: str, cfg: Config) -> list[dict]:
    handle = f"{cfg.export_handle_prefix}{p.get('handle') or item['handle']}"
    options = p.get("options") or []
    opt_names = [o.get("name") for o in options][:3]
    while len(opt_names) < 3:
        opt_names.append(None)
    variants = p.get("variants") or [{}]
    images = [i.get("src") for i in (p.get("images") or []) if i.get("src")]
    if not images and item.get("image"):
        images = [item["image"]]

    tags = ", ".join(filter(None, [
        "rank-tracker",
        f"bron-{store_label.lower().replace(' ', '-')}",
        p.get("product_type") or None,
    ]))

    rows: list[dict] = []
    for vi, v in enumerate(variants):
        price = money(v.get("price"), cfg.export_price_multiplier)
        compare = money(v.get("compare_at_price"), cfg.export_price_multiplier)

        row = {c: "" for c in COLUMNS}
        row["Handle"] = handle
        row["Option1 Name"] = opt_names[0] or "Title"
        row["Option1 Value"] = v.get("option1") or "Default Title"
        row["Option2 Name"] = opt_names[1] or ""
        row["Option2 Value"] = v.get("option2") or ""
        row["Option3 Name"] = opt_names[2] or ""
        row["Option3 Value"] = v.get("option3") or ""
        row["Variant SKU"] = v.get("sku") or ""
        row["Variant Grams"] = v.get("grams") or 0
        row["Variant Inventory Tracker"] = "shopify"
        row["Variant Inventory Qty"] = cfg.export_inventory_qty
        row["Variant Inventory Policy"] = "continue"
        row["Variant Fulfillment Service"] = "manual"
        row["Variant Price"] = price
        row["Variant Compare At Price"] = compare
        row["Variant Requires Shipping"] = "TRUE" if v.get("requires_shipping", True) else "FALSE"
        row["Variant Taxable"] = "TRUE" if v.get("taxable", True) else "FALSE"
        row["Gift Card"] = "FALSE"

        if vi == 0:
            row["Title"] = p.get("title") or item.get("title")
            row["Body (HTML)"] = p.get("body_html") or ""
            row["Vendor"] = p.get("vendor") or store_label
            row["Type"] = p.get("product_type") or ""
            row["Tags"] = tags
            row["Published"] = "FALSE"
            row["Status"] = cfg.export_status
            row["SEO Title"] = (p.get("title") or "")[:70]
            row["SEO Description"] = re.sub(r"<[^>]+>", " ", p.get("body_html") or "")[:320].strip()
            row["Bron store"] = store_label
            row["Bron URL"] = item.get("url", "")
            row["Positie nu"] = item.get("rank_now", "")
            row["Positie toen"] = item.get("rank_before", "")
            row["Stijging"] = item.get("delta", "")
            if images:
                row["Image Src"] = images[0]
                row["Image Position"] = 1
                row["Image Alt Text"] = (p.get("title") or "")[:120]
        rows.append(row)

    for pos, src in enumerate(images[1:], start=2):
        extra = {c: "" for c in COLUMNS}
        extra["Handle"] = handle
        extra["Image Src"] = src
        extra["Image Position"] = pos
        rows.append(extra)
    return rows


def build_shopify_csv(results: list[dict], cfg: Config, outdir: Path,
                      include_new_entrants: bool = True,
                      log=print) -> dict:
    rows: list[dict] = []
    missed: list[str] = []
    seen_handles: set[str] = set()

    for store in results:
        items = list(store.get("risers") or [])
        if include_new_entrants:
            items += list(store.get("new_entrants") or [])
        for item in items:
            key = f"{store['domain']}/{item['handle']}"
            if key in seen_handles:
                continue
            seen_handles.add(key)
            p = fetch_product(store["domain"], item["handle"], cfg)
            if not p:
                missed.append(key)
                continue
            rows.extend(_rows_for_product(p, item, store["label"], cfg))

    path = outdir / "shopify_import.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        w = csv.DictWriter(fh, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)

    products = len({r["Handle"] for r in rows})
    if missed:
        log(f"  let op: {len(missed)} producten konden niet opgehaald worden "
            f"({', '.join(missed[:3])}{'...' if len(missed) > 3 else ''})")
    return {"path": path, "products": products, "rows": len(rows), "missed": missed}


# ==========================================================================
# module-aliassen: db.foo, storage.foo enzovoort verwijzen naar dit bestand
# ==========================================================================

config = db = storage = fetch = lists = analyze = report = export = sys.modules[__name__]


# ==========================================================================
# opdrachtregel
# ==========================================================================

STRATEGY_TTL_DAYS = 7


def _log(msg: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def _stale(checked_at: str | None) -> bool:
    if not checked_at:
        return True
    try:
        return (date.today() - datetime.strptime(checked_at, "%Y-%m-%d").date()).days >= STRATEGY_TTL_DAYS
    except ValueError:
        return True


# ------------------------------------------------------------------ commands
def cmd_snapshot(cfg: Config, args) -> int:
    stores = load_stores()
    if not stores:
        _log("stores.txt is leeg. Zet er minstens één domein in.")
        return 1
    today = args.day or date.today().isoformat()
    _log(f"{len(stores)} stores, {cfg.concurrency} tegelijk, "
         f"max {cfg.max_products} producten per store")

    def work(st):
        state = storage.read_state(st.domain)
        strategy = None if (args.redetect or _stale(state.get("checked_at"))) else state.get("strategy")
        return st, fetch_store(st, cfg, strategy)

    with ThreadPoolExecutor(max_workers=max(1, cfg.concurrency)) as pool:
        results = list(pool.map(work, stores))

    failures = 0
    for st, res in results:
        if not res.ok:
            failures += 1
            _log(f"  MISLUKT  {st.domain}: {res.error}")
            continue
        storage.write_snapshot(st.domain, today, res.handles, res.strategy,
                               datetime.now().isoformat(timespec="seconds"))
        if res.catalog:
            storage.merge_catalog(st.domain, res.catalog, today)
        storage.write_state(st.domain, res.strategy, res.page_size, date.today().isoformat())
        gone = storage.prune(st.domain, cfg.keep_days)
        extra = f", {gone} oude opgeruimd" if gone else ""
        _log(f"  ok       {st.domain}: {len(res.handles)} posities via {res.strategy}{extra}")

    _log(f"klaar: {len(results) - failures} gelukt, {failures} mislukt")
    return 1 if failures == len(results) else 0


def cmd_report(cfg: Config, args) -> int:
    if args.days:
        cfg.lookback_days = args.days
    stores = load_stores()
    if not stores:
        _log("stores.txt is leeg.")
        return 1

    day = args.day or date.today().isoformat()
    man_per_store, man_global, man_titles = lists.load_manual_exclusions()
    excludes: dict[str, set[str]] = {}
    for st in stores:
        ex = set(man_global) | set(man_per_store.get(st.domain, set()))
        if cfg.suppress_repeats:
            ex |= storage.suppressed_handles(st.domain, day, cfg.repeat_cooldown_days)
        excludes[st.domain] = ex
    if man_global or man_per_store or man_titles:
        _log(f"handmatige uitsluitingen: {len(man_global) + sum(len(v) for v in man_per_store.values())} "
             f"handles, {len(man_titles)} titels")

    own_titles = []
    if cfg.my_store:
        titles = lists.fetch_own_catalog(cfg.my_store, cfg)
        own_titles = [lists._tokens(t) for t in titles]
        _log(f"eigen store {cfg.my_store}: {len(titles)} producten geladen om dubbel te voorkomen")

    with db.session() as con:
        storage.rebuild_index(con, stores)
        results = analyze.analyse_all(con, cfg, excludes, man_titles, own_titles)

    if cfg.suppress_repeats and not args.no_record:
        for s in results:
            items = (s.get("risers") or []) + (s.get("new_entrants") or [])
            storage.record_reported(s["domain"], s.get("current_day") or day, items)

    out = report.write_reports(results, cfg)
    _log(f"dashboard: {out['html']}")
    _log(f"csv:       {out['csv']} ({out['rows']} rijen)")

    if not args.no_export:
        exp = export.build_shopify_csv(results, cfg, out["html"].parent, log=_log)
        _log(f"shopify:   {exp['path']} ({exp['products']} producten, {exp['rows']} rijen)")

    total_skipped = sum(s.get("skipped_known", 0) for s in results)
    for s in results:
        risers = s.get("risers") or []
        if risers:
            _log(f"  {s['label']}: " + ", ".join(
                f"{r['title'][:34]} (+{r['delta']})" for r in risers))
        else:
            _log(f"  {s['label']}: {s.get('note') or 'geen stijgers'}")
    if total_skipped:
        _log(f"{total_skipped} producten overgeslagen omdat je ze al eerder kreeg")
    return 0


def cmd_forget(cfg: Config, args) -> int:
    """Wist de geschiedenis van al gemelde producten."""
    storage.forget_reported(args.store)
    _log(f"geschiedenis gewist voor {args.store or 'alle stores'}; "
         f"producten mogen weer opnieuw voorbijkomen")
    return 0


def cmd_run(cfg: Config, args) -> int:
    rc = cmd_snapshot(cfg, args)
    rc2 = cmd_report(cfg, args)
    return rc2 if rc == 0 else rc


def cmd_stores(cfg: Config, args) -> int:
    for st in load_stores():
        files = storage.snapshot_files(st.domain)
        state = storage.read_state(st.domain)
        span = f"{files[0][0]} t/m {files[-1][0]}" if files else "nog geen data"
        print(f"{st.domain:<34} {st.label:<26} {len(files):>3} snapshots  "
              f"[{state.get('strategy') or 'nog niet bepaald'}]  {span}")
    return 0


def cmd_simulate(cfg: Config, args) -> int:
    """Schrijft verzonnen snapshots weg zodat je het dashboard kunt zien
    zonder eerst dagen te wachten. Zet de demo-domeinen zelf in stores.txt."""

    def demo_image(seed: int) -> str:
        hue = (seed * 47) % 360
        svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="300" height="300">'
               f'<rect width="300" height="300" fill="hsl({hue},42%,72%)"/>'
               f'<circle cx="150" cy="128" r="62" fill="hsl({hue},48%,52%)"/>'
               f'<rect x="66" y="208" width="168" height="16" rx="8" fill="hsl({hue},40%,58%)"/>'
               f"</svg>")
        return "data:image/svg+xml;base64," + base64.b64encode(svg.encode()).decode()

    random.seed(args.seed)
    demo = [("demo-store-a.com", "Demo Store A"), ("demo-store-b.com", "Demo Store B")]
    for domain, label in demo:
        n = 400
        handles = [f"product-{i:03d}" for i in range(1, n + 1)]
        catalog = {h: {"title": f"Demo product {h.split('-')[1]}",
                       "price": round(random.uniform(9, 89), 2),
                       "image": demo_image(i), "vendor": label,
                       "product_id": i, "created_at": "2026-01-01"}
                   for i, h in enumerate(handles, 1)}
        order = handles[:]
        random.shuffle(order)
        climbers = random.sample(order[220:380], 6)
        climb_start = {h: random.randint(2, max(3, args.days_of_history - 1)) for h in climbers}
        newcomers = [f"newcomer-{k}" for k in range(1, 3)]
        for h in newcomers:
            catalog[h] = {"title": f"Nieuw product {h[-1]}",
                          "price": round(random.uniform(19, 59), 2),
                          "image": demo_image(900 + int(h[-1])), "vendor": label,
                          "product_id": 9000 + int(h[-1]),
                          "created_at": date.today().isoformat()}
        for back in range(args.days_of_history, -1, -1):
            day = (date.today() - timedelta(days=back)).isoformat()
            for _ in range(60):
                i = random.randrange(len(order))
                j = min(len(order) - 1, max(0, i + random.randint(-5, 5)))
                order[i], order[j] = order[j], order[i]
            for h in climbers:
                if back > climb_start[h]:
                    continue
                cur = order.index(h)
                if cur < 12:
                    continue
                order.insert(max(0, cur - random.randint(12, 30)), order.pop(cur))
            if back <= 1:
                for h in newcomers:
                    if h not in order:
                        order.insert(random.randint(20, 90), h)
            storage.write_snapshot(domain, day, list(order), "json", f"{day}T06:00:00")
            storage.merge_catalog(domain, catalog, day)
        storage.write_state(domain, "json", None, date.today().isoformat())
    _log(f"simulatiedata klaar voor {len(demo)} stores over {args.days_of_history + 1} dagen")
    _log("zet demo-store-a.com en demo-store-b.com in stores.txt en draai: python rank_tracker.py report")
    return 0


def main(argv=None) -> int:
    cfg = Config.load()
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd")

    def common(sp):
        sp.add_argument("--days", type=int, help="vergelijkvenster in dagen")
        sp.add_argument("--day", help="forceer de snapshotdatum, YYYY-MM-DD")
        sp.add_argument("--redetect", action="store_true",
                        help="opnieuw bepalen hoe de sortering opgehaald wordt")
        sp.add_argument("--no-export", action="store_true",
                        help="geen Shopify import-CSV bouwen")
        sp.add_argument("--no-record", action="store_true",
                        help="niet onthouden dat deze producten gemeld zijn")

    for name, fn in (("run", cmd_run), ("snapshot", cmd_snapshot),
                     ("report", cmd_report), ("stores", cmd_stores)):
        sp = sub.add_parser(name)
        common(sp)
        sp.set_defaults(fn=fn)

    sp = sub.add_parser("forget")
    sp.add_argument("--store", help="alleen dit domein vergeten")
    common(sp)
    sp.set_defaults(fn=cmd_forget)

    sp = sub.add_parser("simulate")
    common(sp)
    sp.add_argument("--days-of-history", type=int, default=12)
    sp.add_argument("--seed", type=int, default=7)
    sp.set_defaults(fn=cmd_simulate)

    args = p.parse_args(argv)
    if not getattr(args, "fn", None):
        p.print_help()
        return 1
    return args.fn(cfg, args)


if __name__ == "__main__":
    sys.exit(main())
