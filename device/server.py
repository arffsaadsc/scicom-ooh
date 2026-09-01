#!/usr/bin/env python3
"""
wallboard device daemon.

Model
-----
DEVICE (this Pi, stable uid)  ->  DISPLAYS (one per HDMI output)  ->  each display
independently plays a PLAYLIST drawn from a device-wide library.

Two displays may share a mirror_group, in which case one rotation clock drives
both so they switch together.

Control is MQTT only; the device makes an outbound connection to the broker and
never listens for control traffic. A small loopback HTTP server exists solely to
serve the idle screen and media viewer pages that Chromium loads.
"""
import base64
import io
import json
import logging
import math
import mimetypes
import os
import re
import secrets
import shutil
import socket
import sqlite3
import subprocess
import threading
import time
import urllib.parse
from datetime import datetime

import requests
from flask import Flask, render_template, request, send_from_directory

DB_PATH = os.environ.get("WALLBOARD_DB", "/var/lib/wallboard/wallboard.db")
MEDIA_DIR = os.environ.get("WALLBOARD_MEDIA", os.path.join(os.path.dirname(DB_PATH), "media"))
PORT = int(os.environ.get("WALLBOARD_PORT", "8080"))
STATE_FILE = os.environ.get("WALLBOARD_STATE_FILE", "/run/wallboard/state.json")
TICKER_DONE_FILE = os.environ.get("WALLBOARD_TICKER_DONE",
                                 "/run/wallboard/ticker-done.json")
PENDING_FILE = os.path.join(os.path.dirname(DB_PATH), "pending-settings.json")
APPLY_HELPER = "/usr/local/bin/wallboard-settings-apply"
CEC_DEV = os.environ.get("WALLBOARD_CEC", "/dev/cec0")

BROKER = os.environ.get("WALLBOARD_BROKER", "127.0.0.1")
BROKER_PORT = int(os.environ.get("WALLBOARD_BROKER_PORT", "1883"))
ROOT = os.environ.get("WALLBOARD_ROOT", "wallboard")
STATUS_INTERVAL = float(os.environ.get("WALLBOARD_STATUS_INTERVAL", "0.25"))

MAX_UPLOAD = int(os.environ.get("WALLBOARD_MAX_UPLOAD", str(512 * 1024 * 1024)))
MIN_FREE_BYTES = 1024 * 1024 * 1024
IMAGE_EXT = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp", ".svg"}
VIDEO_EXT = {".mp4", ".webm", ".m4v", ".mov", ".mkv", ".ogv"}

# Topic ids that earlier builds used (hostname-derived). Cleared once at startup
# so a rename can never leave a ghost device on the broker again.
LEGACY_IDS = ("aies-infra-dashboard", "wallboard")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("wallboard")

WAYLAND_ENV = dict(os.environ,
                   XDG_RUNTIME_DIR="/run/user/%d" % os.getuid(),
                   WAYLAND_DISPLAY=os.environ.get("WAYLAND_DISPLAY", "wayland-0"))

SCHEMA = """
CREATE TABLE IF NOT EXISTS playlists (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  name           TEXT NOT NULL,
  dwell          INTEGER NOT NULL DEFAULT 30,
  reload_on_show INTEGER NOT NULL DEFAULT 0,
  enabled        INTEGER NOT NULL DEFAULT 1,
  scroll_delay   INTEGER NOT NULL DEFAULT 4,
  keep_live      INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS items (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
  url         TEXT NOT NULL,
  title       TEXT NOT NULL DEFAULT '',
  dwell       INTEGER,
  position    INTEGER NOT NULL DEFAULT 0,
  enabled     INTEGER NOT NULL DEFAULT 1,
  scroll_y    INTEGER NOT NULL DEFAULT 0,
  zoom        REAL NOT NULL DEFAULT 1.0,
  media_id    INTEGER
);
CREATE TABLE IF NOT EXISTS schedules (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  playlist_id INTEGER NOT NULL REFERENCES playlists(id) ON DELETE CASCADE,
  display_id  TEXT,                                  -- NULL = any display
  days        TEXT NOT NULL DEFAULT '0,1,2,3,4,5,6',
  start_time  TEXT NOT NULL DEFAULT '08:00',
  end_time    TEXT NOT NULL DEFAULT '18:00',
  priority    INTEGER NOT NULL DEFAULT 0,
  enabled     INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS media (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  stored   TEXT NOT NULL,
  original TEXT NOT NULL,
  mime     TEXT NOT NULL DEFAULT '',
  kind     TEXT NOT NULL DEFAULT 'image',
  bytes    INTEGER NOT NULL DEFAULT 0,
  duration REAL,
  created  TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS displays (
  id           TEXT PRIMARY KEY,                     -- output name, e.g. HDMI-A-1
  label        TEXT NOT NULL DEFAULT '',
  enabled      INTEGER NOT NULL DEFAULT 1,
  mirror_group TEXT,                                 -- NULL = independent
  override     TEXT NOT NULL DEFAULT '',             -- manual playlist pin
  first_seen   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS tv_schedule (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  days     TEXT NOT NULL DEFAULT '0,1,2,3,4',
  on_time  TEXT NOT NULL DEFAULT '08:00',
  off_time TEXT NOT NULL DEFAULT '19:00',
  enabled  INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS ticker_messages (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  text     TEXT NOT NULL,
  enabled  INTEGER NOT NULL DEFAULT 1,
  position INTEGER NOT NULL DEFAULT 0,
  created  TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS ticker_batches (
  id       INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id   TEXT NOT NULL,
  text     TEXT NOT NULL,
  items    TEXT NOT NULL DEFAULT '[]',   -- json list of the messages in the batch
  repeats  INTEGER NOT NULL DEFAULT 2,
  source   TEXT NOT NULL DEFAULT 'manual',
  started  TEXT NOT NULL DEFAULT '',
  active   INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS ticker_schedules (
  id        INTEGER PRIMARY KEY AUTOINCREMENT,
  kind      TEXT NOT NULL DEFAULT 'recurring',   -- recurring | oneoff
  days      TEXT NOT NULL DEFAULT '0,1,2,3,4',   -- recurring: 0=Mon .. 6=Sun
  at_date   TEXT NOT NULL DEFAULT '',            -- oneoff: YYYY-MM-DD
  at_time   TEXT NOT NULL DEFAULT '09:00',
  repeats   INTEGER,                             -- NULL = use the default
  enabled   INTEGER NOT NULL DEFAULT 1,
  last_run  TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS settings (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""


def db():
    c = sqlite3.connect(DB_PATH, timeout=10)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys=ON")
    return c


def get_setting(k, default=None):
    with db() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (k,)).fetchone()
    return r["value"] if r else default


def set_setting(k, v):
    with db() as c:
        c.execute("INSERT INTO settings(key,value) VALUES(?,?) "
                  "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (k, str(v)))


def device_uid():
    """Stable identity, independent of hostname.

    A ghost device on the broker is caused by the topic key changing while old
    retained messages stay pinned. Deriving the key from machine-id and then
    persisting it means no rename, IP change or relabel can ever move us to a
    new topic tree.
    """
    uid = get_setting("device_uid")
    if uid:
        return uid
    try:
        uid = "wb-" + open("/etc/machine-id").read().strip()[:12]
    except Exception:
        uid = "wb-" + secrets.token_hex(6)
    set_setting("device_uid", uid)
    log.info("device uid assigned (persistent): %s", uid)
    return uid


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    os.makedirs(MEDIA_DIR, exist_ok=True)
    with db() as c:
        c.executescript(SCHEMA)
        cols = {r["name"] for r in c.execute("PRAGMA table_info(schedules)")}
        if "display_id" not in cols:
            c.execute("ALTER TABLE schedules ADD COLUMN display_id TEXT")
            log.info("migrated: schedules.display_id")
        for tbl, col, ddl in (("items", "zoom", "REAL NOT NULL DEFAULT 1.0"),
                             ("items", "media_id", "INTEGER"),
                             ("items", "scroll_y", "INTEGER NOT NULL DEFAULT 0"),
                             ("playlists", "keep_live", "INTEGER NOT NULL DEFAULT 1"),
                             ("playlists", "scroll_delay", "INTEGER NOT NULL DEFAULT 4")):
            have = {r["name"] for r in c.execute("PRAGMA table_info(%s)" % tbl)}
            if col not in have:
                c.execute("ALTER TABLE %s ADD COLUMN %s %s" % (tbl, col, ddl))
                log.info("migrated: %s.%s", tbl, col)


# ---------------------------------------------------------------------------
# Output discovery
# ---------------------------------------------------------------------------
def cdp_port_for(name):
    """HDMI-A-1 -> 9222, HDMI-A-2 -> 9223. Keyed by port, per the design choice
    that a display's identity is the socket you plug into."""
    m = re.search(r"(\d+)$", name or "")
    return 9222 + (int(m.group(1)) - 1 if m else 0)


def list_outputs():
    """Connected outputs and their current mode, via wlr-randr."""
    try:
        out = subprocess.run(["wlr-randr"], capture_output=True, text=True,
                             timeout=10, env=WAYLAND_ENV).stdout
    except Exception as e:
        log.warning("wlr-randr failed: %s", e)
        return []
    res, cur = [], None
    for line in out.splitlines():
        if line and not line[0].isspace():
            name = line.split('"')[0].strip()
            if name:
                cur = {"id": name, "label": "", "mode": None, "enabled": False}
                m = re.search(r'"([^"]+)"', line)
                if m:
                    cur["label"] = m.group(1)
                res.append(cur)
        elif cur is not None:
            s = line.strip()
            if s.startswith("Enabled:"):
                cur["enabled"] = s.split()[-1].lower() == "yes"
            elif "current" in s:
                mm = re.match(r"([0-9]+x[0-9]+)\s*px,\s*([0-9.]+)\s*Hz", s)
                if mm:
                    cur["mode"] = "%s@%.0fHz" % (mm.group(1), float(mm.group(2)))
    for r in res:
        # labwc synthesises a NOOP output when no monitor is attached. It is a
        # usable surface (headless operation) but must never be persisted as a
        # display, or it outlives the unplug and collides with the real output.
        r["synthetic"] = r["id"].startswith("NOOP")
    return res


def sync_displays():
    """Upsert discovered outputs into the displays table."""
    outs = list_outputs()
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    with db() as c:
        for o in outs:
            if o.get("synthetic"):
                continue
            r = c.execute("SELECT id FROM displays WHERE id=?", (o["id"],)).fetchone()
            if r is None:
                c.execute("INSERT INTO displays(id,label,first_seen) VALUES(?,?,?)",
                          (o["id"], o["label"] or o["id"], now))
                log.info("new display registered: %s (%s)", o["id"], o["label"])
        # purge synthetic rows left behind by older builds
        gone = c.execute("SELECT id FROM displays WHERE id LIKE 'NOOP%'").fetchall()
        for g in gone:
            c.execute("DELETE FROM displays WHERE id=?", (g["id"],))
            log.info("removed synthetic display row: %s", g["id"])
    return {o["id"]: o for o in outs}


# ---------------------------------------------------------------------------
# Injected JS. All of this is carried over verbatim from the verified build.
# ---------------------------------------------------------------------------
_CANDS = """
  const all = Array.from(document.querySelectorAll('*'))
    .concat([document.scrollingElement, document.body, document.documentElement]);
  const cands = all.filter(e => e && e.scrollHeight > e.clientHeight + 40);
  cands.sort((a,b) => (b.scrollHeight-b.clientHeight)-(a.scrollHeight-a.clientHeight));
  const info = e => ({top: Math.round(e.scrollTop),
                      max: Math.max(0, e.scrollHeight - e.clientHeight),
                      tag: e.tagName});
"""

# Picking the scroller by "largest overflow" is unsafe: in Grafana, BODY reports
# one pixel more than HTML but ignores scrollTop. Try each and keep the one that
# measurably MOVES.
JS_SET_SCROLL = "(() => {" + _CANDS + """
  const y = __Y__;
  for (const e of cands.slice(0, 40)) {
    const before = e.scrollTop;
    try { e.scrollTop = y; } catch (_) { continue; }
    const d = info(e);
    if (Math.abs(d.top - Math.min(y, d.max)) <= 3 && d.top > 0) {
      return JSON.stringify(Object.assign({ok: true}, d));
    }
    try { e.scrollTop = before; } catch (_) {}
  }
  window.scrollTo(0, y);
  const de = document.scrollingElement || document.documentElement;
  const d = de ? info(de) : {top: 0, max: 0, tag: 'none'};
  return JSON.stringify(Object.assign({ok: d.top > 0}, d));
})()"""

JS_READ_SCROLL = "(() => {" + _CANDS + """
  for (const e of cands.slice(0, 40)) {
    if (Math.round(e.scrollTop) > 0) return JSON.stringify(info(e));
  }
  for (const e of cands.slice(0, 40)) {
    const b = e.scrollTop;
    try { e.scrollTop = b + 10; } catch (_) { continue; }
    const moved = Math.round(e.scrollTop) !== Math.round(b);
    try { e.scrollTop = b; } catch (_) {}
    if (moved) return JSON.stringify(info(e));
  }
  return JSON.stringify({top: 0, max: 0, tag: 'none'});
})()"""

# CSS zoom reflows the document, so a dashboard fits MORE in rather than being
# magnified - which is what you want on a wallboard.
JS_SET_ZOOM = """(() => {
  const z = __Z__;
  const el = document.documentElement;
  if (!el) return 'no-doc';
  el.style.zoom = (z === 1 ? '' : String(z));
  return 'zoom=' + (el.style.zoom || '1');
})()"""

# Grafana pauses its query timers while hidden and refetches on becoming visible,
# which is the "every page loads fresh" flash. Measured: with this installed a
# hidden tab still issues ~26 data queries per 30s.
JS_KEEP_LIVE = """(() => {
  if (window.__wb_live) return 'already';
  const vis = { get: () => 'visible', configurable: true };
  const hid = { get: () => false, configurable: true };
  try { Object.defineProperty(document, 'visibilityState', vis); } catch (e) {}
  try { Object.defineProperty(document, 'webkitVisibilityState', vis); } catch (e) {}
  try { Object.defineProperty(document, 'hidden', hid); } catch (e) {}
  try { Object.defineProperty(document, 'webkitHidden', hid); } catch (e) {}
  document.addEventListener('visibilitychange', e => e.stopImmediatePropagation(), true);
  window.addEventListener('blur', e => e.stopImmediatePropagation(), true);
  window.__wb_live = 1;
  return 'installed';
})()"""

JS_DOC_STATE = "document.readyState"

# Videos must run to completion, so the rotator asks the page rather than
# trusting a duration guessed at upload time.
JS_VIDEO_STATE = """(() => {
  const v = document.querySelector('video');
  if (!v) return JSON.stringify({video: false});
  return JSON.stringify({video: true, ended: !!v.ended, paused: !!v.paused,
                         t: +(v.currentTime || 0).toFixed(2),
                         dur: +(v.duration || 0).toFixed(2)});
})()"""


class Chrome:
    """CDP client for ONE Chromium instance (one display)."""

    def __init__(self, port):
        self.port = port
        self.base = "http://127.0.0.1:%d" % port

    def _get(self, path, timeout=5):
        return requests.get(self.base + path, timeout=timeout)

    def alive(self):
        try:
            self._get("/json/version", timeout=2)
            return True
        except Exception:
            return False

    def pages(self):
        """None means 'could not ask' - distinct from [] meaning 'no pages'.
        Conflating them made a transient error trigger a full tab rebuild."""
        try:
            targets = self._get("/json/list").json()
        except Exception:
            return None
        return [t for t in targets
                if t.get("type") == "page" and not t.get("url", "").startswith("devtools://")]

    def new_tab(self, url):
        q = urllib.parse.quote(url, safe="")
        try:
            r = requests.put(self.base + "/json/new?" + q, timeout=10)
            if r.status_code >= 400:
                r = requests.get(self.base + "/json/new?" + q, timeout=10)
            return r.json()
        except Exception as e:
            log.warning("[%d] new_tab(%s) failed: %s", self.port, url, e)
            return None

    def close(self, tid):
        try:
            self._get("/json/close/" + tid)
        except Exception:
            pass

    def activate(self, tid):
        try:
            self._get("/json/activate/" + tid)
            return True
        except Exception:
            return False

    def eval(self, tid, expr, timeout=8):
        try:
            import websocket
        except ImportError:
            return None
        tgt = next((t for t in (self.pages() or []) if t["id"] == tid), None)
        if not tgt or "webSocketDebuggerUrl" not in tgt:
            return None
        try:
            ws = websocket.create_connection(tgt["webSocketDebuggerUrl"], timeout=timeout)
            ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate",
                                "params": {"expression": expr, "returnByValue": True}}))
            for _ in range(40):
                m = json.loads(ws.recv())
                if m.get("id") == 1:
                    ws.close()
                    return m.get("result", {}).get("result", {}).get("value")
            ws.close()
        except Exception:
            return None

    def navigate(self, tid, url):
        try:
            import websocket
        except ImportError:
            return False
        tgt = next((t for t in (self.pages() or []) if t["id"] == tid), None)
        if not tgt:
            return False
        try:
            ws = websocket.create_connection(tgt["webSocketDebuggerUrl"], timeout=5)
            ws.send(json.dumps({"id": 1, "method": "Page.navigate",
                                "params": {"url": url}}))
            ws.recv()
            ws.close()
            return True
        except Exception:
            return False

    def cdp(self, tid, calls, timeout=8):
        """Send a list of (method, params) over one websocket and return replies."""
        try:
            import websocket
        except ImportError:
            return None
        tgt = next((t for t in (self.pages() or []) if t["id"] == tid), None)
        if not tgt or "webSocketDebuggerUrl" not in tgt:
            return None
        out = []
        try:
            ws = websocket.create_connection(tgt["webSocketDebuggerUrl"], timeout=timeout)
            for n, (method, params) in enumerate(calls, 1):
                ws.send(json.dumps({"id": n, "method": method, "params": params or {}}))
            wanted = len(calls)
            for _ in range(wanted * 8):
                m = json.loads(ws.recv())
                if "id" in m:
                    out.append(m)
                    if len(out) >= wanted:
                        break
            ws.close()
            return out
        except Exception as e:
            log.warning("[%d] cdp %s failed: %s", self.port, calls and calls[0][0], e)
            return None

    def viewport(self, tid):
        v = self.eval(tid, "JSON.stringify([innerWidth,innerHeight])")
        try:
            w, h = json.loads(v)
            return int(w), int(h)
        except Exception:
            return None, None

    def read_scroll(self, tid):
        try:
            return json.loads(self.eval(tid, JS_READ_SCROLL))
        except (TypeError, ValueError):
            return None

    def set_scroll(self, tid, y):
        return self.eval(tid, JS_SET_SCROLL.replace("__Y__", str(int(y))))

    def set_zoom(self, tid, z):
        return self.eval(tid, JS_SET_ZOOM.replace("__Z__", repr(float(z))))

    def video_state(self, tid):
        try:
            return json.loads(self.eval(tid, JS_VIDEO_STATE))
        except (TypeError, ValueError):
            return None


IDLE_URL = "http://127.0.0.1:%d/idle" % PORT


class Rotator:
    """Playlist rotation for ONE display."""

    PRIME_MIN, PRIME_MAX, PRIME_TOTAL = 1.5, 10.0, 120.0
    SCROLL_EVERY = 1.0
    ALIVE_EVERY = PAGES_EVERY = 2.0

    def __init__(self, display_id):
        self.id = display_id
        self.chrome = Chrome(cdp_port_for(display_id))
        self.lock = threading.RLock()
        self.playlist_id = None
        self.playlist_name = None
        self.tabs = []
        self.index = 0
        self.last_switch = 0.0
        self.chrome_up = False
        self.pending = None
        self.paused = False
        self.warming = False
        self.warm_i = 0
        self.warm_deadline = self.warm_all_by = 0.0
        self.warm_note = ""
        self.scroll_state = {}
        self.zoom_done = {}
        self.live_done = {}
        self.vid_state = {}      # target_id -> last video state, per tab
        self._t = {}

    # ---- small throttle helper so a fast tick does not multiply CDP calls ----
    def due(self, name, every):
        now = time.time()
        if now - self._t.get(name, 0.0) >= every:
            self._t[name] = now
            return True
        return False

    def items_for(self, pid):
        with db() as c:
            pl = c.execute("SELECT * FROM playlists WHERE id=?", (pid,)).fetchone()
            if not pl:
                return None, []
            # join the media kind: "is this a video" must not be inferred from
            # merely being a media item, or photos get treated as clips
            items = c.execute(
                "SELECT i.*, m.kind AS media_kind FROM items i "
                "LEFT JOIN media m ON m.id = i.media_id "
                "WHERE i.playlist_id=? AND i.enabled=1 "
                "ORDER BY i.position, i.id", (pid,)).fetchall()
        return pl, items

    def rebuild(self, playlist_id):
        with self.lock:
            existing = self.chrome.pages() or []
            urls = []
            if playlist_id:
                pl, items = self.items_for(playlist_id)
                if pl and items:
                    for it in items:
                        urls.append({
                            "url": it["url"], "title": it["title"] or it["url"],
                            "dwell": it["dwell"] or pl["dwell"],
                            "item_id": it["id"], "scroll_y": it["scroll_y"] or 0,
                            "zoom": it["zoom"] or 1.0,
                            "scroll_delay": pl["scroll_delay"] or 4,
                            "keep_live": pl["keep_live"],
                            "media_id": it["media_id"],
                            "media_kind": it["media_kind"],
                        })
            if not urls:
                playlist_id = None
                urls = [{"url": IDLE_URL, "title": "Idle", "dwell": 3600,
                         "item_id": None, "scroll_y": 0, "zoom": 1.0,
                         "scroll_delay": 4, "keep_live": 0, "media_id": None,
                         "media_kind": None}]

            new_tabs = []
            for u in urls:
                t = self.chrome.new_tab(u["url"])
                if t and t.get("id"):
                    d = dict(u)
                    d["target_id"] = t["id"]
                    new_tabs.append(d)
                time.sleep(0.3)
            for t in existing:                       # drop old tabs only once
                self.chrome.close(t["id"])           # replacements exist

            self.tabs = new_tabs
            self.playlist_id = playlist_id
            with db() as c:
                r = (c.execute("SELECT name FROM playlists WHERE id=?",
                               (playlist_id,)).fetchone() if playlist_id else None)
            self.playlist_name = r["name"] if r else None
            self.index = 0
            self.last_switch = time.time()
            self.scroll_state, self.zoom_done, self.live_done = {}, {}, {}
            self.vid_state = {}
            self.warming = False
            if new_tabs:
                self.chrome.activate(new_tabs[0]["target_id"])
            log.info("[%s] rebuilt: playlist=%s tabs=%d", self.id, playlist_id, len(new_tabs))
            if playlist_id is not None and new_tabs:
                self.prime_begin()

    def resync_meta(self):
        """Apply title/dwell/scroll/zoom edits without reopening tabs - reopening
        reloads the page and looks exactly like 'reload on show' misfiring."""
        with self.lock:
            if not self.playlist_id or not self.tabs:
                return
            pl, items = self.items_for(self.playlist_id)
            if not pl:
                return
            by_id = {it["id"]: it for it in items}
            for t in self.tabs:
                it = by_id.get(t.get("item_id"))
                if it:
                    t["dwell"] = it["dwell"] or pl["dwell"]
                    t["title"] = it["title"] or it["url"]
                    t["scroll_y"] = it["scroll_y"] or 0
                    t["zoom"] = it["zoom"] or 1.0
                    t["scroll_delay"] = pl["scroll_delay"] or 4

    # ------------------------------- priming -------------------------------
    def prime_begin(self):
        if not self.tabs:
            self.warming = False
            return
        self.warming = True
        self.warm_i = 0
        now = time.time()
        self.warm_deadline = now + self.PRIME_MAX
        self.warm_all_by = now + self.PRIME_TOTAL
        self.index = 0
        self.last_switch = now
        self.warm_note = self.tabs[0]["title"][:48]
        self.chrome.activate(self.tabs[0]["target_id"])
        log.info("[%s] priming pass started: %d tabs", self.id, len(self.tabs))

    def primed(self, tab):
        """Primed = load kicked off and keep-live installed, so it keeps working
        while hidden. We do NOT wait for readyState complete."""
        el = time.time() - self.last_switch
        if el < self.PRIME_MIN:
            return False
        if tab["url"].startswith("http://127.0.0.1:"):
            return True
        if self.live_done.get(tab["target_id"]):
            return True
        return el >= self.PRIME_MAX

    def prime_step(self):
        n = len(self.tabs)
        if not n:
            self.warming = False
            return
        i = min(self.warm_i, n - 1)
        tab = self.tabs[i]
        self.warm_note = tab["title"][:48]
        now = time.time()
        if not (self.primed(tab) or now >= self.warm_deadline):
            return
        if i + 1 >= n or now >= self.warm_all_by:
            self.warming = False
            self.warm_note = ""
            self.index = 0
            self.last_switch = time.time()
            self.chrome.activate(self.tabs[0]["target_id"])
            log.info("[%s] priming complete: %d tabs triggered", self.id, n)
            return
        self.warm_i = i + 1
        nxt = self.tabs[self.warm_i]
        self.warm_note = nxt["title"][:48]
        self.chrome.activate(nxt["target_id"])
        self.index = self.warm_i
        self.last_switch = time.time()
        self.warm_deadline = time.time() + self.PRIME_MAX

    # --------------------------- view enforcement ---------------------------
    def ensure_keep_live(self):
        if not self.tabs:
            return
        tab = self.tabs[self.index]
        if not tab.get("keep_live") or tab["url"].startswith("http://127.0.0.1:"):
            return
        tid = tab["target_id"]
        if self.live_done.get(tid):
            return
        if self.chrome.eval(tid, JS_KEEP_LIVE) in ("installed", "already"):
            self.live_done[tid] = True

    def enforce_view(self):
        """Hold the visible tab at its locked zoom and scroll. Grafana reflows
        repeatedly while panels load, so a single scroll never sticks. Skipped
        while paused so manual adjustment is possible."""
        if self.paused or not self.tabs:
            return
        tab = self.tabs[self.index]
        tid = tab["target_id"]

        z = float(tab.get("zoom") or 1.0)
        if abs(self.zoom_done.get(tid, 1.0) - z) > 0.001:
            self.chrome.set_zoom(tid, z)
            self.zoom_done[tid] = z
            self.scroll_state.pop(tid, None)      # zoom reflows; re-assert scroll

        y = tab.get("scroll_y") or 0
        if not y:
            self.scroll_state.pop(tid, None)
            return
        if time.time() - self.last_switch < (tab.get("scroll_delay") or 4):
            return
        if self.scroll_state.get(tid) and not self.due("scroll:" + tid, self.SCROLL_EVERY):
            return
        try:
            info = json.loads(self.chrome.set_scroll(tid, y))
        except (TypeError, ValueError):
            info = None
        ok = bool(info and info.get("ok"))
        if ok != self.scroll_state.get(tid):
            self.scroll_state[tid] = ok

    def show(self, i, reload_it=None):
        with self.lock:
            if not self.tabs:
                return
            self.index = i % len(self.tabs)
            tab = self.tabs[self.index]
            if reload_it is None and self.playlist_id:
                with db() as c:
                    pl = c.execute("SELECT reload_on_show FROM playlists WHERE id=?",
                                   (self.playlist_id,)).fetchone()
                reload_it = bool(pl and pl["reload_on_show"])
            if reload_it:
                self.chrome.navigate(tab["target_id"], tab["url"])
            self.chrome.activate(tab["target_id"])
            self.last_switch = time.time()
            self.scroll_state.pop(tab["target_id"], None)
            # the clip restarts from 0 on becoming visible, so the old reading
            # (often ended=True) must not be trusted
            self.vid_state.pop(tab["target_id"], None)

    def step(self, d):
        self.show(self.index + d)

    def is_video(self, tab):
        return (tab or {}).get("media_kind") == "video"

    def should_advance(self):
        """Photos and dashboards use the dwell. Videos play to completion: we ask
        the page whether the element has ended rather than trusting a duration
        recorded at upload time."""
        if not self.tabs or len(self.tabs) < 2:
            return False
        tab = self.tabs[self.index]
        elapsed = time.time() - self.last_switch
        if self.is_video(tab):
            tid = tab["target_id"]
            if elapsed < 1.0:
                return False
            if self.due("vid:" + tid, 0.5):
                # keyed per tab: a single shared field let a finished clip's
                # ended=True leak into the next tab and cascade the rotation
                self.vid_state[tid] = self.chrome.video_state(tid)
            st = self.vid_state.get(tid)
            if st and st.get("video"):
                if st.get("ended"):
                    return True
                dur = st.get("dur") or 0
                if dur and st.get("t", 0) >= dur - 0.35:
                    return True
                # a stalled or unplayable clip must not wedge the rotation
                return elapsed > max(30.0, dur + 20.0)
            return elapsed > 30.0
        return elapsed >= max(3, int(tab.get("dwell") or 30))

    def tick_fast(self):
        """Cheap per-tick work: CDP health, rebuilds, view enforcement."""
        up = (self.chrome.alive() if (self.due("alive", self.ALIVE_EVERY)
                                      or not self.chrome_up) else self.chrome_up)
        if up != self.chrome_up:
            log.info("[%s] chromium %s", self.id, "connected" if up else "gone")
            self.chrome_up = up
            if up:
                self.tabs = []
                self.pending = self.playlist_id or 0
        if not up:
            return
        cur = self.chrome.pages() if self.due("pages", self.PAGES_EVERY) else None
        if self.tabs and cur is not None:
            live = {t["id"] for t in cur}
            if not any(t["target_id"] in live for t in self.tabs):
                log.info("[%s] tabs went stale; rebuilding", self.id)
                self.tabs = []
                self.pending = self.playlist_id or 0
        if self.pending is not None:
            target = self.pending or None
            self.pending = None
            self.rebuild(target)
        self.ensure_keep_live()
        self.enforce_view()
        if self.warming:
            self.prime_step()

    def status(self):
        cur = self.tabs[self.index] if self.tabs else None
        dwell = int(cur.get("dwell") or 0) if cur else 0
        left = 0
        if cur:
            if self.is_video(cur):
                st = self.vid_state.get(cur["target_id"]) or {}
                left = max(0, int(round((st.get("dur") or 0) - (st.get("t") or 0))))
            else:
                left = max(0, int(dwell - (time.time() - self.last_switch)))
        return {
            "id": self.id, "chrome_up": self.chrome_up, "cdp_port": self.chrome.port,
            "playlist_id": self.playlist_id, "playlist_name": self.playlist_name,
            "idle": self.playlist_id is None, "paused": self.paused,
            "index": self.index, "tab_count": len(self.tabs),
            "current": ({"title": cur["title"], "url": cur["url"],
                         "item_id": cur.get("item_id"), "zoom": cur.get("zoom", 1.0),
                         "scroll_y": cur.get("scroll_y", 0),
                         "is_video": self.is_video(cur)} if cur else None),
            "seconds_left": left,
            "warming": self.warming, "warm_index": self.warm_i + 1 if self.warming else 0,
            "warm_total": len(self.tabs) if self.warming else 0,
            "warm_label": self.warm_note if self.warming else "",
        }


# ---------------------------------------------------------------------------
# CEC
# ---------------------------------------------------------------------------
def _cec(args, timeout=20):
    cmd = ["cec-ctl", "-d", CEC_DEV, "--playback"] + args
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.returncode, ((r.stdout or "") + (r.stderr or "")).strip()
    except FileNotFoundError:
        return 127, "cec-ctl not installed"
    except subprocess.TimeoutExpired:
        return 124, "cec-ctl timed out - the TV did not answer"
    except Exception as e:
        return 1, str(e)


def tv_power(action):
    if action == "off":
        rc, out = _cec(["--to", "0", "--standby"])
    elif action == "on":
        rc, out = _cec(["--to", "0", "--image-view-on"])
        rc2, out2 = _cec(["--active-source", "phys-addr=1.0.0.0"])
        rc, out = (rc or rc2), out + "\n" + out2
    else:
        rc, out = _cec(["--to", "0", "--give-device-power-status"])
    power = None
    for line in out.splitlines():
        if "pwr-state" in line.lower():
            power = line.strip()
    return (rc == 0), power, out[-400:]


# ---------------------------------------------------------------------------
# Preview: grim has no jpeg support in this build, so capture PNG and let
# Pillow do the downscale + JPEG encode. Only runs while a page asks for it.
# ---------------------------------------------------------------------------
class Preview:
    WIDTH = 480
    QUALITY = 45

    def __init__(self):
        self.until = {}          # display id -> unix time the request expires

    def enable(self, display, seconds=35):
        self.until[display] = time.time() + seconds

    def disable(self, display):
        self.until.pop(display, None)

    def wanted(self, display):
        return time.time() < self.until.get(display, 0)

    def capture(self, display):
        try:
            from PIL import Image
        except ImportError:
            return None, "Pillow not installed"
        try:
            r = subprocess.run(["grim", "-o", display, "-"], capture_output=True,
                               timeout=15, env=WAYLAND_ENV)
            if r.returncode != 0 or not r.stdout:
                return None, (r.stderr or b"").decode()[:120] or "grim failed"
            im = Image.open(io.BytesIO(r.stdout))
            im = im.convert("RGB")
            w, h = im.size
            im = im.resize((self.WIDTH, max(1, int(h * self.WIDTH / w))), Image.BILINEAR)
            buf = io.BytesIO()
            im.save(buf, "JPEG", quality=self.QUALITY, optimize=True)
            return buf.getvalue(), None
        except Exception as e:
            return None, str(e)


preview = Preview()


# ---------------------------------------------------------------------------
# Display manager: owns one Rotator per display and keeps mirror groups in step
# ---------------------------------------------------------------------------
class Manager(threading.Thread):
    TICK = float(os.environ.get("WALLBOARD_TICK", "0.25"))
    SYNC_EVERY, SCHED_EVERY, TV_EVERY = 5.0, 2.0, 5.0

    def __init__(self):
        super().__init__(daemon=True)
        self.ticker = None          # {"run_id","text","repeats","started","cfg"}
        self.rot = {}
        self.present = {}
        self.tv_desired = None
        self.tv_power_str = None
        self._t = {}

    def due(self, name, every):
        now = time.time()
        if now - self._t.get(name, 0.0) >= every:
            self._t[name] = now
            return True
        return False

    def ensure(self):
        """One Rotator per output that is actually PRESENT.

        Driving this off the displays table alone was the bug behind the endless
        "tabs went stale; rebuilding" loop: a stale NOOP-1 row survived an unplug
        and mapped to the same CDP port as HDMI-A-1, so two rotators fought over
        one Chromium, each closing the other's tabs and re-priming forever.
        """
        self.present = sync_displays()
        with db() as c:
            enabled = {r["id"] for r in c.execute("SELECT id FROM displays WHERE enabled=1")}

        # a present output qualifies if it is an enabled display, or is the
        # headless fallback (so a Pi with no monitor still works)
        cands = [o for o in self.present
                 if o in enabled or self.present[o].get("synthetic")]

        # resolve CDP port collisions, preferring real outputs over synthetic
        chosen, by_port = {}, {}
        for oid in sorted(cands, key=lambda x: (bool(self.present[x].get("synthetic")), x)):
            port = cdp_port_for(oid)
            if port in by_port:
                log.warning("display %s shares cdp %d with %s; ignoring %s",
                            oid, port, by_port[port], oid)
                continue
            by_port[port] = oid
            chosen[oid] = port

        for oid, port in chosen.items():
            if oid not in self.rot:
                self.rot[oid] = Rotator(oid)
                log.info("display attached: %s (cdp %d)", oid, port)
        for gone in [d for d in self.rot if d not in chosen]:
            log.info("display detached: %s", gone)
            self.rot.pop(gone, None)

    def desired_for(self, display):
        """Manual pin wins, else the highest-priority schedule window."""
        with db() as c:
            row = c.execute("SELECT override FROM displays WHERE id=?", (display,)).fetchone()
        ov = (row["override"] if row else "") or ""
        if ov:
            try:
                o = json.loads(ov)
                return o.get("playlist_id") or None
            except ValueError:
                pass
        now = datetime.now()
        dow, hm = now.weekday(), now.strftime("%H:%M")
        with db() as c:
            rows = c.execute(
                "SELECT s.* FROM schedules s JOIN playlists p ON p.id=s.playlist_id "
                "WHERE s.enabled=1 AND p.enabled=1 AND (s.display_id IS NULL OR s.display_id=?) "
                "ORDER BY s.priority DESC, s.id", (display,)).fetchall()
        for s in rows:
            days = [d.strip() for d in s["days"].split(",") if d.strip() != ""]
            if str(dow) not in days:
                continue
            a, b = s["start_time"], s["end_time"]
            if (a <= hm < b) if a <= b else (hm >= a or hm < b):
                return s["playlist_id"]
        return None

    def groups(self):
        """{group_name: [display ids]} for displays that are mirroring."""
        with db() as c:
            rows = c.execute("SELECT id,mirror_group FROM displays "
                             "WHERE enabled=1 AND mirror_group IS NOT NULL "
                             "AND mirror_group<>''").fetchall()
        g = {}
        for r in rows:
            g.setdefault(r["mirror_group"], []).append(r["id"])
        return {k: sorted(v) for k, v in g.items() if len(v) > 1}

    def ticker_start(self, repeats=None, source="manual", drain=False):
        """Turn the queue into a batch and display it.

        A manual push DRAINS the queue: the batch it produced is persisted and
        keeps displaying, and the queue is left empty so the same announcement
        cannot be pushed twice by accident. A scheduled run does not drain, so a
        recurring window keeps working week after week.
        """
        rows = [m for m in ticker_messages(True) if m["text"].strip()]
        if not rows:
            raise ValueError("the queue is empty - add a message first")
        msgs = [m["text"].strip() for m in rows]
        cfg = ticker_config()
        text = cfg["separator"].join(msgs)
        n = max(1, int(repeats if repeats else cfg["repeats"]))
        run_id = secrets.token_hex(4)
        try:                       # a leftover file must not kill the new run
            os.path.exists(TICKER_DONE_FILE) and os.unlink(TICKER_DONE_FILE)
        except OSError:
            pass
        with db() as c:
            c.execute("UPDATE ticker_batches SET active=0 WHERE active=1")
            bid = c.execute("INSERT INTO ticker_batches(run_id,text,items,repeats,source,started,active)"
                            " VALUES(?,?,?,?,?,?,1)",
                            (run_id, text, json.dumps(msgs), n, source,
                             datetime.now().strftime("%Y-%m-%d %H:%M:%S"))).lastrowid
            if drain:
                for m in rows:
                    c.execute("DELETE FROM ticker_messages WHERE id=?", (m["id"],))
        self.ticker = {"run_id": run_id, "text": text, "repeats": n,
                       "started": time.time(), "count": len(msgs),
                       "source": source, "cfg": cfg, "batch_id": bid}
        log.info("ticker batch %d (%s): %d message(s) x%d passes%s",
                 bid, source, len(msgs), n, " [queue drained]" if drain else "")
        return {"run_id": run_id, "batch_id": bid, "messages": len(msgs),
                "repeats": n, "drained": bool(drain)}

    def ticker_stop(self):
        if self.ticker:
            with db() as c:
                c.execute("UPDATE ticker_batches SET active=0 WHERE active=1")
        self.ticker = None
        return {}

    def ticker_resume(self):
        """Reload an in-flight batch after a restart, but only a recent one -
        a stale bar reappearing hours later would be worse than losing it."""
        with db() as c:
            r = c.execute("SELECT * FROM ticker_batches WHERE active=1 "
                          "ORDER BY id DESC LIMIT 1").fetchone()
        if not r:
            return
        try:
            age = (datetime.now() - datetime.strptime(r["started"], "%Y-%m-%d %H:%M:%S")).total_seconds()
        except Exception:
            age = 1e9
        if age > 900:
            with db() as c:
                c.execute("UPDATE ticker_batches SET active=0 WHERE id=?", (r["id"],))
            return
        self.ticker = {"run_id": r["run_id"], "text": r["text"], "repeats": r["repeats"],
                       "started": time.time(), "count": len(json.loads(r["items"] or "[]")),
                       "source": r["source"] + " (resumed)", "cfg": ticker_config(),
                       "batch_id": r["id"]}
        log.info("ticker batch %d resumed after restart (%.0fs old)", r["id"], age)

    def ticker_tick(self):
        """Fire due schedules, and expire a run that has clearly finished.

        The overlay owns the animation and counts passes, so the only job here is
        a generous backstop: if a run has been up far longer than its passes could
        possibly need, drop it rather than leaving a bar on screen forever.
        """
        t = self.ticker
        if t:
            # The overlay tells us the moment it has finished scrolling; without
            # this the run lingered for tens of seconds after the bar was gone,
            # so the page still said "on screen" and the batch stayed active.
            try:
                if os.path.exists(TICKER_DONE_FILE):
                    with open(TICKER_DONE_FILE) as fh:
                        done = json.load(fh)
                    os.unlink(TICKER_DONE_FILE)
                    if done.get("run_id") == t["run_id"]:
                        log.info("ticker batch %s finished (overlay reported)",
                                 t.get("batch_id"))
                        with db() as c:
                            c.execute("UPDATE ticker_batches SET active=0 WHERE active=1")
                        self.ticker = None
                        return
            except Exception as e:
                log.warning("ticker done-file unreadable: %s", e)
            cfg = t["cfg"]
            # worst case: text is ~30px per character wide, plus a screen width
            span = (len(t["text"]) * cfg["font_size"] + 4000) / max(20, cfg["speed"])
            # backstop only, for when the overlay is not running at all
            if time.time() - t["started"] > span * t["repeats"] + 60:
                log.info("ticker batch %s expired (no completion report)",
                         t.get("batch_id"))
                with db() as c:
                    c.execute("UPDATE ticker_batches SET active=0 WHERE active=1")
                self.ticker = None

        now = datetime.now()
        today = now.strftime("%Y-%m-%d")
        hm = now.strftime("%H:%M")
        dow = str(now.weekday())
        with db() as c:
            rows = c.execute("SELECT * FROM ticker_schedules WHERE enabled=1 ORDER BY id").fetchall()
        for r in rows:
            stamp = "%s %s" % (today, r["at_time"])
            if r["last_run"] == stamp:
                continue                       # already fired this minute
            if r["at_time"] != hm:
                continue
            if r["kind"] == "oneoff":
                if r["at_date"] != today:
                    continue
            elif dow not in [d.strip() for d in r["days"].split(",") if d.strip()]:
                continue
            with db() as c:
                c.execute("UPDATE ticker_schedules SET last_run=? WHERE id=?", (stamp, r["id"]))
                if r["kind"] == "oneoff":
                    c.execute("UPDATE ticker_schedules SET enabled=0 WHERE id=?", (r["id"],))
            try:
                self.ticker_start(r["repeats"], source="schedule %d" % r["id"],
                                  drain=False)
            except ValueError as e:
                log.warning("scheduled ticker skipped: %s", e)

    def tv_tick(self):
        now = datetime.now()
        dow, hm = now.weekday(), now.strftime("%H:%M")
        want = None
        with db() as c:
            rows = c.execute("SELECT * FROM tv_schedule WHERE enabled=1 ORDER BY id").fetchall()
        for r in rows:
            days = [d.strip() for d in r["days"].split(",") if d.strip() != ""]
            if str(dow) not in days:
                continue
            a, b = r["on_time"], r["off_time"]
            want = "on" if ((a <= hm < b) if a <= b else (hm >= a or hm < b)) else "off"
        if want is None or want == self.tv_desired:
            if want is not None:
                self.tv_desired = want
            return
        self.tv_desired = want
        log.info("tv schedule -> %s", want)
        threading.Thread(target=tv_power, args=(want,), daemon=True).start()

    def run(self):
        while True:
            try:
                self.tick()
            except Exception:
                log.exception("manager tick failed")
            time.sleep(self.TICK)

    def tick(self):
        if self.due("sync", self.SYNC_EVERY):
            self.ensure()
        if self.due("tv", self.TV_EVERY):
            self.tv_tick()
        if self.due("ticker", 2.0):
            self.ticker_tick()

        mirrors = self.groups()
        followers = {}
        for grp, ids in mirrors.items():
            leader = ids[0]
            for f in ids[1:]:
                followers[f] = leader

        # desired playlist per display (throttled; followers copy their leader)
        if self.due("sched", self.SCHED_EVERY):
            for did, r in self.rot.items():
                want = self.desired_for(followers.get(did, did))
                if (want or None) != r.playlist_id and r.pending is None:
                    r.pending = want or 0

        for r in self.rot.values():
            r.tick_fast()

        # advancement: leaders and independents decide; followers are pushed to
        # the same index in this same iteration so the switch looks simultaneous
        for did, r in self.rot.items():
            if did in followers or r.paused or r.warming:
                continue
            if r.should_advance():
                r.step(1)
        for f, leader in followers.items():
            fr, lr = self.rot.get(f), self.rot.get(leader)
            if not fr or not lr or fr.warming or lr.warming:
                continue
            if fr.tabs and fr.index != lr.index:
                fr.show(lr.index)

        if self.due("tvq", 60.0):
            threading.Thread(target=self._refresh_tv_power, daemon=True).start()

    def _refresh_tv_power(self):
        _, p, _ = tv_power("status")
        if p:
            self.tv_power_str = p

    def status(self):
        rots = list(self.rot.values())
        warming = [r for r in rots if r.warming]
        lead = warming[0] if warming else None
        # Top-level summary is deliberate: the boot shield consumes this file and
        # must not have to understand the per-display structure. Nesting these
        # under `displays` left the shield believing the browser was never up.
        t = self.ticker
        return {
            "ticker": ({"run_id": t["run_id"], "text": t["text"],
                        "repeats": t["repeats"], "count": t["count"],
                        "source": t["source"], "cfg": t["cfg"]} if t else None),
            "tv_power": self.tv_power_str,
            "chrome_up": any(r.chrome_up for r in rots),
            "warming": bool(warming),
            "warm_index": (lead.warm_i + 1) if lead else 0,
            "warm_total": len(lead.tabs) if lead else 0,
            "warm_label": (lead.warm_note if lead else ""),
            "display_count": len(rots),
            "displays": {d: r.status() for d, r in self.rot.items()},
        }


mgr = Manager()


# ---------------------------------------------------------------------------
# Settings (reboot-required) + media
# ---------------------------------------------------------------------------
TICKER_DEFAULTS = {
    "position": "top",       # top | bottom
    "offset": 0,             # px from that edge - lets it sit under a header
    "height": 64,            # bar height in px
    "font_size": 30,         # text height in px
    "text_color": "#ffffff",
    "bar_color": "#0f766e",
    "speed": 140,            # px per second
    "repeats": 2,            # passes per run, overridable per push/schedule
    "separator": "   \u2022   ",
    "bold": 1,
    "fade": 400,             # fade in/out duration in ms; 0 = snap
}


def ticker_config():
    try:
        d = json.loads(get_setting("ticker_config") or "{}")
    except ValueError:
        d = {}
    out = dict(TICKER_DEFAULTS)
    out.update({k: v for k, v in d.items() if k in TICKER_DEFAULTS})
    return out


def ticker_messages(only_enabled=False):
    with db() as c:
        q = "SELECT * FROM ticker_messages%s ORDER BY position, id" % (
            " WHERE enabled=1" if only_enabled else "")
        return [dict(r) for r in c.execute(q)]


SETTINGS_KEYS = ("name", "resolution", "refresh", "hdmi_4kp60", "overscan",
                 "rotation", "hostname", "timezone", "keyboard",
                 "kiosk_autostart", "blanking")


def device_settings():
    try:
        d = json.loads(get_setting("device_settings") or "{}")
    except ValueError:
        d = {}
    d.setdefault("name", socket.gethostname())
    return d


def _probe_duration(path):
    try:
        out = subprocess.run(["ffprobe", "-v", "error", "-show_entries",
                              "format=duration", "-of", "default=nw=1:nk=1", path],
                             capture_output=True, text=True, timeout=45)
        return round(float(out.stdout.strip()), 2)
    except Exception:
        return None


# Direct upload over MQTT. The browser cannot POST to a third-party host from a
# file:// page - Chromium gives such pages an opaque origin and blocks the
# request outright (blocked:origin) - so the file comes to us over the broker the
# page is already connected to. No third party, no expiry, no CORS.
UPLOADS = {}
CHUNK_TIMEOUT = 600.0


def upload_begin(d):
    reap_uploads()
    uid = str(d.get("upload_id") or secrets.token_hex(8))
    name = os.path.basename(str(d.get("filename") or "upload"))
    size = int(d.get("size") or 0)
    pid = int(d["playlist_id"])
    ext = os.path.splitext(name)[1].lower()
    if ext not in IMAGE_EXT and ext not in VIDEO_EXT:
        raise ValueError("unsupported extension %r" % ext)
    if size > MAX_UPLOAD:
        raise ValueError("file is %d MB; limit is %d MB"
                         % (size // 1048576, MAX_UPLOAD // 1048576))
    free = shutil.disk_usage(MEDIA_DIR).free
    if free - size < MIN_FREE_BYTES:
        raise ValueError("not enough free space (%d MB free)" % (free // 1048576))
    path = os.path.join(MEDIA_DIR, ".upload-" + uid)
    UPLOADS[uid] = {"path": path, "name": name, "size": size, "pid": pid,
                    "got": 0, "seq": 0, "fh": open(path, "wb"),
                    "started": time.time(), "touched": time.time()}
    log.info("upload begin: %s (%.1f MB) -> playlist %s", name, size / 1048576.0, pid)
    # 256 KB chunks with a client-side pipeline: 48 KB chunks each waiting for
    # their own ack capped throughput at ~0.2 MB/s on a LAN.
    return {"upload_id": uid, "chunk_size": 256 * 1024}


def upload_chunk(d):
    uid = str(d.get("upload_id"))
    u = UPLOADS.get(uid)
    if not u:
        raise ValueError("unknown upload_id (did it time out?)")
    seq = int(d.get("seq", -1))
    if seq != u["seq"]:
        raise ValueError("out of order chunk: expected %d got %d" % (u["seq"], seq))
    blob = base64.b64decode(d.get("b64") or "")
    u["got"] += len(blob)
    if u["got"] > MAX_UPLOAD:
        abort_upload(uid)
        raise ValueError("upload exceeded the size limit")
    u["fh"].write(blob)
    u["seq"] += 1
    u["touched"] = time.time()
    return {"upload_id": uid, "got": u["got"], "seq": u["seq"]}


def upload_end(d):
    uid = str(d.get("upload_id"))
    u = UPLOADS.pop(uid, None)
    if not u:
        raise ValueError("unknown upload_id")
    u["fh"].close()
    got, want = u["got"], u["size"]
    if want and got != want:
        os.unlink(u["path"])
        raise ValueError("size mismatch: got %d of %d bytes" % (got, want))
    if not got:
        os.unlink(u["path"])
        raise ValueError("received 0 bytes")
    ext = os.path.splitext(u["name"])[1].lower()
    kind = "image" if ext in IMAGE_EXT else "video"
    stored = secrets.token_hex(16) + ext
    os.replace(u["path"], os.path.join(MEDIA_DIR, stored))
    dur = _probe_duration(os.path.join(MEDIA_DIR, stored)) if kind == "video" else None
    with db() as c:
        mid = c.execute("INSERT INTO media(stored,original,mime,kind,bytes,duration,created)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (stored, u["name"], mimetypes.guess_type(u["name"])[0] or "",
                         kind, got, dur,
                         datetime.now().strftime("%Y-%m-%d %H:%M"))).lastrowid
        pos = c.execute("SELECT COALESCE(MAX(position),0)+1 p FROM items WHERE playlist_id=?",
                        (u["pid"],)).fetchone()["p"]
        # videos run to completion, so no dwell; photos inherit the playlist's
        c.execute("INSERT INTO items(playlist_id,url,title,dwell,position,media_id)"
                  " VALUES(?,?,?,?,?,?)",
                  (u["pid"], "http://127.0.0.1:%d/media/view/%d" % (PORT, mid),
                   os.path.splitext(u["name"])[0][:60], None, pos, mid))
    touch_playlist(u["pid"])
    log.info("upload complete: %s (%s, %.1f MB, dur=%s) in %.1fs",
             u["name"], kind, got / 1048576.0, dur, time.time() - u["started"])
    return {"media_id": mid, "kind": kind, "bytes": got, "duration": dur}


def abort_upload(uid):
    u = UPLOADS.pop(uid, None)
    if not u:
        return
    try:
        u["fh"].close()
        os.unlink(u["path"])
    except Exception:
        pass


def reap_uploads():
    """Drop half-finished uploads so a closed browser tab cannot leak temp files."""
    for uid in [k for k, v in UPLOADS.items() if time.time() - v["touched"] > CHUNK_TIMEOUT]:
        log.warning("upload %s timed out; discarding", uid)
        abort_upload(uid)


def media_from_url(pid, url, title=None):
    """The control page uploads to a temp host and hands us the URL; we pull it
    down and keep a local copy, so the link expiring does not matter."""
    if not url.startswith(("http://", "https://")):
        raise ValueError("url must be http(s)")
    if shutil.disk_usage(MEDIA_DIR).free < MIN_FREE_BYTES:
        raise ValueError("device is low on disk")
    clean = url.split("?")[0]
    ext = os.path.splitext(clean)[1].lower()
    if ext not in IMAGE_EXT and ext not in VIDEO_EXT:
        raise ValueError("unsupported extension %r" % ext)
    kind = "image" if ext in IMAGE_EXT else "video"
    name = title or os.path.basename(clean) or "download"
    stored = secrets.token_hex(16) + ext
    dest = os.path.join(MEDIA_DIR, stored)
    total = 0
    with requests.get(url, stream=True, timeout=120, allow_redirects=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(1 << 16):
                total += len(chunk)
                if total > MAX_UPLOAD:
                    fh.close()
                    os.unlink(dest)
                    raise ValueError("file exceeds %d MB" % (MAX_UPLOAD // 1048576))
                fh.write(chunk)
    if not total:
        os.unlink(dest)
        raise ValueError("downloaded 0 bytes")
    dur = _probe_duration(dest) if kind == "video" else None
    with db() as c:
        mid = c.execute("INSERT INTO media(stored,original,mime,kind,bytes,duration,created)"
                        " VALUES(?,?,?,?,?,?,?)",
                        (stored, name, mimetypes.guess_type(name)[0] or "", kind,
                         total, dur, datetime.now().strftime("%Y-%m-%d %H:%M"))).lastrowid
        pos = c.execute("SELECT COALESCE(MAX(position),0)+1 p FROM items WHERE playlist_id=?",
                        (pid,)).fetchone()["p"]
        # videos run to completion, so leave dwell NULL and let the rotator watch
        # the element; only photos are governed by the playlist dwell
        c.execute("INSERT INTO items(playlist_id,url,title,dwell,position,media_id)"
                  " VALUES(?,?,?,?,?,?)",
                  (pid, "http://127.0.0.1:%d/media/view/%d" % (PORT, mid),
                   os.path.splitext(name)[0][:60], None, pos, mid))
    log.info("media pulled: %s (%s, %.1f MB, dur=%s)", name, kind, total / 1048576.0, dur)
    return {"media_id": mid, "kind": kind, "bytes": total, "duration": dur}


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------
import paho.mqtt.client as mqtt

UID = None          # set in main once the DB exists
T = {}


def build_topics():
    global T
    T = {"ann": "%s/%s/announce" % (ROOT, UID),
         "status": "%s/%s/status" % (ROOT, UID),
         "library": "%s/%s/library" % (ROOT, UID),
         "displays": "%s/%s/displays" % (ROOT, UID),
         "settings": "%s/%s/settings" % (ROOT, UID),
         "cmd": "%s/%s/cmd" % (ROOT, UID),
         "ack": "%s/%s/ack" % (ROOT, UID)}


# Subprocess lookups are cached: announce_payload() runs inside the MQTT
# on_message callback, and shelling out there blocked the network loop for
# seconds, stalling every subsequent command.
_CACHE = {}


def cached(key, ttl, fn):
    hit = _CACHE.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    try:
        val = fn()
    except Exception:
        val = hit[1] if hit else None
    _CACHE[key] = (time.time(), val)
    return val


def tailscale_ip():
    def _go():
        out = subprocess.run(["tailscale", "ip", "-4"], capture_output=True,
                             text=True, timeout=6).stdout.strip().splitlines()
        return out[0] if out else None
    return cached("ts_ip", 300, _go)


def lan_ip():
    return cached("lan_ip", 60,
                  lambda: (subprocess.run(["hostname", "-I"], capture_output=True,
                                          text=True, timeout=5).stdout.split() or [""])[0])


def device_model():
    return cached("model", 86400,
                  lambda: open("/proc/device-tree/model").read().strip("\x00").strip())


def announce_payload():
    st = device_settings()
    model = device_model() or "unknown"
    with db() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM displays ORDER BY id")]
    for r in rows:
        r["present"] = r["id"] in mgr.present
        r["mode"] = (mgr.present.get(r["id"]) or {}).get("mode")
    return {"uid": UID, "schema": 2, "name": st.get("name"),
            "hostname": socket.gethostname(), "model": model,
            "ip": lan_ip(), "tailscale_ip": tailscale_ip(), "app": "wallboard",
            "caps": ["displays", "playlists", "media", "video", "cec", "zoom",
                     "scroll", "settings", "preview", "mirror"],
            "displays": rows}


def library_payload():
    with db() as c:
        pls = [dict(r) for r in c.execute("SELECT * FROM playlists ORDER BY id")]
        for p in pls:
            p["items"] = [dict(r) for r in c.execute(
                "SELECT * FROM items WHERE playlist_id=? ORDER BY position, id", (p["id"],))]
            p["schedules"] = [dict(r) for r in c.execute(
                "SELECT * FROM schedules WHERE playlist_id=? ORDER BY priority DESC, id",
                (p["id"],))]
        media = [dict(r) for r in c.execute(
            "SELECT m.*, (SELECT COUNT(*) FROM items i WHERE i.media_id=m.id) uses "
            "FROM media m ORDER BY m.id DESC")]
        tv = [dict(r) for r in c.execute("SELECT * FROM tv_schedule ORDER BY id")]
    with db() as c:
        tsched = [dict(r) for r in c.execute("SELECT * FROM ticker_schedules ORDER BY id")]
    with db() as c:
        b = c.execute("SELECT * FROM ticker_batches ORDER BY id DESC LIMIT 5").fetchall()
    batches = []
    for r in b:
        x = dict(r)
        try:
            x["items"] = json.loads(x["items"] or "[]")
        except ValueError:
            x["items"] = []
        batches.append(x)
    return {"playlists": pls, "media": media, "tv_schedule": tv,
            "ticker": {"config": ticker_config(), "messages": ticker_messages(),
                       "schedules": tsched, "defaults": TICKER_DEFAULTS,
                       "batches": batches},
            "free_mb": shutil.disk_usage(MEDIA_DIR).free // (1024 * 1024),
            "max_upload_mb": MAX_UPLOAD // (1024 * 1024)}


def displays_payload():
    with db() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM displays ORDER BY id")]
    for r in rows:
        r["present"] = r["id"] in mgr.present
        r["mode"] = (mgr.present.get(r["id"]) or {}).get("mode")
        r["cdp_port"] = cdp_port_for(r["id"])
        try:
            r["override_playlist"] = (json.loads(r["override"]).get("playlist_id")
                                      if r["override"] else None)
        except ValueError:
            r["override_playlist"] = None
    return {"displays": rows}


def settings_payload():
    return {"current": device_settings(), "keys": list(SETTINGS_KEYS),
            "pending": os.path.exists(PENDING_FILE),
            "outputs": list(mgr.present.values())}


class Bus:
    def __init__(self):
        self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                             client_id="wallboard-%s" % UID)
        self.c.on_connect = self.on_connect
        self.c.on_message = self.on_message
        self.c.will_set(T["status"], json.dumps({"uid": UID, "online": False}),
                        qos=1, retain=True)

    def start(self):
        while True:
            try:
                self.c.connect(BROKER, BROKER_PORT, keepalive=30)
                break
            except Exception as e:
                log.warning("broker %s:%s unreachable (%s)", BROKER, BROKER_PORT, e)
                time.sleep(5)
        self.c.loop_start()
        threading.Thread(target=self.publisher, daemon=True).start()
        threading.Thread(target=self.previewer, daemon=True).start()

    def on_connect(self, client, userdata, flags, rc, props=None):
        log.info("mqtt connected as %s", UID)
        client.subscribe(T["cmd"], qos=1)
        # one-time hygiene: delete retained messages from hostname-keyed ids used
        # by earlier builds, so no ghost device can linger
        for old in LEGACY_IDS:
            if old != UID:
                for leaf in ("announce", "status", "playlists", "library",
                             "displays", "settings", "ack", "cmd"):
                    client.publish("%s/%s/%s" % (ROOT, old, leaf), None, qos=1, retain=True)
        self.pub_all()

    def pub(self, topic, obj, retain=False, qos=0):
        try:
            self.c.publish(topic, json.dumps(obj, default=str), qos=qos, retain=retain)
        except Exception as e:
            log.warning("publish %s failed: %s", topic, e)

    def pub_all(self):
        self.pub(T["ann"], announce_payload(), retain=True, qos=1)
        self.pub(T["library"], library_payload(), retain=True, qos=1)
        self.pub(T["displays"], displays_payload(), retain=True, qos=1)
        self.pub(T["settings"], settings_payload(), retain=True, qos=1)

    def publisher(self):
        while True:
            try:
                st = mgr.status()
                st.update({"uid": UID, "online": True,
                           "name": device_settings().get("name")})
                blob = json.dumps(st, sort_keys=True, default=str)
                self.c.publish(T["status"], blob, qos=0, retain=True)
                try:
                    tmp = STATE_FILE + ".tmp"
                    with open(tmp, "w") as fh:
                        fh.write(blob)
                    os.replace(tmp, STATE_FILE)
                except Exception:
                    pass
            except Exception:
                log.exception("status publish failed")
            time.sleep(STATUS_INTERVAL)

    def previewer(self):
        """Only captures while a page has asked for it, and the request expires
        so a closed tab cannot leave the Pi encoding frames forever."""
        while True:
            try:
                for did in list(mgr.rot.keys()):
                    if not preview.wanted(did):
                        continue
                    img, err = preview.capture(did)
                    topic = "%s/%s/preview/%s" % (ROOT, UID, did)
                    if img:
                        self.c.publish(topic, json.dumps({
                            "display": did, "ts": int(time.time()),
                            "jpeg_b64": base64.b64encode(img).decode()}), qos=0)
                    else:
                        self.c.publish(topic, json.dumps({"display": did, "error": err}), qos=0)
            except Exception:
                log.exception("preview failed")
            time.sleep(3.0)

    def on_message(self, client, userdata, msg):
        try:
            d = json.loads(msg.payload.decode() or "{}")
        except ValueError:
            return
        rid, action = d.get("rid"), d.get("action")
        try:
            res = dispatch(action, d) or {}
            self.pub(T["ack"], {"rid": rid, "action": action, "ok": True, **res})
        except Exception as e:
            log.exception("command %s failed", action)
            self.pub(T["ack"], {"rid": rid, "action": action, "ok": False, "error": str(e)})
        finally:
            # media_chunk fires hundreds of times per upload; republishing the
            # whole library each time would swamp the broker
            if action == "media_chunk" or action == "media_begin":
                pass
            elif action and action.startswith(("playlist", "item", "schedule",
                                               "media", "tv_sched", "ticker")):
                self.pub(T["library"], library_payload(), retain=True, qos=1)
            if action and action.startswith(("display", "mirror")):
                self.pub(T["displays"], displays_payload(), retain=True, qos=1)
                self.pub(T["ann"], announce_payload(), retain=True, qos=1)
            if action and action.startswith("settings"):
                self.pub(T["settings"], settings_payload(), retain=True, qos=1)


bus = None


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def rot_for(d):
    did = d.get("display")
    if did:
        if did in mgr.rot:
            return mgr.rot[did]
        raise ValueError("no such display %r" % did)
    if mgr.rot:
        return mgr.rot[sorted(mgr.rot)[0]]
    raise ValueError("no displays registered yet")


def set_override(display, playlist_id):
    with db() as c:
        c.execute("UPDATE displays SET override=? WHERE id=?",
                  (json.dumps({"playlist_id": playlist_id}) if playlist_id is not None else "",
                   display))


def touch_playlist(pid):
    """Any display currently showing this playlist needs new tabs."""
    for r in mgr.rot.values():
        if r.playlist_id == pid:
            r.pending = pid


def dispatch(action, d):
    # ------------------------ transport (per display) ------------------------
    if action in ("play", "idle", "auto", "next", "prev", "goto", "reload",
                  "rebuild", "pause"):
        r = rot_for(d)
        if action == "play":
            pid = int(d["playlist_id"])
            set_override(r.id, pid)
            r.pending = pid
        elif action == "idle":
            set_override(r.id, 0)
            r.pending = 0
        elif action == "auto":
            set_override(r.id, None)
            r.pending = mgr.desired_for(r.id) or 0
        elif action == "next":
            r.step(1)
        elif action == "prev":
            r.step(-1)
        elif action == "goto":
            r.show(int(d.get("index", 0)))
        elif action == "reload":
            r.show(r.index, reload_it=True)
        elif action == "rebuild":
            r.pending = r.playlist_id or 0
        elif action == "pause":
            r.paused = not r.paused
            return {"paused": r.paused, "display": r.id}
        return {"display": r.id}

    if action == "request_state":
        bus.pub_all()
        return {}

    # ----------------------------- displays -----------------------------
    if action == "display_update":
        did = d["id"]
        fields, vals = [], []
        for k in ("label", "enabled", "mirror_group"):
            if k in d:
                fields.append(k + "=?")
                vals.append(d[k] if k != "enabled" else int(d[k]))
        if fields:
            vals.append(did)
            with db() as c:
                c.execute("UPDATE displays SET %s WHERE id=?" % ",".join(fields), vals)
        if "mirror_group" in d:
            mgr._t.pop("sched", None)      # re-resolve immediately
        return {}
    if action == "display_forget":
        with db() as c:
            c.execute("DELETE FROM displays WHERE id=?", (d["id"],))
        mgr.rot.pop(d["id"], None)
        return {}

    # -------------------- interactive input into a tab --------------------
    # Lets the control page drive a real login form without VNC. Coordinates
    # arrive normalised (0..1) so the page does not need to know the panel size.
    if action in ("tab_click", "tab_text", "tab_key", "tab_wheel"):
        r = rot_for(d)
        if not r.tabs:
            raise ValueError("that display has no tabs")
        tab = r.tabs[r.index]
        tid = tab["target_id"]

        if action == "tab_click":
            w, h = r.chrome.viewport(tid)
            if not w:
                raise ValueError("could not read the tab viewport")
            x = max(0, min(w - 1, int(float(d.get("nx", 0.5)) * w)))
            y = max(0, min(h - 1, int(float(d.get("ny", 0.5)) * h)))
            clicks = int(d.get("clicks", 1))
            base = {"x": x, "y": y, "button": "left", "clickCount": clicks}
            res = r.chrome.cdp(tid, [
                ("Input.dispatchMouseEvent", dict(type="mouseMoved", **{k: v for k, v in base.items() if k != "clickCount"})),
                ("Input.dispatchMouseEvent", dict(type="mousePressed", buttons=1, **base)),
                ("Input.dispatchMouseEvent", dict(type="mouseReleased", buttons=0, **base)),
            ])
            if res is None:
                raise ValueError("input dispatch failed")
            return {"display": r.id, "x": x, "y": y, "viewport": [w, h]}

        if action == "tab_text":
            text = d.get("text") or ""
            if not text:
                raise ValueError("no text")
            # insertText is far more reliable than synthesising per-character
            # key events, and handles non-ASCII correctly
            if r.chrome.cdp(tid, [("Input.insertText", {"text": text})]) is None:
                raise ValueError("input dispatch failed")
            return {"display": r.id, "chars": len(text)}

        if action == "tab_key":
            key = d.get("key") or "Enter"
            spec = {"Enter": (13, "Enter"), "Tab": (9, "Tab"), "Backspace": (8, "Backspace"),
                    "Escape": (27, "Escape"), "ArrowDown": (40, "ArrowDown"),
                    "ArrowUp": (38, "ArrowUp"), "ArrowLeft": (37, "ArrowLeft"),
                    "ArrowRight": (39, "ArrowRight"), "Delete": (46, "Delete")}
            if key not in spec:
                raise ValueError("unsupported key %r" % key)
            vk, code = spec[key]
            common = {"key": key, "code": code, "windowsVirtualKeyCode": vk,
                      "nativeVirtualKeyCode": vk}
            res = r.chrome.cdp(tid, [
                ("Input.dispatchKeyEvent", dict(type="rawKeyDown", **common)),
                ("Input.dispatchKeyEvent", dict(type="keyUp", **common)),
            ])
            if res is None:
                raise ValueError("input dispatch failed")
            return {"display": r.id, "key": key}

        if action == "tab_wheel":
            w, h = r.chrome.viewport(tid)
            if not w:
                raise ValueError("could not read the tab viewport")
            x = max(0, min(w - 1, int(float(d.get("nx", 0.5)) * w)))
            y = max(0, min(h - 1, int(float(d.get("ny", 0.5)) * h)))
            res = r.chrome.cdp(tid, [("Input.dispatchMouseEvent", {
                "type": "mouseWheel", "x": x, "y": y,
                "deltaX": 0, "deltaY": float(d.get("dy", 120))})])
            if res is None:
                raise ValueError("input dispatch failed")
            return {"display": r.id, "dy": d.get("dy")}

    # ----------------------------- preview ------------------------------
    if action == "preview_on":
        r = rot_for(d)
        preview.enable(r.id, float(d.get("seconds", 35)))
        return {"display": r.id}
    if action == "preview_off":
        r = rot_for(d)
        preview.disable(r.id)
        return {"display": r.id}

    # ---------------------------- playlists -----------------------------
    if action == "playlist_create":
        with db() as c:
            pid = c.execute("INSERT INTO playlists(name,dwell) VALUES(?,?)",
                            (d.get("name", "New playlist"),
                             int(d.get("dwell", 30)))).lastrowid
        return {"id": pid}
    if action == "playlist_update":
        pid = int(d["id"])
        f, v = [], []
        for k in ("name", "dwell", "reload_on_show", "enabled", "scroll_delay", "keep_live"):
            if k in d:
                f.append(k + "=?")
                v.append(d[k] if k == "name" else int(d[k]))
        if f:
            v.append(pid)
            with db() as c:
                c.execute("UPDATE playlists SET %s WHERE id=?" % ",".join(f), v)
        for r in mgr.rot.values():
            if r.playlist_id == pid:
                if {"enabled", "keep_live"} & set(d.keys()):
                    r.pending = pid
                else:
                    r.resync_meta()
        return {}
    if action == "playlist_delete":
        pid = int(d["id"])
        with db() as c:
            c.execute("DELETE FROM playlists WHERE id=?", (pid,))
        for r in mgr.rot.values():
            if r.playlist_id == pid:
                r.pending = 0
        return {}

    # ------------------------------- items ------------------------------
    if action == "item_add":
        pid = int(d["playlist_id"])
        url = (d.get("url") or "").strip()
        if not url:
            raise ValueError("url required")
        if not url.startswith(("http://", "https://")):
            url = "https://" + url
        with db() as c:
            pos = c.execute("SELECT COALESCE(MAX(position),0)+1 p FROM items "
                            "WHERE playlist_id=?", (pid,)).fetchone()["p"]
            iid = c.execute("INSERT INTO items(playlist_id,url,title,position) "
                            "VALUES(?,?,?,?)", (pid, url, d.get("title", ""), pos)).lastrowid
        touch_playlist(pid)
        return {"id": iid, "url": url}
    if action == "item_update":
        iid = int(d["id"])
        f, v = [], []
        for k in ("url", "title", "dwell", "enabled", "position", "scroll_y", "zoom"):
            if k in d:
                f.append(k + "=?")
                if k in ("url", "title"):
                    v.append(d[k])
                elif k == "zoom":
                    v.append(max(0.25, min(4.0, float(d[k] or 1.0))))
                elif k == "scroll_y":
                    v.append(int(d[k] or 0))
                else:
                    v.append(None if d[k] in (None, "") else int(d[k]))
        if not f:
            return {}
        v.append(iid)
        with db() as c:
            c.execute("UPDATE items SET %s WHERE id=?" % ",".join(f), v)
            row = c.execute("SELECT playlist_id FROM items WHERE id=?", (iid,)).fetchone()
        pid = row["playlist_id"] if row else None
        if pid:
            if {"url", "enabled", "position"} & set(d.keys()):
                touch_playlist(pid)
            else:
                for r in mgr.rot.values():
                    if r.playlist_id == pid:
                        r.resync_meta()
        return {}
    if action == "item_delete":
        with db() as c:
            row = c.execute("SELECT playlist_id FROM items WHERE id=?", (int(d["id"]),)).fetchone()
            c.execute("DELETE FROM items WHERE id=?", (int(d["id"]),))
        if row:
            touch_playlist(row["playlist_id"])
        return {}
    if action == "item_reorder":
        pid = int(d["playlist_id"])
        with db() as c:
            for pos, iid in enumerate(d.get("ids", [])):
                c.execute("UPDATE items SET position=? WHERE id=? AND playlist_id=?",
                          (pos, int(iid), pid))
        touch_playlist(pid)
        return {}

    # -------------------- live scroll / zoom (no VNC) --------------------
    if action in ("item_scroll", "item_zoom"):
        iid = int(d["id"])
        with db() as c:
            row = c.execute("SELECT scroll_y,zoom FROM items WHERE id=?", (iid,)).fetchone()
        if not row:
            raise ValueError("no such item")
        if action == "item_scroll":
            y = (int(d["y"]) if "y" in d
                 else max(0, int(row["scroll_y"] or 0) + int(d.get("delta", 0))))
            with db() as c:
                c.execute("UPDATE items SET scroll_y=? WHERE id=?", (y, iid))
            out = {"scroll_y": y}
        else:
            z = max(0.25, min(4.0, float(d["zoom"]) if "zoom" in d
                              else float(row["zoom"] or 1.0) + float(d.get("delta", 0))))
            with db() as c:
                c.execute("UPDATE items SET zoom=? WHERE id=?", (z, iid))
            out = {"zoom": round(z, 3)}
        for r in mgr.rot.values():
            r.resync_meta()
            tab = next((t for t in r.tabs if t.get("item_id") == iid), None)
            if tab:
                r.zoom_done.pop(tab["target_id"], None)
                r.scroll_state.pop(tab["target_id"], None)
        return out
    if action == "item_grab_scroll":
        iid = int(d["id"])
        for r in mgr.rot.values():
            tab = next((t for t in r.tabs if t.get("item_id") == iid), None)
            if tab:
                info = r.chrome.read_scroll(tab["target_id"])
                if not info or info.get("max", 0) <= 0:
                    raise ValueError("that page is not scrollable yet")
                y = int(info["top"])
                with db() as c:
                    c.execute("UPDATE items SET scroll_y=? WHERE id=?", (y, iid))
                r.resync_meta()
                return {"scroll_y": y}
        raise ValueError("that item is not loaded on any display")

    # ----------------------------- schedules ----------------------------
    if action == "schedule_add":
        with db() as c:
            sid = c.execute("INSERT INTO schedules(playlist_id,display_id,days,"
                            "start_time,end_time,priority) VALUES(?,?,?,?,?,?)",
                            (int(d["playlist_id"]), d.get("display_id") or None,
                             d.get("days", "0,1,2,3,4,5,6"), d.get("start_time", "08:00"),
                             d.get("end_time", "18:00"), int(d.get("priority", 0)))).lastrowid
        return {"id": sid}
    if action == "schedule_delete":
        with db() as c:
            c.execute("DELETE FROM schedules WHERE id=?", (int(d["id"]),))
        return {}

    # ------------------------------- TV ---------------------------------
    if action in ("tv_on", "tv_off", "tv_status"):
        ok, power, out = tv_power({"tv_on": "on", "tv_off": "off"}.get(action, "status"))
        if action != "tv_status":
            mgr.tv_desired = "on" if action == "tv_on" else "off"
        if power:
            mgr.tv_power_str = power
        return {"ok_cec": ok, "power": power, "output": out}
    if action == "tv_sched_add":
        with db() as c:
            sid = c.execute("INSERT INTO tv_schedule(days,on_time,off_time) VALUES(?,?,?)",
                            (d.get("days", "0,1,2,3,4"), d.get("on_time", "08:00"),
                             d.get("off_time", "19:00"))).lastrowid
        mgr.tv_desired = None
        return {"id": sid}
    if action == "tv_sched_delete":
        with db() as c:
            c.execute("DELETE FROM tv_schedule WHERE id=?", (int(d["id"]),))
        mgr.tv_desired = None
        return {}

    # ------------------------------ ticker ------------------------------
    if action == "ticker_config_set":
        cur = ticker_config()
        for k in TICKER_DEFAULTS:
            if k in d:
                v = d[k]
                if k in ("offset", "height", "font_size", "speed", "repeats",
                         "bold", "fade"):
                    v = int(v or 0)
                cur[k] = v
        cur["height"] = max(12, min(400, cur["height"]))
        cur["font_size"] = max(8, min(cur["height"], cur["font_size"]))
        cur["speed"] = max(20, min(600, cur["speed"]))
        cur["repeats"] = max(1, min(50, cur["repeats"]))
        cur["offset"] = max(0, min(2000, cur["offset"]))
        cur["fade"] = max(0, min(3000, cur["fade"]))
        if cur["position"] not in ("top", "bottom"):
            cur["position"] = "top"
        set_setting("ticker_config", json.dumps(cur))
        return {"config": cur}
    if action == "ticker_msg_add":
        txt = (d.get("text") or "").strip()
        if not txt:
            raise ValueError("message text required")
        with db() as c:
            pos = c.execute("SELECT COALESCE(MAX(position),0)+1 p FROM ticker_messages").fetchone()["p"]
            mid = c.execute("INSERT INTO ticker_messages(text,position,created) VALUES(?,?,?)",
                            (txt, pos, datetime.now().strftime("%Y-%m-%d %H:%M"))).lastrowid
        return {"id": mid}
    if action == "ticker_msg_update":
        f, v = [], []
        for k in ("text", "enabled", "position"):
            if k in d:
                f.append(k + "=?")
                v.append(d[k] if k == "text" else int(d[k]))
        if f:
            v.append(int(d["id"]))
            with db() as c:
                c.execute("UPDATE ticker_messages SET %s WHERE id=?" % ",".join(f), v)
        return {}
    if action == "ticker_msg_delete":
        with db() as c:
            c.execute("DELETE FROM ticker_messages WHERE id=?", (int(d["id"]),))
        return {}
    if action == "ticker_msg_reorder":
        with db() as c:
            for pos, mid in enumerate(d.get("ids", [])):
                c.execute("UPDATE ticker_messages SET position=? WHERE id=?", (pos, int(mid)))
        return {}
    if action == "ticker_push":
        return mgr.ticker_start(d.get("repeats"), source="manual", drain=True)
    if action == "ticker_stop":
        return mgr.ticker_stop()
    if action == "ticker_sched_add":
        kind = d.get("kind", "recurring")
        if kind not in ("recurring", "oneoff"):
            raise ValueError("kind must be recurring or oneoff")
        with db() as c:
            sid = c.execute("INSERT INTO ticker_schedules(kind,days,at_date,at_time,repeats)"
                            " VALUES(?,?,?,?,?)",
                            (kind, d.get("days", "0,1,2,3,4"), d.get("at_date", ""),
                             d.get("at_time", "09:00"),
                             int(d["repeats"]) if d.get("repeats") else None)).lastrowid
        return {"id": sid}
    if action == "ticker_sched_delete":
        with db() as c:
            c.execute("DELETE FROM ticker_schedules WHERE id=?", (int(d["id"]),))
        return {}
    if action == "ticker_sched_update":
        f, v = [], []
        for k in ("enabled", "at_time", "at_date", "days", "repeats", "kind"):
            if k in d:
                f.append(k + "=?")
                v.append(int(d[k]) if k in ("enabled", "repeats") and d[k] not in (None, "") else d[k])
        if f:
            v.append(int(d["id"]))
            with db() as c:
                c.execute("UPDATE ticker_schedules SET %s WHERE id=?" % ",".join(f), v)
        return {}

    # ------------------------------ media -------------------------------
    if action == "media_begin":
        return upload_begin(d)
    if action == "media_chunk":
        return upload_chunk(d)
    if action == "media_end":
        return upload_end(d)
    if action == "media_abort":
        abort_upload(str(d.get("upload_id")))
        return {}
    if action == "media_add_url":
        return media_from_url(int(d["playlist_id"]), d["url"], d.get("title"))
    if action == "media_delete":
        mid = int(d["id"])
        with db() as c:
            row = c.execute("SELECT * FROM media WHERE id=?", (mid,)).fetchone()
            if not row:
                raise ValueError("no such media")
            uses = c.execute("SELECT COUNT(*) n FROM items WHERE media_id=?", (mid,)).fetchone()["n"]
            if uses:
                raise ValueError("still used by %d item(s)" % uses)
            c.execute("DELETE FROM media WHERE id=?", (mid,))
        try:
            os.unlink(os.path.join(MEDIA_DIR, row["stored"]))
        except FileNotFoundError:
            pass
        return {}

    # ----------------------------- settings -----------------------------
    if action == "settings_set":
        cur = device_settings()
        for k in SETTINGS_KEYS:
            if k in d:
                cur[k] = d[k]
        set_setting("device_settings", json.dumps(cur))
        blob = dict(cur)
        outs = sorted(mgr.present) or [o["id"] for o in list_outputs()]
        blob["output"] = d.get("output") or (outs[0] if outs else None)
        blob["user"] = "aies-infra"
        with open(PENDING_FILE, "w") as fh:
            json.dump(blob, fh)
        return {"pending": True}
    if action == "settings_apply":
        if not os.path.exists(PENDING_FILE):
            dispatch("settings_set", {})
        r = subprocess.run(["sudo", "-n", APPLY_HELPER, PENDING_FILE],
                           capture_output=True, text=True, timeout=180)
        out = (r.stdout + r.stderr)[-1500:]
        if r.returncode != 0:
            raise RuntimeError("apply failed: %s" % out)
        os.path.exists(PENDING_FILE) and os.unlink(PENDING_FILE)
        if d.get("reboot"):
            threading.Thread(target=lambda: (time.sleep(2),
                             subprocess.run(["sudo", "-n", "/sbin/reboot"])),
                             daemon=True).start()
        return {"output": out, "rebooting": bool(d.get("reboot"))}
    if action == "restart_app":
        threading.Thread(target=lambda: (time.sleep(1), subprocess.run(
            ["sudo", "-n", "/bin/systemctl", "restart", "wallboard"])), daemon=True).start()
        return {}

    raise ValueError("unknown action %r" % action)


# ---------------------------------------------------------------------------
# Loopback pages (the only HTTP surface; no control API lives here)
# ---------------------------------------------------------------------------
app = Flask(__name__)


@app.get("/idle")
def idle():
    # display name in the title lets a labwc window rule place this window on a
    # specific output when a second monitor is attached
    return render_template("idle.html", display=request.args.get("display", ""))


@app.get("/media/file/<int:mid>")
def media_file(mid):
    with db() as c:
        row = c.execute("SELECT stored,mime FROM media WHERE id=?", (mid,)).fetchone()
    if not row:
        return ("not found", 404)
    return send_from_directory(MEDIA_DIR, row["stored"],
                              mimetype=row["mime"] or None, conditional=True)


@app.get("/media/view/<int:mid>")
def media_view(mid):
    with db() as c:
        row = c.execute("SELECT * FROM media WHERE id=?", (mid,)).fetchone()
    if not row:
        return ("not found", 404)
    return render_template("media.html", m=dict(row), src="/media/file/%d" % mid,
                           fit=request.args.get("fit", "contain"))


if __name__ == "__main__":
    init_db()
    UID = device_uid()
    build_topics()
    sync_displays()
    mgr.ensure()
    mgr.ticker_resume()
    mgr.start()
    bus = Bus()
    bus.start()
    log.info("wallboard up: uid=%s displays=%s", UID, list(mgr.rot))
    app.run(host="127.0.0.1", port=PORT, threaded=True, use_reloader=False)
