#!/usr/bin/env python3
"""
AbimconStudio V3 â License Management Server  v3
Pure Python + boto3 for Cloudflare R2 signed URLs.

New in v3:
  - plan_type (free/pro) per license
  - expiry_date enforcement
  - daily_download_limit per plan
  - Models table (linked to Cloudflare R2)
  - Secure signed-URL download endpoint  POST /api/download-model
  - Download log table for daily-limit tracking
  - Admin: model CRUD, reset daily count, expiry management
"""

import os, sys, json, sqlite3, hmac, hashlib, base64, time, re, uuid, threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta

# ââ Gemini AI (loaded from Railway env var â NEVER hardcoded) ââââââââââââââââââ
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '')
GEMINI_CHAT_MODEL  = 'gemini-2.5-flash'
GEMINI_IMAGE_MODEL = 'gemini-2.5-flash-image'  # fallback / 1K model
# Resolution-specific image models + imageConfig.imageSize
# imageConfig inside generationConfig is the CORRECT key (NOT imageGenerationConfig)
GEMINI_IMAGE_TIERS = {
    '1024x1024': {'model': 'gemini-2.5-flash-image',        'imageSize': '1K'},
    '2048x2048': {'model': 'gemini-3.1-flash-image-preview', 'imageSize': '2K'},
    '4096x4096': {'model': 'gemini-3-pro-image-preview',     'imageSize': '4K'},
}
AI_CHAT_COST  = 1    # credits per chat message
AI_IMAGE_COST = 10   # credits per image (legacy flat rate, kept for backward compat)
RESOLUTION_COSTS = {          # credits per image by resolution
    '1024x1024': 10,
    '2048x2048': 25,
    '4096x4096': 50,
}
AI_EXTRACT_COST = 1  # credits per section analysis (9 sections = 9 credits total)

# ── Section Analysis Prompts (7 active sections — no camera, no watermark) ──────
# IMPORTANT: All prompts use plain prose — NO markdown, NO headers, NO bullets.
# Materials section uses the SketchUp base viewport (geometry reference).
# All others use the inspiration image (style/mood reference).
SECTION_ANALYSIS_PROMPTS = {
    'materials': (
        "You are a Senior Architectural Specification Writer analyzing a SketchUp 3D model viewport. "
        "This is the STRUCTURAL BASE image — focus ONLY on the building geometry: wall surfaces, window frames, "
        "door frames, roof planes, columns, overhangs, fascias, and any decorative architectural elements. "
        "Write a comprehensive detailed specification in flowing professional prose (NO markdown, NO headers, NO bullet points, NO asterisks). "
        "Cover in full detail: (1) Primary wall finish — material type, texture, color, finish grade, typical product spec. "
        "(2) Accent panels or feature walls — contrasting material, color, dimensions. "
        "(3) Window and door frames — aluminium or timber, powder-coat color, glazing type (clear/low-e/tinted), sill detail. "
        "(4) Roof plane — roofing material, surface texture, overhang soffit finish, fascia material and color. "
        "(5) Columns and structural elements — material, finish, dimension character. "
        "(6) Built-in lighting positions — any recessed slots, cove lighting positions, wall-washer channels visible. "
        "Be as specific and technical as possible. Use product-grade language an architect or renderer would use."
    ),
    'landscape': (
        "You are a Landscape Architect and Urban Planner analyzing the surrounding environment in this architectural inspiration image. "
        "Write a comprehensive analysis in flowing professional prose (NO markdown, NO headers, NO bullet points, NO asterisks). "
        "Cover in full detail: (1) Urban context and streetscape — road type, lane count, sidewalk width and material, curbing, street furniture, "
        "utilities, fencing, boundary walls visible in the scene. "
        "(2) Surrounding built environment — neighboring building types, heights, density, architectural style character, setbacks, colors. "
        "(3) Vegetation style and softscape zoning — overall landscape design character, tree canopy coverage, planting zones, screening hedges, lawn areas. "
        "(4) Terrain and natural features — ground slope, elevation changes, soil character, climate indicators from vegetation. "
        "(5) Outdoor lighting strategy — street lights, garden uplighting, perimeter lighting, time-of-day ambiance. "
        "Write enough detail for a visualization artist to fully recreate this environment from scratch."
    ),
    'sky': (
        "You are a Lighting Director and VFX Supervisor analyzing the sky and atmospheric lighting in this architectural inspiration image. "
        "Write a comprehensive technical description in flowing professional prose (NO markdown, NO headers, NO bullet points, NO asterisks). "
        "Cover in full detail: (1) Sky gradient — describe the exact color transition from zenith to horizon using specific color names "
        "(e.g. deep cerulean blue at zenith, transitioning through powder blue mid-sky, to soft pale peach at horizon). "
        "(2) Sun position — estimated time of day, compass direction of sun relative to building facade, solar elevation angle, "
        "hard or soft shadow edges, shadow direction on ground and walls. "
        "(3) Color temperature — estimate in Kelvin (e.g. 3200K warm golden hour, 6500K cool overcast), "
        "describe specular highlights on building surfaces, ambient sky fill quality on shadowed faces. "
        "(4) Cloud character — cloud type, coverage percentage, cloud light/shadow transitions. "
        "(5) Atmospheric haze or depth — describe any aerial perspective, fog layers, dust, humidity effects. "
        "Be precise enough that a Gemini image model can recreate this exact atmospheric condition."
    ),
    'plants': (
        "You are a Botanist and Landscape Designer analyzing the planting design in this architectural inspiration image. "
        "Write a comprehensive identification and design analysis in flowing professional prose (NO markdown, NO headers, NO bullet points, NO asterisks). "
        "For each distinct plant group or species visible: state the common name and likely scientific name, "
        "describe the physical appearance (leaf shape, size, color, texture, growth habit, height range), "
        "and describe its exact position and role in the composition (foreground accent, background screening, "
        "entry statement, ground cover, container planting, vertical element). "
        "Then describe the overall planting design philosophy — tropical/subtropical character, density strategy, "
        "layering from ground cover to mid-shrub to canopy, seasonal behavior, maintenance character. "
        "Cover every visible plant type. Be thorough — a visualization artist should be able to source and place "
        "every plant species from this description alone."
    ),
    'softscape': (
        "You are a Landscape Architect specializing in high-end residential and commercial garden design. "
        "Analyze the garden layout, softscape composition, and outdoor spatial design in this architectural inspiration image. "
        "Write a comprehensive analysis in flowing professional prose (NO markdown, NO headers, NO bullet points, NO asterisks). "
        "Cover in full detail: (1) Spatial layout and zoning — describe how outdoor spaces are divided (entry forecourt, "
        "feature garden beds, lawn zone, patio/terrace), transitions between zones, screening elements, focal points. "
        "(2) Planter and raised bed design — materials (concrete, corten steel, rendered masonry), dimensions, capping details, "
        "soil depth, drainage grilles visible. "
        "(3) Ground cover and texture transitions — how softscape blends into hardscape, mulch type and color, "
        "lawn grass species character, organic ground layers. "
        "(4) Garden lighting design — uplighting on feature plants, step lighting, path edge lighting, ambient glow strategy. "
        "(5) Water features or sculptural elements if visible. "
        "Write with enough specificity that a landscape contractor could cost and build this garden from the description."
    ),
    'pavement': (
        "You are a Materials Specialist and Civil Designer analyzing all ground surfaces in this architectural inspiration image. "
        "Write a comprehensive surface specification in flowing professional prose (NO markdown, NO headers, NO bullet points, NO asterisks). "
        "For every distinct surface area visible describe: the exact material and product specification "
        "(e.g. 600x600mm honed concrete pavers, charcoal grey, 40mm thick, butt-jointed with 3mm grouted joint), "
        "the surface color, texture, and finish (matte/polished/brushed/acid-washed), "
        "the laying pattern (stack bond, running bond, herringbone, random), "
        "and the precise location and zone in the scene. "
        "Cover ALL surfaces: main driveway or forecourt, entry path, pedestrian sidewalk, road edge, "
        "planter base surrounds, step treads and risers, kerbing, any gravel or decorative aggregate areas, "
        "recessed drainage channels, boundary wall base. "
        "Note any lighting integration into paving (recessed in-ground lights, step nosing lights). "
        "Be detailed enough for a quantity surveyor to price the finishes."
    ),
    'mood': (
        "You are a Senior Colorist and Cinematographer specializing in architectural visualization and high-end CGI. "
        "Analyze the complete mood, color grading, and visual atmosphere of this architectural inspiration image. "
        "Write a comprehensive color grade specification in flowing professional prose (NO markdown, NO headers, NO bullet points, NO asterisks). "
        "Cover in full detail: (1) Overall emotional mood — describe the feeling and atmosphere in one or two evocative sentences. "
        "(2) Tonal balance — describe the luminosity distribution: how highlights, midtones, and shadows are treated, "
        "contrast ratio character (low-key/high-key/balanced), vignetting, depth-of-field blur if present. "
        "(3) Color palette — give the palette a cinematic name, then describe 5-6 specific named colors visible in the scene "
        "with their role (sky fill color, shadow tone color, highlight warmth, vegetation green character, "
        "surface material tone, ambient reflected light color). "
        "(4) Lighting quality — describe the quality of light (golden warm, cool blue, neutral daylight), "
        "color temperature in Kelvin, how it affects each material type in the scene. "
        "(5) Post-processing character — describe any bloom, chromatic aberration, film grain, lens flare, "
        "color shift in shadows vs highlights (split toning). "
        "Write with enough technical precision that a Gemini image model can perfectly recreate this color grade."
    ),
}
# ── Supabase (optional — for RAG material context) ──────────────────────────────────────────────
SUPABASE_URL      = os.environ.get('SUPABASE_URL', '')
SUPABASE_ANON_KEY = os.environ.get('SUPABASE_ANON_KEY', '')
SUPABASE_SERVICE_KEY  = os.environ.get('SUPABASE_SERVICE_ROLE_KEY', '')

def _supa(table, method='GET', params=None, body=None, use_service_key=False, extra_headers=None):
    """Thin helper for Supabase REST API calls from server-side Python."""
    if not SUPABASE_URL:
        raise RuntimeError("SUPABASE_URL not configured")
    import urllib.request as _req
    import urllib.error   as _uerr
    key   = SUPABASE_SERVICE_KEY if use_service_key else SUPABASE_ANON_KEY
    url   = SUPABASE_URL.rstrip('/') + '/rest/v1/' + table
    if params:
        from urllib.parse import urlencode
        url += '?' + urlencode(params)
    hdrs  = {
        'apikey':        SUPABASE_ANON_KEY,
        'Authorization': 'Bearer ' + key,
        'Content-Type':  'application/json',
        'Prefer':        'return=representation',
    }
    if extra_headers:
        hdrs.update(extra_headers)
    req = _req.Request(url, headers=hdrs, method=method)
    if body is not None:
        import json as _json2
        req.data = _json2.dumps(body).encode()
    with _req.urlopen(req, timeout=12) as r:
        raw = r.read()
        return json.loads(raw) if raw else []

def _ensure_profile(gmail):
    """Ensure a profile row exists for the given gmail. No-op if already present."""
    if not gmail or not SUPABASE_URL:
        return
    try:
        _supa('profiles', method='POST',
              body={'gmail': gmail, 'display_name': gmail.split('@')[0]},
              extra_headers={'Prefer': 'return=minimal,resolution=ignore-duplicates'},
              use_service_key=True)
    except Exception:
        pass  # profile already exists or Supabase unreachable — safe to ignore



try:
    import urllib.request as _urllib_req
    import urllib.error   as _urllib_err
    _AI_HTTP_AVAILABLE = True
except Exception:
    _AI_HTTP_AVAILABLE = False

# ââ Input validation regexes âââââââââââââââââââââââââââââââââââââââââââââââââââ
_GMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$')
_KEY_RE   = re.compile(r'^ABIM-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$')

# ââ In-memory rate limiter âââââââââââââââââââââââââââââââââââââââââââââââââââââ
_rl_lock   = threading.Lock()
_rl_store  = {}   # ip -> {attempts, window_start, locked_until}
_RL_MAX    = 5    # max attempts per window
_RL_WINDOW = 60   # seconds per window
_RL_LOCK   = 300  # lockout duration (5 min); escalates to 1 hour after 15 total

def _rl_check(ip: str) -> bool:
    """Return True if request is allowed; False if rate-limited."""
    now = time.time()
    with _rl_lock:
        r = _rl_store.get(ip, {"attempts": 0, "window_start": now, "locked_until": 0, "total": 0})
        if now < r.get("locked_until", 0):
            return False
        if now - r.get("window_start", now) > _RL_WINDOW:
            r = {"attempts": 0, "window_start": now, "locked_until": 0, "total": r.get("total", 0)}
        r["attempts"] = r.get("attempts", 0) + 1
        r["total"]    = r.get("total", 0) + 1
        if r["attempts"] >= _RL_MAX:
            lockout = 3600 if r["total"] >= 15 else _RL_LOCK
            r["locked_until"] = now + lockout
            _rl_store[ip] = r
            return False
        _rl_store[ip] = r
        return True

def _rl_reset(ip: str):
    with _rl_lock:
        _rl_store.pop(ip, None)

# ââ Challenge / nonce store ââââââââââââââââââââââââââââââââââââââââââââââââââââ
_nc_lock  = threading.Lock()
_nc_store = {}   # nonce -> expiry_timestamp
_NC_TTL   = 45   # seconds a nonce is valid

def _nc_create() -> str:
    nonce = base64.urlsafe_b64encode(os.urandom(18)).decode().rstrip("=")
    now   = time.time()
    with _nc_lock:
        _nc_store[nonce] = now + _NC_TTL
        # Purge stale nonces
        stale = [k for k, v in list(_nc_store.items()) if v < now]
        for k in stale:
            del _nc_store[k]
    return nonce

def _nc_consume(nonce: str) -> bool:
    """Returns True and removes nonce if valid; False otherwise (one-use)."""
    now = time.time()
    with _nc_lock:
        exp = _nc_store.pop(nonce, None)
    return exp is not None and exp > now

# ââ boto3 for Cloudflare R2 signed URLs ââââââââââââââââââââââââââââââââââââââââ
try:
    import boto3
    from botocore.config import Config as BotoConfig
    HAS_BOTO3 = True
except ImportError:
    HAS_BOTO3 = False
    print("[WARN] boto3 not installed â /api/download-model will be disabled. Run: pip install boto3")

# ââ Config âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
PORT       = int(os.environ.get("PORT", 8080))
ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "abimcon_admin_2026")
SECRET_KEY = os.environ.get("SECRET_KEY",     "abimcon_secret_key_v3_change_me")
DB_PATH    = os.environ.get("DB_PATH",         "/data/abimcon_v3.db")
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)  # ensure /data dir exists

# Cloudflare R2 credentials (set these in Railway environment variables)
R2_ACCOUNT_ID        = os.environ.get("R2_ACCOUNT_ID",        "")
R2_ACCESS_KEY_ID     = os.environ.get("R2_ACCESS_KEY_ID",     "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME       = os.environ.get("R2_BUCKET_NAME",       "abimcon-models")   # bucket for .skp files
R2_ASSETS_BUCKET     = os.environ.get("R2_ASSETS_BUCKET",     "abimcon-assets")   # bucket for thumbnails/images
R2_SIGNED_URL_TTL    = int(os.environ.get("R2_SIGNED_URL_TTL", 300))  # seconds (default 5 min)

# Plan defaults
PLAN_DAILY_LIMITS = {"free": 5, "pro": 100, "trial": 50}

# ââ Database âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    # ââ Licenses ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS licenses (
            id                  TEXT PRIMARY KEY,
            gmail               TEXT UNIQUE NOT NULL,
            license_key         TEXT UNIQUE NOT NULL,
            name                TEXT NOT NULL,
            role                TEXT NOT NULL DEFAULT 'editor',
            plan_type           TEXT NOT NULL DEFAULT 'free',
            active              INTEGER NOT NULL DEFAULT 1,
            max_devices         INTEGER NOT NULL DEFAULT 2,
            daily_download_limit INTEGER,
            expiry_date         TEXT,
            last_login          TEXT,
            created_at          TEXT NOT NULL
        )
    """)

    # ââ Registered HWIDs ââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS registered_hwids (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            license_id TEXT NOT NULL,
            hwid       TEXT NOT NULL,
            first_seen TEXT NOT NULL,
            last_seen  TEXT NOT NULL,
            ip_addr    TEXT,
            UNIQUE(license_id, hwid),
            FOREIGN KEY(license_id) REFERENCES licenses(id) ON DELETE CASCADE
        )
    """)

    # ââ Models ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS models (
            model_id    TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            r2_path     TEXT NOT NULL,
            is_premium  INTEGER NOT NULL DEFAULT 0,
            file_size   INTEGER DEFAULT 0,
            description TEXT DEFAULT '',
            created_at  TEXT NOT NULL
        )
    """)

    # ââ Download Logs (for daily limit tracking) âââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS download_logs (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            license_id    TEXT NOT NULL,
            model_id      TEXT NOT NULL,
            downloaded_at TEXT NOT NULL,
            ip_addr       TEXT,
            hwid          TEXT,
            FOREIGN KEY(license_id) REFERENCES licenses(id) ON DELETE CASCADE
        )
    """)

    # ââ Activity log ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS activity_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            gmail      TEXT,
            action     TEXT,
            success    INTEGER,
            ip_addr    TEXT,
            hwid       TEXT,
            created_at TEXT
        )
    """)

    # ââ AI Wallets ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_wallets (
            id              TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
            gmail           TEXT UNIQUE NOT NULL,
            credits         INTEGER NOT NULL DEFAULT 10,
            total_purchased INTEGER NOT NULL DEFAULT 0,
            total_used      INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
    """)

    # ââ AI Credit Transactions ââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_transactions (
            id            TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
            gmail         TEXT NOT NULL,
            type          TEXT NOT NULL,
            amount        INTEGER NOT NULL,
            balance_after INTEGER NOT NULL,
            description   TEXT,
            created_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
    """)

    # ââ AI Top-up Requests ââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_topup_requests (
            id                TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
            gmail             TEXT NOT NULL,
            credits_requested INTEGER NOT NULL,
            receipt_base64    TEXT,
            note              TEXT,
            status            TEXT DEFAULT 'pending',
            admin_note        TEXT,
            created_at        TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
    """)

    # ââ Plan Features (feature flags per plan/role) âââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS plan_features (
            id          TEXT PRIMARY KEY,
            plan_type   TEXT NOT NULL,
            feature_key TEXT NOT NULL,
            label       TEXT NOT NULL DEFAULT '',
            enabled     INTEGER NOT NULL DEFAULT 1,
            sort_order  INTEGER NOT NULL DEFAULT 99,
            UNIQUE(plan_type, feature_key)
        )
    """)

    # Seed default plan features if empty
    c.execute("SELECT COUNT(*) FROM plan_features")
    if c.fetchone()[0] == 0:
        feature_defs = [
            # (plan_type, feature_key, label, enabled, sort_order)
            ('pro',   'boq_input',        'BOQ Input',          1, 1),
            ('pro',   'excel_sync',        'Excel Sync',         1, 2),
            ('pro',   'asset_browser',     'Asset Browser',      1, 3),
            ('pro',   'project_schedule',  'Project Schedule',   1, 4),
            ('pro',   'project_todo',      'Project To-Do List', 1, 5),
            ('free',  'boq_input',         'BOQ Input',          1, 1),
            ('free',  'excel_sync',         'Excel Sync',         1, 2),
            ('free',  'asset_browser',      'Asset Browser',      1, 3),
            ('free',  'project_schedule',   'Project Schedule',   0, 4),
            ('free',  'project_todo',       'Project To-Do List', 0, 5),
            ('trial', 'boq_input',          'BOQ Input',          1, 1),
            ('trial', 'excel_sync',          'Excel Sync',         1, 2),
            ('trial', 'asset_browser',       'Asset Browser',      1, 3),
            ('trial', 'project_schedule',    'Project Schedule',   1, 4),
            ('trial', 'project_todo',        'Project To-Do List', 1, 5),
        ]
        for plan, fkey, label, enabled, sort in feature_defs:
            c.execute(
                "INSERT INTO plan_features (id,plan_type,feature_key,label,enabled,sort_order) "
                "VALUES (?,?,?,?,?,?)",
                (str(uuid.uuid4()), plan, fkey, label, enabled, sort)
            )

    # ââ Idempotent migrations for existing DBs ââââââââââââââââââââââââââââââââ
    migrations = [
        "ALTER TABLE licenses ADD COLUMN max_devices         INTEGER NOT NULL DEFAULT 2",
        "ALTER TABLE licenses ADD COLUMN plan_type           TEXT    NOT NULL DEFAULT 'free'",
        "ALTER TABLE licenses ADD COLUMN expiry_date         TEXT",
        "ALTER TABLE licenses ADD COLUMN daily_download_limit INTEGER",
        "ALTER TABLE activity_log ADD COLUMN hwid TEXT",
        # v3.1 â new model columns for AssetBrowser UI
        "ALTER TABLE models ADD COLUMN category     TEXT    NOT NULL DEFAULT 'General'",
        "ALTER TABLE models ADD COLUMN tags         TEXT    NOT NULL DEFAULT ''",
        "ALTER TABLE models ADD COLUMN thumbnail    TEXT    NOT NULL DEFAULT ''",
        "ALTER TABLE models ADD COLUMN plan_required TEXT   NOT NULL DEFAULT 'free'",
        "ALTER TABLE models ADD COLUMN file_size_mb REAL   NOT NULL DEFAULT 0",
        "ALTER TABLE models ADD COLUMN active       INTEGER NOT NULL DEFAULT 1",
    ]
    for sql in migrations:
        try:
            c.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists

    # ââ Idempotent plan_features seed/patch (runs every startup) âââââââââââââ
    # Ensures ALL feature keys exist for all plans regardless of DB age.
    # Each (plan_type, feature_key) pair is upserted with the correct defaults.
    required_features = [
        # (plan_type, feature_key, label, enabled, sort_order)
        ('pro',   'boq_input',         'BOQ Input',           1, 1),
        ('pro',   'excel_sync',         'Excel Sync',          1, 2),
        ('pro',   'asset_browser',      'Asset Browser',       1, 3),
        ('pro',   'financial_report',   'BOQ Report',          1, 4),
        ('pro',   'project_schedule',   'Project Schedule',    1, 5),
        ('pro',   'project_todo',       'Project To-Do List',  1, 6),
        ('pro',   'material_schedule',  'Material Schedule',   1, 7),
        ('free',  'boq_input',          'BOQ Input',           1, 1),
        ('free',  'excel_sync',          'Excel Sync',          0, 2),
        ('free',  'asset_browser',       'Asset Browser',       0, 3),
        ('free',  'financial_report',    'BOQ Report',          0, 4),
        ('free',  'project_schedule',    'Project Schedule',    0, 5),
        ('free',  'project_todo',        'Project To-Do List',  0, 6),
        ('free',  'material_schedule',   'Material Schedule',   0, 7),
        ('trial', 'boq_input',           'BOQ Input',           1, 1),
        ('trial', 'excel_sync',           'Excel Sync',          1, 2),
        ('trial', 'asset_browser',        'Asset Browser',       1, 3),
        ('trial', 'financial_report',     'BOQ Report',          1, 4),
        ('trial', 'project_schedule',     'Project Schedule',    1, 5),
        ('trial', 'project_todo',         'Project To-Do List',  1, 6),
        ('trial', 'material_schedule',    'Material Schedule',   1, 7),
    ]
    for plan, fkey, label, enabled, sort in required_features:
        exists = c.execute(
            "SELECT COUNT(*) FROM plan_features WHERE plan_type=? AND feature_key=?",
            (plan, fkey)
        ).fetchone()[0]
        if exists == 0:
            c.execute(
                "INSERT INTO plan_features (id,plan_type,feature_key,label,enabled,sort_order) "
                "VALUES (?,?,?,?,?,?)",
                (str(uuid.uuid4()), plan, fkey, label, enabled, sort)
            )

    # ââ Seed licenses âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("SELECT COUNT(*) FROM licenses")
    if c.fetchone()[0] == 0:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        expiry_pro  = (datetime.now(timezone.utc) + timedelta(days=365)).strftime("%Y-%m-%d")
        seeds = [
            ("abimcon.user01@gmail.com","ABIM-4K9L-MN2P-X001","Abimcon User 01","admin",  "pro",  expiry_pro, 2, 100),
            ("abimcon.user02@gmail.com","ABIM-7R3T-QW5X-X002","Abimcon User 02","editor", "pro",  expiry_pro, 2, 100),
            ("abimcon.user03@gmail.com","ABIM-1ZBP-HV8C-X003","Abimcon User 03","editor", "free", None,       2, 5),
            ("abimcon.user04@gmail.com","ABIM-9NKW-DF4J-X004","Abimcon User 04","viewer", "free", None,       2, 5),
            ("abimcon.user05@gmail.com","ABIM-5MQE-TU6G-X005","Abimcon User 05","editor", "pro",  expiry_pro, 2, 100),
            ("abimcon.user06@gmail.com","ABIM-2XVC-SB3H-X006","Abimcon User 06","viewer", "free", None,       2, 5),
            ("abimcon.user07@gmail.com","ABIM-8FGD-PW7Y-X007","Abimcon User 07","editor", "pro",  expiry_pro, 2, 100),
            ("abimcon.user08@gmail.com","ABIM-3LRJ-CZ9M-X008","Abimcon User 08","viewer", "free", None,       2, 5),
            ("abimcon.user09@gmail.com","ABIM-6THN-AX1K-X009","Abimcon User 09","editor", "pro",  expiry_pro, 2, 100),
            ("abimcon.user10@gmail.com","ABIM-0YAF-LK2R-X010","Abimcon User 10","admin",  "pro",  expiry_pro, 2, 100),
        ]
        for gmail, key, name, role, plan, expiry, max_dev, ddl in seeds:
            c.execute(
                "INSERT INTO licenses (id,gmail,license_key,name,role,plan_type,active,max_devices,daily_download_limit,expiry_date,created_at) "
                "VALUES (?,?,?,?,?,?,1,?,?,?,?)",
                (str(uuid.uuid4()), gmail, key, name, role, plan, max_dev, ddl, expiry, now)
            )

    # ââ Seed models âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("SELECT COUNT(*) FROM models")
    if c.fetchone()[0] == 0:
        now_m = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        model_seeds = [
            ("Tle Jirayut",            "models/Tle_Jirayut_.skp",           "free", "Architecture", "thai,house,residential",   "Thai residential architecture model by Tle Jirayut", 66.6),
            ("Ray Thai Architecture",  "models/ray_Thai_architecture.skp",  "free", "Architecture", "thai,architecture,house",   "Thai architecture reference model",                 17.6),
            ("Sutichai Pluemthanom",   "models/_Sutichai_Pluemthanom.skp", "free", "Architecture", "thai,house,residential",   "Residential model by Sutichai Pluemthanom",          27.2),
            ("\u0e04\u0e21\u0e01\u0e23\u0e34\u0e0a \u0e1e\u0e31\u0e19\u0e42\u0e01\u0e0e\u0e34",  "models/\u0e04\u0e21\u0e01\u0e23\u0e34\u0e0a_\u0e1e\u0e31\u0e19\u0e42\u0e01\u0e0e\u0e34.skp", "free", "Architecture", "thai,house",               "Thai house model",                                  32.5),
            ("\u0e41\u0e1a\u0e1a\u0e1a\u0e49\u0e32\u0e19 GHB 102", "models/\u0e41\u0e1a\u0e1a\u0e1a\u0e49\u0e32\u0e19_GHB_102.skp", "free", "Architecture", "thai,house,GHB,floor plan", "GHB floor plan model 102",                          13.9),
        ]
        for m_name, r2_path, plan_req, category, tags, desc, fsz in model_seeds:
            c.execute(
                "INSERT INTO models "
                "(model_id,name,r2_path,is_premium,file_size,description,created_at,"
                "category,tags,plan_required,file_size_mb,active) "
                "VALUES (?,?,?,0,0,?,?,?,?,?,?,1)",
                (str(uuid.uuid4()), m_name, r2_path, desc, now_m, category, tags, plan_req, fsz)
            )

    conn.commit()
    conn.close()
    print(f"[DB] Initialised at {DB_PATH}")

# ââ Cloudflare R2 signed URL ââââââââââââââââââââââââââââââââââââââââââââââââââââ
def generate_r2_signed_url(r2_path: str, ttl: int = R2_SIGNED_URL_TTL,
                            bucket: str = None) -> str | None:
    if not HAS_BOTO3:
        return None
    if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
        print("[R2] Missing credentials â set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
        return None
    bucket = bucket or R2_BUCKET_NAME
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url          = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id     = R2_ACCESS_KEY_ID,
            aws_secret_access_key = R2_SECRET_ACCESS_KEY,
            config                = BotoConfig(signature_version="s3v4"),
            region_name           = "auto",
        )
        url = s3.generate_presigned_url(
            "get_object",
            Params    = {"Bucket": bucket, "Key": r2_path},
            ExpiresIn = ttl,
        )
        return url
    except Exception as e:
        print(f"[R2] Signed URL error ({bucket}/{r2_path}): {e}")
        return None

def _sign_thumbnail(thumb: str) -> str:
    """Return a signed URL for a thumbnail R2 path, or the value as-is if it's already a URL."""
    if not thumb:
        return ""
    if thumb.startswith("http://") or thumb.startswith("https://"):
        return thumb   # already a full URL â return unchanged
    # treat as R2 key in the assets bucket (thumbnails/xxx.jpg)
    return generate_r2_signed_url(thumb, ttl=3600, bucket=R2_ASSETS_BUCKET) or ""

# ââ JWT helpers ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def _b64(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()

def _b64d(s):
    pad = 4 - len(s) % 4
    return base64.urlsafe_b64decode(s + "=" * (pad % 4))

def make_token(payload, expires_in=86400):
    header  = _b64(json.dumps({"alg":"HS256","typ":"JWT"}).encode())
    payload = dict(payload, exp=int(time.time())+expires_in, iat=int(time.time()))
    body    = _b64(json.dumps(payload).encode())
    sig     = _b64(hmac.new(SECRET_KEY.encode(), f"{header}.{body}".encode(), hashlib.sha256).digest())
    return f"{header}.{body}.{sig}"

def verify_token(token):
    try:
        h, b, s = token.split(".")
        expected = _b64(hmac.new(SECRET_KEY.encode(), f"{h}.{b}".encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(expected, s):
            return None
        payload = json.loads(_b64d(b))
        if payload.get("exp", 0) < time.time():
            return None
        return payload
    except Exception:
        return None

# ââ Admin Panel HTML ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
_admin_path = os.path.join(os.path.dirname(__file__), "admin.html")
ADMIN_HTML  = open(_admin_path).read() if os.path.exists(_admin_path) \
              else "<h1>Admin panel not found</h1>"

# ââ HTTP Handler ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[HTTP] {self.address_string()} - {fmt % args}")

    def _get_client_ip(self):
        xff = self.headers.get("X-Forwarded-For", "").strip()
        if xff:
            return xff.split(",")[0].strip()
        return self.client_address[0]

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type",   "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization,Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, code, html):
        body = html.encode()
        self.send_response(code)
        self.send_header("Content-Type",   "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def _require_admin(self):
        auth  = self.headers.get("Authorization", "")
        token = auth[7:].strip() if auth.startswith("Bearer ") else ""
        payload = verify_token(token)
        if not payload or payload.get("role") != "admin":
            self._json(401, {"error": "Unauthorized"})
            return None
        return payload

    def _require_auth(self):
        """Require any valid session token (admin or user)."""
        auth  = self.headers.get("Authorization", "")
        token = auth[7:].strip() if auth.startswith("Bearer ") else ""
        payload = verify_token(token)
        if not payload:
            self._json(401, {"error": "Unauthorized"})
            return None
        return payload

    def _log_activity(self, gmail, action, success, hwid=""):
        conn = get_db()
        now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.execute(
            "INSERT INTO activity_log (gmail,action,success,ip_addr,hwid,created_at) VALUES (?,?,?,?,?,?)",
            (gmail, action, 1 if success else 0, self._get_client_ip(), hwid or None, now)
        )
        conn.commit(); conn.close()

    # ââ CORS preflight ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization,Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
        self.end_headers()

    # ââ GET âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/") or "/"

        if   path == "/":                         self._html(200, ADMIN_HTML)
        elif path == "/health":                   self._json(200, {"status": "ok"})
        elif path == "/api/challenge":
            nonce = _nc_create()
            self._json(200, {"nonce": nonce, "ttl": _NC_TTL})
        elif path == "/api/admin/verify":
            p = self._require_admin()
            if p: self._json(200, {"ok": True})
        elif path == "/api/admin/licenses":       self._get_licenses()
        elif path == "/api/admin/logs":           self._get_logs()
        elif path == "/api/admin/models":         self._get_models()
        elif path == "/api/admin/plan-features":  self._get_plan_features()
        elif path == "/api/plan-features":        self._get_plan_features_public()
        elif path == "/api/models":               self._get_user_models()
        elif path == "/api/assets":               self._get_assets()   # â AssetBrowser JS endpoint
        elif path == "/api/admin/ai/wallets":     self._admin_ai_wallets()
        elif path == "/api/admin/ai/topups":      self._admin_ai_topups()
        elif path == "/api/social/config":        self._social_config()
        elif path == "/api/social/feed":          self._social_feed_get()
        elif path == "/api/social/notifications": self._social_notifications_get()
        else:
            m_hw  = re.match(r"^/api/admin/licenses/([^/]+)/hwids$",      path)
            m_dl  = re.match(r"^/api/admin/licenses/([^/]+)/downloads$",   path)
            m_mod = re.match(r"^/api/admin/models/([^/]+)$",               path)
            if   m_hw:  self._get_hwids(m_hw.group(1))
            elif m_dl:  self._get_download_stats(m_dl.group(1))
            else:       self._json(404, {"error": "Not found"})

    # ââ POST ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")

        if   path == "/api/admin/login":           self._admin_login()
        elif path == "/api/validate":               self._validate_license()
        elif path == "/api/license-info":           self._license_info()
        elif path == "/api/deactivate-device":      self._deactivate_device()
        elif path == "/api/ai/chat":                self._ai_chat()
        elif path == "/api/ai/image":               self._ai_image()
        elif path == "/api/ai/extract-params":      self._ai_extract_params()
        elif path == "/api/ai/credits":             self._ai_get_credits()
        elif path == "/api/ai/transactions":        self._ai_transactions()
        elif path == "/api/ai/topup":               self._ai_topup_request()
        elif path == "/api/social/feed/post":           self._social_post_create()
        elif path == "/api/social/feed/like":           self._social_post_like()
        elif path == "/api/social/friends":             self._social_friends_list()
        elif path == "/api/social/friends/add":         self._social_friend_action()
        elif path == "/api/social/chat/rooms":          self._social_chat_rooms()
        elif path == "/api/social/chat/send":           self._social_chat_send()
        elif path == "/api/social/org/members":         self._social_org_members()
        elif path == "/api/social/notifications/read":  self._social_notif_read()
        elif path == "/api/social/media/save":          self._social_media_save()
        elif path == "/api/social/media/list":          self._social_media_list()
        elif path == "/api/admin/ai/credits/add":     self._admin_add_credits()
        elif path == "/api/admin/ai/topups/approve": self._admin_approve_topup()
        elif path == "/api/download-model":         self._download_model()
        elif path == "/api/download":               self._download()        # â AssetBrowser JS endpoint
        elif path == "/api/admin/licenses":         self._add_license()
        elif path == "/api/admin/models":           self._add_model()
        elif path == "/api/admin/plan-features":    self._add_plan_or_feature()
        else:
            m_ext = re.match(r"^/api/admin/licenses/([^/]+)/extend$", path)
            if m_ext: self._extend_license(m_ext.group(1))
            else:     self._json(404, {"error": "Not found"})

    # ââ PUT âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_PUT(self):
        path = urlparse(self.path).path.rstrip("/")
        if path == "/api/admin/plan-features":
            self._update_plan_feature()
        else:
            m = re.match(r"^/api/admin/licenses/([^/]+)$", path)
            if m: self._update_license(m.group(1))
            else:
                m2 = re.match(r"^/api/admin/models/([^/]+)$", path)
                if m2: self._update_model(m2.group(1))
                else: self._json(404, {"error": "Not found"})

    # ââ DELETE ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_DELETE(self):
        path = urlparse(self.path).path.rstrip("/")
        m_hw  = re.match(r"^/api/admin/licenses/([^/]+)/hwids$",      path)
        m_dl  = re.match(r"^/api/admin/licenses/([^/]+)/downloads$",   path)
        m_lic = re.match(r"^/api/admin/licenses/([^/]+)$",             path)
        m_mod = re.match(r"^/api/admin/models/([^/]+)$",               path)

        if   m_hw:  self._reset_hwids(m_hw.group(1))
        elif m_dl:  self._reset_downloads(m_dl.group(1))
        elif m_lic: self._delete_license(m_lic.group(1))
        elif m_mod: self._delete_model(m_mod.group(1))
        else:       self._json(404, {"error": "Not found"})

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ Auth Handlers âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _admin_login(self):
        body = self._body()
        if body.get("password", "") != ADMIN_PASS:
            self._log_activity("admin", "admin_login", False)
            self._json(401, {"error": "Invalid password"}); return
        token = make_token({"sub": "admin", "role": "admin"})
        self._log_activity("admin", "admin_login", True)
        self._json(200, {"token": token})

    def _validate_license(self):
        client_ip = self._get_client_ip()

        # ââ Rate limiting ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        if not _rl_check(client_ip):
            self._log_activity("?", "rate_limited", False)
            self._json(429, {
                "ok": False,
                "error": "Too many failed attempts. Please wait 5 minutes before trying again.",
                "code": "RATE_LIMITED"
            }); return

        body  = self._body()
        gmail = body.get("gmail", "").strip().lower()
        key   = body.get("license_key", "").strip()
        hwid  = body.get("hwid", "").strip()
        nonce = body.get("nonce", "").strip()

        # ââ Input validation âââââââââââââââââââââââââââââââââââââââââââââââââââââ
        if not gmail or not key:
            self._json(400, {"ok": False, "error": "Missing credentials"}); return
        if not _GMAIL_RE.match(gmail):
            self._json(400, {"ok": False, "error": "Invalid email format"}); return
        if not _KEY_RE.match(key):
            self._json(400, {"ok": False, "error": "Invalid license key format"}); return

        # ââ Challenge-response nonce verification ââââââââââââââââââââââââââââââââ
        # If a nonce was provided, it must be valid (prevents replay attacks).
        # If no nonce provided, allow through for backward compatibility with
        # older clients â once all clients are updated, make this mandatory.
        if nonce and not _nc_consume(nonce):
            self._json(400, {
                "ok": False,
                "error": "Invalid or expired security challenge. Please try again.",
                "code": "NONCE_INVALID"
            }); return

        conn = get_db()
        row  = conn.execute(
            "SELECT * FROM licenses WHERE gmail=? AND license_key=? AND active=1",
            (gmail, key)
        ).fetchone()

        if not row:
            conn.close()
            self._log_activity(gmail, "sketchup_login", False, hwid)
            self._json(401, {"ok": False, "error": "Invalid Gmail or License Key"}); return

        # ââ Check expiry ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        if row["expiry_date"]:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if row["expiry_date"] < today:
                conn.close()
                self._log_activity(gmail, "license_expired", False, hwid)
                self._json(403, {"ok": False, "error": "License expired. Contact your administrator.", "code": "EXPIRED"}); return

        # ââ HWID enforcement ââââââââââââââââââââââââââââââââââââââââââââââââââââ
        max_dev   = row["max_devices"] if row["max_devices"] else 2
        client_ip = self._get_client_ip()
        now_ts    = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        if hwid:
            hw_rows = conn.execute(
                "SELECT hwid FROM registered_hwids WHERE license_id=?", (row["id"],)
            ).fetchall()
            known = [r["hwid"] for r in hw_rows]

            if hwid in known:
                conn.execute(
                    "UPDATE registered_hwids SET last_seen=?, ip_addr=? WHERE license_id=? AND hwid=?",
                    (now_ts, client_ip, row["id"], hwid)
                )
            else:
                if len(known) >= max_dev:
                    conn.close()
                    self._log_activity(gmail, "hwid_blocked", False, hwid)
                    self._json(403, {
                        "ok":    False,
                        "error": f"Device limit reached ({max_dev} devices). Contact your admin to reset.",
                        "code":  "DEVICE_LIMIT"
                    }); return
                conn.execute(
                    "INSERT INTO registered_hwids (license_id,hwid,first_seen,last_seen,ip_addr) VALUES (?,?,?,?,?)",
                    (row["id"], hwid, now_ts, now_ts, client_ip)
                )

        # Get feature flags for this plan
        feat_rows = conn.execute(
            "SELECT feature_key, enabled FROM plan_features WHERE plan_type=?",
            (row["plan_type"],)
        ).fetchall()
        features = {r["feature_key"]: bool(r["enabled"]) for r in feat_rows}

        conn.execute("UPDATE licenses SET last_login=? WHERE id=?", (now_ts, row["id"]))
        conn.commit(); conn.close()
        self._log_activity(gmail, "sketchup_login", True, hwid)
        _rl_reset(client_ip)  # Clear rate limit counter on successful login

        session_token = make_token(
            {"sub": gmail, "role": row["role"], "lid": row["id"], "plan": row["plan_type"]},
            expires_in=28800
        )

        # ââ Compute days remaining until expiry âââââââââââââââââââââââââââââââââ
        expiry_str     = row["expiry_date"] if row["expiry_date"] else None
        days_remaining = None
        if expiry_str:
            try:
                from datetime import date as _date_cls
                exp            = _date_cls.fromisoformat(expiry_str)
                days_remaining = (exp - _date_cls.today()).days
            except Exception:
                pass

        self._json(200, {
            "ok":            True,
            "name":          row["name"],
            "role":          row["role"],
            "plan":          row["plan_type"],
            "features":      features,
            "token":         session_token,
            "expiry_date":   expiry_str,
            "days_remaining": days_remaining,
            "max_devices":   row["max_devices"] if row["max_devices"] else 2,
        })

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ License Info Endpoint (Settings Hub) âââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _license_info(self):
        """POST /api/license-info  { gmail, license_key, hwid }
        Returns full license info including expiry_date for Settings Hub display.
        Does NOT register HWID or update last_login â read-only lookup."""
        body  = self._body()
        gmail = body.get("gmail", "").strip().lower()
        key   = body.get("license_key", "").strip()
        hwid  = body.get("hwid", "").strip()

        if not gmail or not key:
            self._json(400, {"ok": False, "error": "Missing credentials"}); return

        conn = get_db()
        row  = conn.execute(
            "SELECT * FROM licenses WHERE gmail=? AND license_key=? AND active=1",
            (gmail, key)
        ).fetchone()

        if not row:
            conn.close()
            self._json(401, {"ok": False, "error": "Invalid credentials"}); return

        # Compute days remaining
        expiry_str     = row["expiry_date"] if row["expiry_date"] else None
        days_remaining = None
        if expiry_str:
            try:
                from datetime import date as _date_cls
                exp            = _date_cls.fromisoformat(expiry_str)
                days_remaining = (exp - _date_cls.today()).days
            except Exception:
                pass

        # Count registered devices
        hw_rows      = conn.execute(
            "SELECT hwid, last_seen, ip_addr FROM registered_hwids WHERE license_id=?",
            (row["id"],)
        ).fetchall()
        hwid_list    = [r["hwid"] for r in hw_rows]
        current_hwid = hwid

        conn.close()

        self._json(200, {
            "ok":            True,
            "gmail":         row["gmail"],
            "name":          row["name"],
            "role":          row["role"],
            "plan":          row["plan_type"],
            "license_key":   row["license_key"],
            "expiry_date":   expiry_str,
            "days_remaining": days_remaining,
            "max_devices":   row["max_devices"] if row["max_devices"] else 2,
            "current_hwid":  current_hwid,
            "hwid_list":     hwid_list,
        })

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ Deactivate Device Endpoint ââââââââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _deactivate_device(self):
        """POST /api/deactivate-device
        Headers: Authorization: Bearer <session_token>
        Body:    { "hwid": "..." }
        Removes the HWID from registered_hwids so the device slot is freed."""
        payload = self._require_auth()
        if not payload: return
        body = self._body()
        hwid = body.get("hwid", "").strip()
        if not hwid:
            self._json(400, {"ok": False, "error": "Missing hwid"}); return
        lid = payload.get("lid", "")
        if not lid:
            self._json(401, {"ok": False, "error": "Invalid token"}); return
        conn = get_db()
        result = conn.execute(
            "DELETE FROM registered_hwids WHERE license_id=? AND hwid=?", (lid, hwid)
        )
        conn.commit()
        removed = result.rowcount > 0
        conn.close()
        self._log_activity(payload.get("sub", "?"), "device_deactivated", removed, hwid)
        if removed:
            self._json(200, {"ok": True, "message": "Device deactivated successfully."})
        else:
            self._json(404, {"ok": False, "error": "Device not found for this license."})

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ Secure Download Endpoint ââââââââââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _download_model(self):
        """
        POST /api/download-model
        Headers: Authorization: Bearer <session_token>
        Body:    { "model_id": "...", "hwid": "..." }

        Checks:
          1. Valid session token
          2. License active & not expired
          3. HWID matches a registered device
          4. Daily download limit not exceeded
          5. Free users cannot download premium models
          6. Generates a short-lived Cloudflare R2 signed URL
        """
        payload = self._require_auth()
        if not payload: return

        body     = self._body()
        model_id = body.get("model_id", "").strip()
        hwid     = body.get("hwid",     "").strip()
        gmail    = payload.get("sub")
        lid      = payload.get("lid")

        if not model_id:
            self._json(400, {"ok": False, "error": "model_id required"}); return

        conn = get_db()

        # ââ Re-verify license is still active & not expired âââââââââââââââââ
        lic = conn.execute("SELECT * FROM licenses WHERE id=? AND active=1", (lid,)).fetchone()
        if not lic:
            conn.close()
            self._json(403, {"ok": False, "error": "License suspended or not found."}); return

        if lic["expiry_date"]:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if lic["expiry_date"] < today:
                conn.close()
                self._json(403, {"ok": False, "error": "License expired.", "code": "EXPIRED"}); return

        # ââ Verify HWID is registered for this license âââââââââââââââââââââââ
        if hwid:
            hw_row = conn.execute(
                "SELECT 1 FROM registered_hwids WHERE license_id=? AND hwid=?", (lid, hwid)
            ).fetchone()
            if not hw_row:
                conn.close()
                self._log_activity(gmail, "download_hwid_mismatch", False, hwid)
                self._json(403, {"ok": False, "error": "Device not registered for this license.", "code": "HWID_MISMATCH"}); return

        # ââ Check daily download limit âââââââââââââââââââââââââââââââââââââââ
        daily_limit = lic["daily_download_limit"]
        if daily_limit is None:
            daily_limit = PLAN_DAILY_LIMITS.get(lic["plan_type"], 5)

        today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_count  = conn.execute(
            "SELECT COUNT(*) as n FROM download_logs WHERE license_id=? AND downloaded_at LIKE ?",
            (lid, f"{today_prefix}%")
        ).fetchone()["n"]

        if today_count >= daily_limit:
            conn.close()
            self._json(429, {
                "ok":    False,
                "error": f"Daily download limit reached ({daily_limit}/day). Resets at midnight UTC.",
                "code":  "DAILY_LIMIT",
                "limit": daily_limit,
                "used":  today_count,
            }); return

        # ââ Resolve model â R2 path & bucket âââââââââââââââââââââââââââââââââ
        # model_id can be either:
        #   (a) A raw R2 path from abimcon-assets  e.g. "SN/house.skp"
        #   (b) A legacy DB UUID from abimcon-models
        r2_key    = None
        r2_bucket = R2_BUCKET_NAME
        model_name = model_id  # fallback display name

        if "/" in model_id or model_id.lower().endswith(".skp"):
            # ââ Direct R2 path (abimcon-assets dynamic scan(ââââââââââââââ
            r2_key    = model_id
            r2_bucket = R2_ASSETS_BUCKET
            base_filename = model_id.split("/")[-1]
            model_name = base_filename[:-4] if base_filename.lower().endswith(".skp") else base_filename
            # Plan check: folders named "pro" require pro plan
            folder = model_id.split("/")[0] if "/" in model_id else ""
            if folder.lower() == "pro" and lic["plan_type"] != "pro":
                conn.close()
                self._json(403, {
                    "ok": False,
                    "error": "This model requires a Pro plan. Please upgrade.",
                    "code": "PLAN_REQUIRED"
                }); return
        else:
            # ââ Legacy DB lookup (abimcon-models bucket) ââââââââââââââââââ
            model = conn.execute("SELECT * FROM models WHERE model_id=?", (model_id,)).fetchone()
            if not model:
                conn.close()
                self._json(404, {"ok": False, "error": "Model not found."}); return
            keys = model.keys()
            model_plan = model["plan_required"] if "plan_required" in keys else ("pro" if model["is_premium"] else "free")
            if model_plan == "pro" and lic["plan_type"] != "pro":
                conn.close()
                self._json(403, {
                    "ok": False, "error": "This model requires a Pro plan. Please upgrade.",
                    "code": "PLAN_REQUIRED"
                }); return
            r2_key    = model["r2_path"]
            r2_bucket = R2_BUCKET_NAME
            model_name = model["name"]

        # ââ Generate signed URL ââââââââââââââââââââââââââââââââââââââââââââââ
        signed_url = generate_r2_signed_url(r2_key, bucket=r2_bucket)
        if not signed_url:
            conn.close()
            self._json(503, {"ok": False, "error": "Download service unavailable. R2 not configured."}); return

        # ââ Log the download âââââââââââââââââââââââââââââââââââââââââââââââââ
        now_ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.execute(
            "INSERT INTO download_logs (license_id,model_id,downloaded_at,ip_addr,hwid) VALUES (?,?,?,?,?)",
            (lid, model_id, now_ts, self._get_client_ip(), hwid or None)
        )
        conn.commit(); conn.close()
        self._log_activity(gmail, "model_download", True, hwid)

        self._json(200, {
            "ok":         True,
            "url":        signed_url,
            "filename":   model_name + ".skp",
            "expires_in": R2_SIGNED_URL_TTL,
            "model_name": model_name,
            "used_today": today_count + 1,
            "limit_today": daily_limit,
        })

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ License Admin Handlers ââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _get_licenses(self):
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute("SELECT * FROM licenses ORDER BY created_at").fetchall()
        logs = conn.execute(
            "SELECT COUNT(*) as n FROM activity_log WHERE action='sketchup_login' AND success=1"
        ).fetchone()

        lics = []
        today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for r in rows:
            d = dict(r)
            d["device_count"] = conn.execute(
                "SELECT COUNT(*) as n FROM registered_hwids WHERE license_id=?", (d["id"],)
            ).fetchone()["n"]
            d["downloads_today"] = conn.execute(
                "SELECT COUNT(*) as n FROM download_logs WHERE license_id=? AND downloaded_at LIKE ?",
                (d["id"], f"{today_prefix}%")
            ).fetchone()["n"]
            # Fetch registered HWIDs for display
            hw_rows = conn.execute(
                "SELECT hwid, last_seen, ip_addr FROM registered_hwids WHERE license_id=? ORDER BY last_seen DESC",
                (d["id"],)
            ).fetchall()
            d["hwids"] = [dict(h) for h in hw_rows]
            lics.append(d)

        conn.close()
        self._json(200, {
            "licenses":     lics,
            "total":        len(lics),
            "active":       sum(1 for l in lics if l["active"]),
            "suspended":    sum(1 for l in lics if not l["active"]),
            "total_logins": logs["n"] if logs else 0,
        })

    def _get_logs(self):
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute("SELECT * FROM activity_log ORDER BY id DESC LIMIT 100").fetchall()
        conn.close()
        self._json(200, {"logs": [dict(r) for r in rows]})

    def _get_hwids(self, lid):
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM registered_hwids WHERE license_id=? ORDER BY last_seen DESC", (lid,)
        ).fetchall()
        conn.close()
        self._json(200, {"hwids": [dict(r) for r in rows]})

    def _get_download_stats(self, lid):
        if not self._require_admin(): return
        conn = get_db()
        today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_count  = conn.execute(
            "SELECT COUNT(*) as n FROM download_logs WHERE license_id=? AND downloaded_at LIKE ?",
            (lid, f"{today_prefix}%")
        ).fetchone()["n"]
        recent = conn.execute(
            "SELECT dl.*, m.name as model_name FROM download_logs dl "
            "LEFT JOIN models m ON dl.model_id=m.model_id "
            "WHERE dl.license_id=? ORDER BY dl.id DESC LIMIT 20", (lid,)
        ).fetchall()
        conn.close()
        self._json(200, {"today": today_count, "recent": [dict(r) for r in recent]})

    def _reset_hwids(self, lid):
        if not self._require_admin(): return
        conn = get_db()
        count = conn.execute("SELECT COUNT(*) as n FROM registered_hwids WHERE license_id=?", (lid,)).fetchone()["n"]
        conn.execute("DELETE FROM registered_hwids WHERE license_id=?", (lid,))
        conn.commit(); conn.close()
        self._json(200, {"ok": True, "unbound": count})

    def _reset_downloads(self, lid):
        if not self._require_admin(): return
        conn = get_db()
        today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        count = conn.execute(
            "SELECT COUNT(*) as n FROM download_logs WHERE license_id=? AND downloaded_at LIKE ?",
            (lid, f"{today_prefix}%")
        ).fetchone()["n"]
        conn.execute(
            "DELETE FROM download_logs WHERE license_id=? AND downloaded_at LIKE ?",
            (lid, f"{today_prefix}%")
        )
        conn.commit(); conn.close()
        self._json(200, {"ok": True, "reset": count})

    def _add_license(self):
        if not self._require_admin(): return
        body    = self._body()
        name    = body.get("name",    "").strip()
        gmail   = body.get("gmail",   "").strip().lower()
        key     = body.get("license_key", "").strip().upper()
        role    = body.get("role",    "editor")
        plan    = body.get("plan_type",   "free")
        max_dev = int(body.get("max_devices", 2))
        expiry  = body.get("expiry_date", None) or None
        ddl     = body.get("daily_download_limit", None)
        if ddl is not None: ddl = int(ddl)
        # Trial plan: auto-set expiry if not provided
        if plan == "trial" and not expiry:
            trial_days = int(body.get("trial_days", 10))
            expiry = (datetime.now(timezone.utc) + timedelta(days=trial_days)).strftime("%Y-%m-%d")
        if not name or not gmail or not key:
            self._json(400, {"error": "name, gmail, license_key required"}); return
        now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO licenses (id,gmail,license_key,name,role,plan_type,active,max_devices,daily_download_limit,expiry_date,created_at) "
                "VALUES (?,?,?,?,?,?,1,?,?,?,?)",
                (str(uuid.uuid4()), gmail, key, name, role, plan, max_dev, ddl, expiry, now)
            )
            conn.commit()
            self._json(201, {"ok": True})
        except sqlite3.IntegrityError as e:
            self._json(409, {"error": f"Duplicate email or key: {e}"})
        finally:
            conn.close()

    def _update_license(self, lid):
        if not self._require_admin(): return
        body = self._body()
        fields, vals = [], []
        for col in ("name", "gmail", "license_key", "role", "plan_type", "expiry_date"):
            if col in body:
                fields.append(f"{col}=?")
                v = body[col].strip() if body[col] else None
                if col == "gmail" and v:       v = v.lower()
                if col == "license_key" and v: v = v.upper()
                vals.append(v)
        if "active" in body:
            fields.append("active=?"); vals.append(1 if body["active"] else 0)
        if "max_devices" in body:
            fields.append("max_devices=?"); vals.append(max(1, int(body["max_devices"])))
        if "daily_download_limit" in body:
            fields.append("daily_download_limit=?")
            vals.append(int(body["daily_download_limit"]) if body["daily_download_limit"] is not None else None)
        if not fields:
            self._json(400, {"error": "Nothing to update"}); return
        vals.append(lid)
        conn = get_db()
        try:
            conn.execute(f"UPDATE licenses SET {','.join(fields)} WHERE id=?", vals)
            conn.commit()
            self._json(200, {"ok": True})
        except sqlite3.IntegrityError as e:
            self._json(409, {"error": f"Duplicate: {e}"})
        finally:
            conn.close()

    def _extend_license(self, lid):
        """POST /api/admin/licenses/:id/extend  { "days": 90 }  OR  { "expiry_date": "2027-01-01" }"""
        if not self._require_admin(): return
        body = self._body()
        from datetime import date as _date_cls, timedelta as _td
        conn = get_db()
        row  = conn.execute("SELECT expiry_date FROM licenses WHERE id=?", (lid,)).fetchone()
        if not row:
            conn.close()
            self._json(404, {"error": "License not found"}); return

        if "expiry_date" in body and body["expiry_date"]:
            new_expiry = body["expiry_date"].strip()
        elif "days" in body:
            try:
                days = int(body["days"])
            except (TypeError, ValueError):
                conn.close()
                self._json(400, {"error": "days must be an integer"}); return
            base = _date_cls.today()
            if row["expiry_date"]:
                try:
                    existing = _date_cls.fromisoformat(row["expiry_date"])
                    if existing > base:
                        base = existing        # extend from current expiry, not today
                except Exception:
                    pass
            new_expiry = (base + _td(days=days)).isoformat()
        else:
            conn.close()
            self._json(400, {"error": "Provide 'days' or 'expiry_date'"}); return

        conn.execute("UPDATE licenses SET expiry_date=? WHERE id=?", (new_expiry, lid))
        conn.commit(); conn.close()
        self._json(200, {"ok": True, "expiry_date": new_expiry})

    def _delete_license(self, lid):
        if not self._require_admin(): return
        conn = get_db()
        conn.execute("DELETE FROM registered_hwids WHERE license_id=?", (lid,))
        conn.execute("DELETE FROM download_logs     WHERE license_id=?", (lid,))
        conn.execute("DELETE FROM licenses          WHERE id=?",         (lid,))
        conn.commit(); conn.close()
        self._json(200, {"ok": True})

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ Plan Feature Management âââââââââââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _get_plan_features_public(self):
        """GET /api/plan-features?plan=pro â no auth, for SketchUp extension."""
        qs = parse_qs(urlparse(self.path).query)
        plan_type = qs.get("plan", ["free"])[0].strip().lower()
        if not re.match(r'^[a-z0-9_]+$', plan_type):
            self._json(400, {"error": "Invalid plan"}); return
        conn = get_db()
        rows = conn.execute(
            "SELECT feature_key, enabled FROM plan_features WHERE plan_type=?",
            (plan_type,)
        ).fetchall()
        conn.close()
        features = {r["feature_key"]: bool(r["enabled"]) for r in rows}
        self._json(200, {"plan": plan_type, "features": features})

    def _get_plan_features(self):
        """GET /api/admin/plan-features â return feature matrix for admin UI."""
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute(
            "SELECT plan_type, feature_key, label, enabled, sort_order "
            "FROM plan_features ORDER BY sort_order, feature_key"
        ).fetchall()
        conn.close()

        plans    = []
        features = {}   # feature_key -> {label, sort_order}
        matrix   = {}   # plan_type   -> {feature_key -> bool}

        for r in rows:
            pt, fk = r["plan_type"], r["feature_key"]
            if pt not in plans:
                plans.append(pt)
                matrix[pt] = {}
            if fk not in features:
                features[fk] = {"label": r["label"], "sort_order": r["sort_order"]}
            matrix[pt][fk] = bool(r["enabled"])

        # canonical plan order: free, pro, trial, then any custom
        order = ["free", "pro", "trial"]
        plans = sorted(plans, key=lambda p: order.index(p) if p in order else 99)

        self._json(200, {"plans": plans, "features": features, "matrix": matrix})

    def _update_plan_feature(self):
        """PUT /api/admin/plan-features â toggle one feature for one plan."""
        if not self._require_admin(): return
        body        = self._body()
        plan_type   = body.get("plan_type",   "").strip()
        feature_key = body.get("feature_key", "").strip()
        enabled     = bool(body.get("enabled", False))

        if not plan_type or not feature_key:
            self._json(400, {"error": "plan_type and feature_key required"}); return

        conn = get_db()
        row = conn.execute(
            "SELECT id FROM plan_features WHERE plan_type=? AND feature_key=?",
            (plan_type, feature_key)
        ).fetchone()

        if row:
            conn.execute(
                "UPDATE plan_features SET enabled=? WHERE plan_type=? AND feature_key=?",
                (1 if enabled else 0, plan_type, feature_key)
            )
        else:
            label      = body.get("label",      feature_key.replace("_", " ").title())
            sort_order = body.get("sort_order",  99)
            conn.execute(
                "INSERT INTO plan_features (id,plan_type,feature_key,label,enabled,sort_order) "
                "VALUES (?,?,?,?,?,?)",
                (str(uuid.uuid4()), plan_type, feature_key, label, 1 if enabled else 0, sort_order)
            )
        conn.commit(); conn.close()
        self._json(200, {"ok": True})

    def _add_plan_or_feature(self):
        """POST /api/admin/plan-features â add a new plan or a new feature key.
        Body: { "action": "add_plan",    "plan_type": "enterprise", "default_enabled": true }
           or { "action": "add_feature", "feature_key": "reports",  "label": "Reports" }
        """
        if not self._require_admin(): return
        body   = self._body()
        action = body.get("action", "").strip()
        conn   = get_db()

        if action == "add_plan":
            plan_type = body.get("plan_type", "").strip().lower()
            if not plan_type or not re.match(r'^[a-z0-9_]+$', plan_type):
                conn.close()
                self._json(400, {"error": "Invalid plan_type (lowercase alphanumeric + underscore only)"}); return
            # Check if already exists
            if conn.execute("SELECT COUNT(*) FROM plan_features WHERE plan_type=?", (plan_type,)).fetchone()[0] > 0:
                conn.close()
                self._json(409, {"error": f"Plan '{plan_type}' already exists"}); return
            # Copy feature structure from 'pro' (all enabled by default if default_enabled=true)
            default_enabled = 1 if body.get("default_enabled", True) else 0
            templates = conn.execute(
                "SELECT feature_key, label, sort_order FROM plan_features WHERE plan_type='pro'"
            ).fetchall()
            for t in templates:
                conn.execute(
                    "INSERT INTO plan_features (id,plan_type,feature_key,label,enabled,sort_order) "
                    "VALUES (?,?,?,?,?,?)",
                    (str(uuid.uuid4()), plan_type, t["feature_key"], t["label"], default_enabled, t["sort_order"])
                )
            conn.commit(); conn.close()
            self._json(201, {"ok": True, "plan_type": plan_type})

        elif action == "add_feature":
            feature_key = body.get("feature_key", "").strip().lower().replace(" ", "_")
            label       = body.get("label", feature_key.replace("_", " ").title())
            if not feature_key or not re.match(r'^[a-z0-9_]+$', feature_key):
                conn.close()
                self._json(400, {"error": "Invalid feature_key (lowercase alphanumeric + underscore only)"}); return
            max_sort = conn.execute("SELECT COALESCE(MAX(sort_order),0) FROM plan_features").fetchone()[0]
            plans    = conn.execute("SELECT DISTINCT plan_type FROM plan_features").fetchall()
            added = 0
            for p in plans:
                try:
                    conn.execute(
                        "INSERT INTO plan_features (id,plan_type,feature_key,label,enabled,sort_order) "
                        "VALUES (?,?,?,?,?,?)",
                        (str(uuid.uuid4()), p["plan_type"], feature_key, label, 0, max_sort + 1)
                    )
                    added += 1
                except sqlite3.IntegrityError:
                    pass
            conn.commit(); conn.close()
            self._json(201, {"ok": True, "feature_key": feature_key, "plans_updated": added})

        else:
            conn.close()
            self._json(400, {"error": "action must be 'add_plan' or 'add_feature'"})

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ AssetBrowser JS Endpoints (v3.1) ââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _license_by_key(self, key):
        """Validate a raw license key string. Returns (row, None) or (None, error_dict)."""
        if not key:
            return None, {"ok": False, "error": "License key required", "code": "NO_KEY"}
        conn = get_db()
        row  = conn.execute(
            "SELECT * FROM licenses WHERE license_key=? AND active=1", (key.strip().upper(),)
        ).fetchone()
        conn.close()
        if not row:
            return None, {"ok": False, "error": "Invalid or inactive license key", "code": "INVALID_KEY"}
        if row["expiry_date"]:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if row["expiry_date"] < today:
                return None, {"ok": False, "error": "License expired. Contact your administrator.", "code": "EXPIRED"}
        return row, None

    def _get_assets(self):
        """
        GET /api/assets
        Authorization: Bearer <license_key>   (raw key, not JWT)
        Returns asset list shaped for the AssetBrowser JS UI.
        """
        auth = self.headers.get("Authorization", "")
        key  = auth[7:].strip() if auth.startswith("Bearer ") else ""
        lic, err = self._license_by_key(key)
        if err:
            self._json(401, err); return
        plan = lic["plan_type"]
        conn = get_db()
        if plan == "pro":
            rows = conn.execute(
                "SELECT model_id, name, description, category, tags, thumbnail, "
                "file_size_mb, plan_required FROM models WHERE active=1 ORDER BY category, name"
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT model_id, name, description, category, tags, thumbnail, "
                "file_size_mb, plan_required FROM models WHERE active=1 AND plan_required='free' ORDER BY category, name"
            ).fetchall()
        conn.close()
        assets = []
        for r in rows:
            d = dict(r)
            d["id"] = d.pop("model_id")   # JS expects "id"
            d["thumbnail"] = _sign_thumbnail(d.get("thumbnail", ""))
            assets.append(d)
        self._json(200, {"ok": True, "plan": plan, "assets": assets})

    def _download(self):
        """
        POST /api/download
        Body: { license_key, hwid, model_id }
        Validates by raw license key (not JWT), enforces all limits, returns signed URL.
        """
        body     = self._body()
        key      = body.get("license_key", "").strip()
        hwid     = body.get("hwid",        "").strip()
        model_id = body.get("model_id",    "").strip()

        if not model_id:
            self._json(400, {"ok": False, "error": "model_id required"}); return

        lic, err = self._license_by_key(key)
        if err:
            self._json(401, err); return

        lid       = lic["id"]
        gmail     = lic["gmail"]
        plan      = lic["plan_type"]
        now_ts    = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        client_ip = self._get_client_ip()

        conn = get_db()

        # ââ HWID: register new devices, enforce device limit âââââââââââââââââ
        if hwid:
            hw_rows = conn.execute(
                "SELECT hwid FROM registered_hwids WHERE license_id=?", (lid,)
            ).fetchall()
            known   = [r["hwid"] for r in hw_rows]
            max_dev = lic["max_devices"] if lic["max_devices"] else 2
            if hwid in known:
                conn.execute(
                    "UPDATE registered_hwids SET last_seen=?, ip_addr=? WHERE license_id=? AND hwid=?",
                    (now_ts, client_ip, lid, hwid)
                )
            else:
                if len(known) >= max_dev:
                    conn.close()
                    self._log_activity(gmail, "download_hwid_limit", False, hwid)
                    self._json(403, {
                        "ok":    False,
                        "error": f"Device limit reached ({max_dev} devices). Contact your admin.",
                        "code":  "HWID_LIMIT",
                    }); return
                conn.execute(
                    "INSERT INTO registered_hwids (license_id,hwid,first_seen,last_seen,ip_addr) VALUES (?,?,?,?,?)",
                    (lid, hwid, now_ts, now_ts, client_ip)
                )
            conn.commit()

        # ââ Daily download limit âââââââââââââââââââââââââââââââââââââââââââââ
        daily_limit  = lic["daily_download_limit"]
        if daily_limit is None:
            daily_limit = PLAN_DAILY_LIMITS.get(plan, 5)
        today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        today_count  = conn.execute(
            "SELECT COUNT(*) as n FROM download_logs WHERE license_id=? AND downloaded_at LIKE ?",
            (lid, f"{today_prefix}%")
        ).fetchone()["n"]
        if today_count >= daily_limit:
            conn.close()
            self._json(429, {
                "ok":    False,
                "error": f"Daily download limit reached ({daily_limit}/day). Resets at midnight UTC.",
                "code":  "DAILY_LIMIT",
                "limit": daily_limit,
                "used":  today_count,
            }); return

        # ââ Get model ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        model = conn.execute("SELECT * FROM models WHERE model_id=?", (model_id,)).fetchone()
        if not model:
            conn.close()
            self._json(404, {"ok": False, "error": "Model not found.", "code": "MODEL_NOT_FOUND"}); return

        # ââ Plan check âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        keys = model.keys()
        model_plan = model["plan_required"] if "plan_required" in keys else ("pro" if model["is_premium"] else "free")
        if model_plan == "pro" and plan != "pro":
            conn.close()
            self._json(403, {"ok": False, "error": "This model requires a Pro plan. Please upgrade.", "code": "PLAN_REQUIRED"}); return

        # ââ Generate R2 signed URL âââââââââââââââââââââââââââââââââââââââââââ
        signed_url = generate_r2_signed_url(model["r2_path"])
        if not signed_url:
            conn.close()
            self._json(503, {"ok": False, "error": "Download service unavailable. R2 not configured."}); return

        # ââ Log the download âââââââââââââââââââââââââââââââââââââââââââââââââ
        conn.execute(
            "INSERT INTO download_logs (license_id,model_id,downloaded_at,ip_addr,hwid) VALUES (?,?,?,?,?)",
            (lid, model_id, now_ts, client_ip, hwid or None)
        )
        conn.commit(); conn.close()
        self._log_activity(gmail, "model_download", True, hwid)

        self._json(200, {
            "ok":         True,
            "url":        signed_url,
            "filename":   model["name"] + ".skp",
            "expires_in": R2_SIGNED_URL_TTL,
            "used_today": today_count + 1,
            "limit_today": daily_limit,
        })

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ User-facing Model List ââââââââââââââââââââââââââââââââââââââââââââââââ
    # GET /api/models
    # Requires: Authorization: Bearer <session_token>
    # Returns models filtered by the user's plan (free sees free only, pro sees all).
    def _get_user_models(self):
        """
        GET /api/models
        Requires: Authorization: Bearer <session_token>

        Dynamically scans the abimcon-assets R2 bucket.
        Every sub-folder becomes a Category.  All .skp files inside are
        returned as models.  Thumbnails are expected at the same path but
        with a .jpg or .png extension (signed URL is generated; the
        front-end onerror fallback handles missing ones gracefully).
        """
        payload = self._require_auth()
        if not payload: return
        plan = payload.get("plan", "free")

        if not HAS_BOTO3 or not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
            self._json(503, {"ok": False, "error": "R2 not configured on server."}); return

        try:
            s3 = boto3.client(
                "s3",
                endpoint_url          = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
                aws_access_key_id     = R2_ACCESS_KEY_ID,
                aws_secret_access_key = R2_SECRET_ACCESS_KEY,
                config                = BotoConfig(signature_version="s3v4"),
                region_name           = "auto",
            )

            # ââ Single-pass: collect ALL bucket keys + sizes ââââââââââââââ
            all_objects = {}   # key â size_bytes
            paginator   = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=R2_ASSETS_BUCKET):
                for obj in page.get("Contents", []):
                    all_objects[obj["Key"]] = obj["Size"]

            # Build a set of all non-skp keys for fast thumbnail existence check
            all_keys = set(all_objects.keys())

            models_out = []
            for key, size in all_objects.items():
                # Only .skp files; skip folder marker objects
                if not key.lower().endswith(".skp") or key.endswith("/"):
                    continue

                parts    = key.split("/")
                category = parts[0] if len(parts) > 1 else "General"
                filename = parts[-1]
                name     = filename[:-4]   # strip .skp extension

                # Folders named "pro" (case-insensitive) â pro-only models
                plan_required = "pro" if category.lower() == "pro" else "free"

                # Free users don't see pro-only models
                if plan_required == "pro" and plan != "pro":
                    continue

                # Thumbnail: only generate signed URL if .jpg or .png actually EXISTS
                base_key  = key[:-4]
                if base_key + ".jpg" in all_keys:
                    thumb_url = generate_r2_signed_url(base_key + ".jpg", ttl=3600, bucket=R2_ASSETS_BUCKET) or ""
                elif base_key + ".png" in all_keys:
                    thumb_url = generate_r2_signed_url(base_key + ".png", ttl=3600, bucket=R2_ASSETS_BUCKET) or ""
                else:
                    thumb_url = ""   # no thumbnail â HTML shows placeholder

                models_out.append({
                    "model_id":      key,           # R2 path used as unique ID
                    "name":          name,
                    "category":      category,
                    "thumbnail":     thumb_url,
                    "file_size_mb":  round(size / 1048576, 1),
                    "plan_required": plan_required,
                    "tags":          category.lower(),
                    "description":   "",
                })

            # Sort by category then name
            models_out.sort(key=lambda m: (m["category"].lower(), m["name"].lower()))
            self._json(200, {"ok": True, "plan": plan, "models": models_out})

        except Exception as e:
            print(f"[R2] list_objects error: {e}")
            self._json(500, {"ok": False, "error": f"R2 scan failed: {e}"})

    # ââ Model Admin Handlers ââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _get_models(self):
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute("SELECT * FROM models ORDER BY created_at DESC").fetchall()
        conn.close()
        self._json(200, {"models": [dict(r) for r in rows], "total": len(rows)})

    def _add_model(self):
        if not self._require_admin(): return
        body         = self._body()
        name         = body.get("name",         "").strip()
        r2_path      = body.get("r2_path",      "").strip()
        is_premium   = 1 if body.get("is_premium") else 0
        plan_required = body.get("plan_required", "pro" if is_premium else "free")
        file_size    = int(body.get("file_size", 0))
        file_size_mb = float(body.get("file_size_mb", round(file_size / 1048576, 2)))
        description  = body.get("description",  "").strip()
        category     = body.get("category",     "General").strip()
        tags         = body.get("tags",         "").strip()
        thumbnail    = body.get("thumbnail",    "").strip()
        if not name or not r2_path:
            self._json(400, {"error": "name and r2_path required"}); return
        now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn = get_db()
        mid  = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO models (model_id,name,r2_path,is_premium,plan_required,file_size,file_size_mb,"
            "description,category,tags,thumbnail,active,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,1,?)",
            (mid, name, r2_path, is_premium, plan_required, file_size, file_size_mb,
             description, category, tags, thumbnail, now)
        )
        conn.commit(); conn.close()
        self._json(201, {"ok": True, "model_id": mid})

    def _update_model(self, mid):
        if not self._require_admin(): return
        body = self._body()
        fields, vals = [], []
        for col in ("name", "r2_path", "description", "category", "tags", "thumbnail"):
            if col in body:
                fields.append(f"{col}=?"); vals.append(body[col].strip() if body[col] else "")
        if "is_premium" in body:
            ip = 1 if body["is_premium"] else 0
            fields.append("is_premium=?"); vals.append(ip)
            # Sync plan_required with is_premium if not explicitly provided
            if "plan_required" not in body:
                fields.append("plan_required=?"); vals.append("pro" if ip else "free")
        if "plan_required" in body:
            fields.append("plan_required=?"); vals.append(body["plan_required"] or "free")
        if "file_size" in body:
            fields.append("file_size=?"); vals.append(int(body["file_size"]))
        if "file_size_mb" in body:
            fields.append("file_size_mb=?"); vals.append(float(body["file_size_mb"] or 0))
        if "active" in body:
            fields.append("active=?"); vals.append(1 if body["active"] else 0)
        if not fields:
            self._json(400, {"error": "Nothing to update"}); return
        vals.append(mid)
        conn = get_db()
        conn.execute(f"UPDATE models SET {','.join(fields)} WHERE model_id=?", vals)
        conn.commit(); conn.close()
        self._json(200, {"ok": True})

    def _delete_model(self, mid):
        if not self._require_admin(): return
        conn = get_db()
        conn.execute("DELETE FROM models WHERE model_id=?", (mid,))
        conn.commit(); conn.close()
        self._json(200, {"ok": True})


    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ AI Suite (Banana Pro) âââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

    def _fetch_material_context(self, prompt: str) -> str:
        """Query Supabase boq_items for materials relevant to the prompt.

        Whitelist SELECT only: material_name, material_price, labor_name, labor_price.
        Never exposes gmail, password, user_id, or any PII.
        Returns a formatted string to inject into the system prompt, or '' if unavailable.
        """
        if not SUPABASE_URL or not SUPABASE_ANON_KEY:
            return ''
        # Simple keyword detection for construction topics
        construction_keywords = [
            'material', 'price', 'cost', 'concrete', 'steel', 'tile', 'brick', 'cement',
            'wood', 'glass', 'paint', 'plumbing', 'electrical', 'labour', 'labor',
            'floor', 'wall', 'roof', 'door', 'window', 'beam', 'column', 'footing',
        ]
        prompt_lower = prompt.lower()
        is_construction = any(kw in prompt_lower for kw in construction_keywords)
        if not is_construction:
            return ''
        try:
            url = (SUPABASE_URL.rstrip('/') +
                   "/rest/v1/boq_items"
                   "?select=material_name,material_price,labor_name,labor_price"
                   "&limit=20")
            req = _urllib_req.Request(url, headers={
                "apikey":        SUPABASE_ANON_KEY,
                "Authorization": f"Bearer {SUPABASE_ANON_KEY}",
                "Content-Type":  "application/json",
            })
            with _urllib_req.urlopen(req, timeout=5) as r:
                rows = json.loads(r.read())
            if not rows:
                return ''
            lines = ["Reference Material Prices (from project database):"]
            for row in rows[:20]:
                mn = row.get('material_name', '')
                mp = row.get('material_price', '')
                ln = row.get('labor_name', '')
                lp = row.get('labor_price', '')
                if mn:
                    lines.append(f"  - {mn}: {mp}" + (f" | Labor: {ln} {lp}" if ln else ''))
            return '\n'.join(lines)
        except Exception:
            return ''  # RAG is best-effort

    def _ai_validate_token_and_credits(self, cost):
        """Validate session token + check/deduct credits. Returns (payload, gmail, error)."""
        payload = self._require_auth()
        if not payload:
            return None, None, None   # _require_auth already sent response
        gmail = payload.get('sub', '')
        conn  = get_db()
        # Ensure wallet exists
        conn.execute(
            "INSERT OR IGNORE INTO ai_wallets (gmail, credits) VALUES (?, 10)",
            (gmail,)
        )
        row = conn.execute("SELECT credits FROM ai_wallets WHERE gmail=?", (gmail,)).fetchone()
        credits = row['credits'] if row else 0
        if credits < cost:
            conn.close()
            self._json(402, {"ok": False, "error": f"Insufficient credits. You need {cost} but have {credits}.", "code": "INSUFFICIENT_CREDITS"})
            return None, None, True
        conn.close()
        return payload, gmail, False

    def _ai_deduct_credits(self, gmail, cost, tx_type, description):
        conn = get_db()
        conn.execute("UPDATE ai_wallets SET credits = credits - ?, total_used = total_used + ? WHERE gmail=?", (cost, cost, gmail))
        row = conn.execute("SELECT credits FROM ai_wallets WHERE gmail=?", (gmail,)).fetchone()
        balance = row['credits'] if row else 0
        conn.execute(
            "INSERT INTO ai_transactions (gmail, type, amount, balance_after, description) VALUES (?,?,?,?,?)",
            (gmail, tx_type, -cost, balance, description)
        )
        conn.commit(); conn.close()
        return balance

    def _ai_chat(self):
        """POST /api/ai/chat -- BIM Assistant multi-turn chat via Gemini (1 credit, atomic refund on failure)"""
        if not GEMINI_API_KEY:
            self._json(503, {"ok": False, "error": "AI service not configured"}); return
        payload, gmail, err = self._ai_validate_token_and_credits(AI_CHAT_COST)
        if payload is None: return

        body              = self._body()
        user_message      = body.get("message", "").strip()
        context_json      = body.get("context_json")
        image_b64         = body.get("image_base64")
        previous_messages = body.get("previous_messages", [])

        if not user_message:
            self._json(400, {"ok": False, "error": "message is required"}); return

        credits_after = self._ai_deduct_credits(gmail, AI_CHAT_COST, "chat", "Chat: "+user_message[:60])

        material_context = self._fetch_material_context(user_message)

        system_prompt = (
            "You are a professional BIM & BOQ Expert built into AbimconStudio SketchUp extension. "
            "Always provide comprehensive, step-by-step reasoning for construction calculations. "
            "Do not summarize unless explicitly asked. "
            "When given BOQ JSON data: systematically analyze every line item \u2014 quantities, unit rates, and totals; "
            "identify cost-saving opportunities, over-estimates, or missing items; "
            "provide detailed step-by-step breakdowns of labour, materials, and overhead; "
            "compare costs against regional market benchmarks; "
            "give professional procurement, scheduling, and sequencing recommendations. "
            "Always use clear section headings and numbered steps. "
            "Never truncate \u2014 complete every section fully before finishing. "
            "Use Lao/Thai construction context and terminology where relevant."
        )
        if material_context:
            system_prompt += f"\n\n{material_context}"

        contents = []
        for msg in previous_messages:
            role    = msg.get("role", "user")
            content = msg.get("content", "")
            if role in ("user", "model") and content:
                contents.append({"role": role, "parts": [{"text": content}]})

        parts = []
        if context_json:
            parts.append({"text": f"BOQ Context Data:\n{json.dumps(context_json, ensure_ascii=False, indent=2)}\n\n"})
        if image_b64:
            mime = "image/jpeg" if body.get("image_mime", "jpeg") == "jpeg" else "image/png"
            parts.append({"inlineData": {"mimeType": mime, "data": image_b64}})
        parts.append({"text": user_message})
        contents.append({"role": "user", "parts": parts})

        gemini_body = {
            "system_instruction": {"parts": [{"text": system_prompt}]},
            "contents": contents,
            "generationConfig": {"temperature": 0.7, "maxOutputTokens": 4096}
        }

        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_CHAT_MODEL}:generateContent?key={GEMINI_API_KEY}"
            req = _urllib_req.Request(url, data=json.dumps(gemini_body).encode(), headers={"Content-Type": "application/json"})
            with _urllib_req.urlopen(req, timeout=90) as r:
                resp_data = json.loads(r.read())
            candidate    = resp_data["candidates"][0]
            ai_text      = candidate["content"]["parts"][0]["text"]
            finish_reason = candidate.get("finishReason", "STOP")
        except Exception as e:
            try:
                conn = get_db()
                conn.execute("UPDATE ai_wallets SET credits = credits + ?, total_used = total_used - ? WHERE gmail=?",
                             (AI_CHAT_COST, AI_CHAT_COST, gmail))
                conn.execute(
                    "INSERT INTO ai_transactions (gmail, type, amount, balance_after, description) VALUES (?,?,?,?,?)",
                    (gmail, 'refund', AI_CHAT_COST,
                     conn.execute("SELECT credits FROM ai_wallets WHERE gmail=?", (gmail,)).fetchone()['credits'],
                     f"Auto-refund: Gemini chat error")
                )
                conn.commit(); conn.close()
            except Exception:
                pass
            self._json(500, {"ok": False, "error": f"Gemini API error: {str(e)}"}); return

        self._json(200, {"ok": True, "response": ai_text, "finish_reason": finish_reason, "credits_after": credits_after})

    def _ai_image(self):
        """POST /api/ai/image -- Multi-image Gemini generation (10 credits x count, atomic refund on failure)"""
        if not GEMINI_API_KEY:
            self._json(503, {"ok": False, "error": "AI service not configured"}); return

        body          = self._body()
        prompt        = body.get("prompt", "").strip()
        style         = body.get("style", "").strip()
        image_b64     = body.get("image_base64")
        inspo_b64     = body.get("inspiration_base64")
        image_count   = max(1, min(4, int(body.get("image_count", 1))))
        resolution    = body.get("resolution", "2048x2048")
        aspect_ratio  = body.get("aspect_ratio", "1:1")

        if not prompt:
            self._json(400, {"ok": False, "error": "prompt is required"}); return

        cost_per   = RESOLUTION_COSTS.get(resolution, 25)
        total_cost = cost_per * image_count
        payload, gmail, err = self._ai_validate_token_and_credits(total_cost)
        if payload is None: return

        credits_after = self._ai_deduct_credits(
            gmail, total_cost, "image",
            f"Image Gen x{image_count}: " + prompt[:50]
        )

        # ── Build expanded prompt (secret sauce) ────────────────────────────────────
        style_desc = style if style else "Realistic"
        _ar_labels = {"16:9": "16:9 widescreen landscape", "9:16": "9:16 portrait vertical",
                      "4:3": "4:3 standard landscape", "3:4": "3:4 portrait", "1:1": ""}
        ar_hint    = _ar_labels.get(str(aspect_ratio), f"aspect ratio {aspect_ratio}") if aspect_ratio != "1:1" else ""
        lighting   = body.get("lighting", "cinematic lighting")

        # Resolution quality tier
        if resolution == "4096x4096":
            quality_tags = "8K ultra resolution, hyperrealistic, extreme detail, ray-traced global illumination, masterpiece"
        elif resolution == "2048x2048":
            quality_tags = "4K resolution, photorealistic, highly detailed textures, ray-tracing, cinematic lighting, sharp focus, masterpiece"
        else:
            quality_tags = "high resolution, photorealistic, detailed, cinematic lighting, sharp focus"

        # Negative prompt — appended to every request
        negative_prompt = "blurry, distorted geometry, low resolution, messy, low quality, watermark, text overlay, overexposed, underexposed, cartoonish, sketch, noise, artifacts"

        has_ref = bool(image_b64 or inspo_b64)
        if has_ref:
            full_prompt = (
                f"High-end architectural visualization of {prompt}, {style_desc} style, "
                f"{lighting}, {quality_tags}"
                + (f", {ar_hint}" if ar_hint else "")
                + f". KEEP GEOMETRY 100% IDENTICAL to the attached SketchUp viewport — "
                f"apply only the lighting, materials, landscaping and atmosphere from the inspiration image. "
                f"Negative: {negative_prompt}."
            )
        else:
            full_prompt = (
                f"High-end architectural visualization of {prompt}, {style_desc} style, "
                f"{lighting}, {quality_tags}"
                + (f", {ar_hint}" if ar_hint else "")
                + f". Professional architectural render, ultra-sharp, magazine quality. "
                f"Negative: {negative_prompt}."
            )

        # ── Map aspect ratio to API values ────────────────────────────────────────
        _ar_map = {
            "1:1": "1:1", "square": "1:1",
            "4:3": "4:3", "standard": "4:3",
            "3:4": "3:4", "portrait": "3:4",
            "16:9": "16:9", "wide": "16:9",
            "9:16": "9:16", "story": "9:16",
        }
        api_ar = _ar_map.get(str(aspect_ratio).lower(), "1:1")

        # ── Generate image_count images ───────────────────────────────────────────
        # ── Resolve model + imageSize from resolution tier ────────────────────────
        tier       = GEMINI_IMAGE_TIERS.get(resolution, GEMINI_IMAGE_TIERS['1024x1024'])
        img_model  = tier['model']
        img_size   = tier['imageSize']

        # Map aspect ratio string to Gemini imageConfig value
        _ar_map = {
            '1:1': '1:1', 'square': '1:1',
            '4:3': '4:3', 'standard': '4:3',
            '3:4': '3:4', 'portrait': '3:4',
            '16:9': '16:9', 'wide': '16:9',
            '9:16': '9:16', 'story': '9:16',
        }
        api_ar = _ar_map.get(str(aspect_ratio).lower(), '1:1')

        # ── Generate image_count images ───────────────────────────────────────────
        images = []
        last_error = None

        for i in range(image_count):
            try:
                gemini_parts = []
                ref = image_b64 or inspo_b64
                if ref:
                    ref_mime_key = 'image_mime' if image_b64 else 'inspiration_mime'
                    mime = 'image/jpeg' if body.get(ref_mime_key, 'jpeg') == 'jpeg' else 'image/png'
                    gemini_parts.append({'inlineData': {'mimeType': mime, 'data': ref}})
                gemini_parts.append({'text': full_prompt})

                # imageConfig.imageSize = true resolution tier (1K/2K/4K)
                # imageConfig.aspectRatio = proper aspect ratio support
                # IMPORTANT: use imageConfig, NOT imageGenerationConfig (causes HTTP 400)
                gemini_body = {
                    'contents': [{'role': 'user', 'parts': gemini_parts}],
                    'generationConfig': {
                        'responseModalities': ['TEXT', 'IMAGE'],
                        'imageConfig': {
                            'imageSize':   img_size,
                            'aspectRatio': api_ar
                        }
                    }
                }
                url = (f'https://generativelanguage.googleapis.com/v1beta/models/'
                       f'{img_model}:generateContent?key={GEMINI_API_KEY}')
                req = _urllib_req.Request(url, data=json.dumps(gemini_body).encode(),
                                          headers={'Content-Type': 'application/json'})
                with _urllib_req.urlopen(req, timeout=90) as r:
                    resp_data = json.loads(r.read())

                img_b64 = None
                candidates = resp_data.get('candidates', [])
                if candidates:
                    for part in candidates[0].get('content', {}).get('parts', []):
                        if 'inlineData' in part:
                            img_b64 = part['inlineData']['data']
                            break

                if not img_b64:
                    finish    = candidates[0].get('finishReason', '?') if candidates else 'no_candidates'
                    pts_debug = [list(p.keys()) for p in candidates[0].get('content', {}).get('parts', [])] if candidates else []
                    raise ValueError(f'No image in response #{i+1}. finishReason={finish}, parts={pts_debug}')

                images.append(img_b64)

            except Exception as e:
                last_error = str(e)
                break
        generated = len(images)
        failed    = image_count - generated
        if failed > 0:
            refund_amt = cost_per * failed
            try:
                conn = get_db()
                conn.execute(
                    "UPDATE ai_wallets SET credits = credits + ?, total_used = total_used - ? WHERE gmail=?",
                    (refund_amt, refund_amt, gmail)
                )
                new_bal = conn.execute("SELECT credits FROM ai_wallets WHERE gmail=?", (gmail,)).fetchone()['credits']
                conn.execute(
                    "INSERT INTO ai_transactions (gmail, type, amount, balance_after, description) VALUES (?,?,?,?,?)",
                    (gmail, 'refund', refund_amt, new_bal,
                     f"Partial refund: {failed} image(s) failed to generate")
                )
                conn.commit(); conn.close()
                credits_after = new_bal
            except Exception:
                pass

        if not images:
            self._json(500, {"ok": False,
                             "error": f"Image generation error: {last_error}"}); return

        self._json(200, {
            "ok":           True,
            "images":       images,
            "image_base64": images[0],
            "count":        generated,
            "credits_after": credits_after,
        })

    def _ai_extract_params(self):
        """POST /api/ai/extract-params — Analyse inspiration image → 4 render profile params (1 credit)"""
        if not GEMINI_API_KEY:
            self._json(503, {"ok": False, "error": "AI service not configured"}); return

        body      = self._body()
        inspo_b64 = body.get("inspiration_base64", "").strip()
        inspo_mime= body.get("inspiration_mime", "jpeg")
        section_type = body.get("type", "quick")   # "quick" = legacy 4-param; section key = new detailed

        # │─ New: section-specific deep analysis │────────────────────
        if section_type in SECTION_ANALYSIS_PROMPTS:
            # For 'camera' section, prefer the base viewport; otherwise use inspiration image
            # Section 1 (materials) uses the SketchUp 3D base viewport (geometry locked).
            # All other sections use the inspiration / reference image.
            base_b64  = body.get("base_base64", "").strip()
            base_mime = body.get("base_mime",   "jpeg")
            if section_type == "materials" and base_b64:
                img_b64  = base_b64
                img_mime = base_mime
            else:
                img_b64  = inspo_b64
                img_mime = inspo_mime
            if not img_b64:
                self._json(400, {"ok": False, "error": "No image provided for section analysis"}); return

            payload, gmail, err = self._ai_validate_token_and_credits(AI_EXTRACT_COST)
            if payload is None: return

            credits_after = self._ai_deduct_credits(
                gmail, AI_EXTRACT_COST, f"analyze_{section_type}",
                f"Section analysis: {section_type}"
            )

            mime_str    = "image/jpeg" if img_mime in ("jpeg", "jpg") else "image/png"
            prompt_text = SECTION_ANALYSIS_PROMPTS[section_type]

            gemini_body = {
                "contents": [{"role": "user", "parts": [
                    {"inlineData": {"mimeType": mime_str, "data": img_b64}},
                    {"text": prompt_text}
                ]}],
                "generationConfig": {"temperature": 0.4, "maxOutputTokens": 2048}
            }

            try:
                url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
                       f"{GEMINI_CHAT_MODEL}:generateContent?key={GEMINI_API_KEY}")
                req = _urllib_req.Request(url, data=json.dumps(gemini_body).encode(),
                                          headers={"Content-Type": "application/json"})
                with _urllib_req.urlopen(req, timeout=60) as r:
                    resp_data = json.loads(r.read())
                result_text = resp_data["candidates"][0]["content"]["parts"][0]["text"].strip()
            except Exception as e:
                # Full refund on failure
                try:
                    conn = get_db()
                    conn.execute(
                        "UPDATE ai_wallets SET credits = credits + ?, total_used = total_used - ? WHERE gmail=?",
                        (AI_EXTRACT_COST, AI_EXTRACT_COST, gmail)
                    )
                    conn.commit(); conn.close()
                except Exception:
                    pass
                self._json(500, {"ok": False, "error": f"Analysis failed: {e}",
                                  "type": section_type}); return

            self._json(200, {
                "ok":           True,
                "type":         section_type,
                "result":       result_text,
                "credits_after": credits_after,
            }); return

        if not inspo_b64:
            self._json(400, {"ok": False, "error": "inspiration_base64 is required"}); return

        payload, gmail, err = self._ai_validate_token_and_credits(AI_EXTRACT_COST)
        if payload is None: return

        credits_after = self._ai_deduct_credits(
            gmail, AI_EXTRACT_COST, "extract_params",
            "Render profile extraction from inspiration image"
        )

        mime_str = "image/jpeg" if inspo_mime == "jpeg" else "image/png"
        analysis_prompt = (
            "Analyse this architectural inspiration image and extract 4 render parameters. "
            "Rules: output ONLY a raw JSON object. No markdown, no code fences, no explanations. "
            "Values must be plain text with NO quotation marks, NO apostrophes, NO special characters. "
            "Keep each value under 10 words. Use these 4 keys:\n"
            "landscape_context: site/landscape (example: tropical garden urban street mountain hillside)\n"
            "sky_condition: sky and lighting (example: golden hour sunset overcast midday clear blue sky)\n"
            "cars_props: vehicles and props (example: modern SUVs parked no vehicles light street traffic)\n"
            "mood_tone: mood and colour tone (example: warm inviting cool minimalist dramatic moody)\n"
            "Copy this format exactly and fill VALUES without any quotes inside them:\n"
            '{"landscape_context":"VALUE","sky_condition":"VALUE","cars_props":"VALUE","mood_tone":"VALUE"}'
        )
        gemini_body = {
            "contents": [{
                "role": "user",
                "parts": [
                    {"inlineData": {"mimeType": mime_str, "data": inspo_b64}},
                    {"text": analysis_prompt}
                ]
            }],
            "generationConfig": {"temperature": 0.3, "maxOutputTokens": 512}
        }

        try:
            url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
                   f"{GEMINI_CHAT_MODEL}:generateContent?key={GEMINI_API_KEY}")
            req = _urllib_req.Request(url, data=json.dumps(gemini_body).encode(),
                                      headers={"Content-Type": "application/json"})
            with _urllib_req.urlopen(req, timeout=60) as r:
                resp_data = json.loads(r.read())

            raw_text = resp_data["candidates"][0]["content"]["parts"][0]["text"].strip()

            # ── Robust JSON extraction ──────────────────────────────
            # 1) Strip markdown fences
            if "```" in raw_text:
                parts_md = raw_text.split("```")
                for part_md in parts_md:
                    part_md = part_md.strip()
                    if part_md.startswith("json"):
                        part_md = part_md[4:].strip()
                    if part_md.startswith("{"):
                        raw_text = part_md
                        break
            # 2) Extract only the {...} block (ignore prose around JSON)
            brace_s = raw_text.find("{")
            brace_e = raw_text.rfind("}")
            if brace_s != -1 and brace_e != -1:
                raw_text = raw_text[brace_s : brace_e + 1]
            # 3) Collapse literal newlines inside the JSON string
            raw_text = raw_text.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
            import re as _re
            raw_text = _re.sub(r'  +', ' ', raw_text)
            # 4) Try strict JSON parse; fall back to per-key regex (handles unescaped quotes)
            try:
                params = json.loads(raw_text)
            except Exception:
                def _extract_val(txt, key):
                    m = _re.search(r'"' + key + r'"\s*:\s*"(.*?)"(?=\s*[,}])', txt, _re.DOTALL)
                    if m: return m.group(1).strip()
                    m2 = _re.search(r'"' + key + r'"\s*:\s*"([^"]{3,})', txt)
                    return m2.group(1).strip() if m2 else "unknown"
                params = {
                    "landscape_context": _extract_val(raw_text, "landscape_context"),
                    "sky_condition":     _extract_val(raw_text, "sky_condition"),
                    "cars_props":        _extract_val(raw_text, "cars_props"),
                    "mood_tone":         _extract_val(raw_text, "mood_tone"),
                }
            params = json.loads(raw_text)
            required = {"landscape_context", "sky_condition", "cars_props", "mood_tone"}
            if not required.issubset(params.keys()):
                raise ValueError(f"Missing keys in response: {required - set(params.keys())}")

        except Exception as e:
            # Full refund on failure
            try:
                conn = get_db()
                conn.execute(
                    "UPDATE ai_wallets SET credits = credits + ?, total_used = total_used - ? WHERE gmail=?",
                    (AI_EXTRACT_COST, AI_EXTRACT_COST, gmail)
                )
                conn.execute(
                    "INSERT INTO ai_transactions (gmail, type, amount, balance_after, description) VALUES (?,?,?,?,?)",
                    (gmail, 'refund', AI_EXTRACT_COST,
                     credits_after + AI_EXTRACT_COST,
                     "Refund: param extraction failed")
                )
                conn.commit(); conn.close()
            except Exception:
                pass
            self._json(500, {"ok": False, "error": f"Extraction failed: {e}"}); return

        self._json(200, {
            "ok":             True,
            "params":         params,
            "credits_after":  credits_after,
        })

    def _ai_get_credits(self):
        """POST /api/ai/credits â Get current credit balance"""
        payload = self._require_auth()
        if not payload: return
        gmail = payload.get('sub', '')
        conn  = get_db()
        conn.execute("INSERT OR IGNORE INTO ai_wallets (gmail, credits) VALUES (?, 10)", (gmail,))
        row = conn.execute("SELECT credits, total_purchased, total_used FROM ai_wallets WHERE gmail=?", (gmail,)).fetchone()
        conn.commit(); conn.close()
        self._json(200, {
            "ok": True,
            "credits":         row['credits']         if row else 10,
            "total_purchased": row['total_purchased']  if row else 0,
            "total_used":      row['total_used']        if row else 0,
        })

    def _ai_transactions(self):
        """POST /api/ai/transactions â Get recent credit transactions"""
        payload = self._require_auth()
        if not payload: return
        gmail = payload.get('sub', '')
        conn  = get_db()
        rows  = conn.execute(
            "SELECT type, amount, balance_after, description, created_at FROM ai_transactions WHERE gmail=? ORDER BY created_at DESC LIMIT 30",
            (gmail,)
        ).fetchall()
        conn.close()
        self._json(200, {"ok": True, "transactions": [dict(r) for r in rows]})

    def _ai_topup_request(self):
        """POST /api/ai/topup â Submit a top-up request with receipt"""
        payload = self._require_auth()
        if not payload: return
        gmail = payload.get('sub', '')
        body  = self._body()
        credits_req = int(body.get("credits", 0))
        receipt_b64 = body.get("receipt_base64", "")
        note        = body.get("note", "")
        if credits_req <= 0:
            self._json(400, {"ok": False, "error": "Invalid credits amount"}); return
        conn = get_db()
        rid  = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO ai_topup_requests (id, gmail, credits_requested, receipt_base64, note, status) VALUES (?,?,?,?,?,?)",
            (rid, gmail, credits_req, receipt_b64[:500000], note, 'pending')
        )
        conn.commit(); conn.close()
        self._json(200, {"ok": True, "request_id": rid, "message": "Top-up request submitted. Credits will be added after review."})

    def _admin_add_credits(self):
        """POST /api/admin/ai/credits/add â Admin: manually add credits to user"""
        if not self._require_admin(): return
        body   = self._body()
        gmail  = body.get("gmail", "").strip().lower()
        amount = int(body.get("amount", 0))
        note   = body.get("note", "Admin credit grant")
        if not gmail or amount <= 0:
            self._json(400, {"ok": False, "error": "gmail and amount required"}); return
        conn = get_db()
        conn.execute("INSERT OR IGNORE INTO ai_wallets (gmail, credits) VALUES (?, 0)", (gmail,))
        conn.execute("UPDATE ai_wallets SET credits = credits + ?, total_purchased = total_purchased + ? WHERE gmail=?", (amount, amount, gmail))
        row = conn.execute("SELECT credits FROM ai_wallets WHERE gmail=?", (gmail,)).fetchone()
        balance = row['credits'] if row else amount
        conn.execute(
            "INSERT INTO ai_transactions (gmail, type, amount, balance_after, description) VALUES (?,?,?,?,?)",
            (gmail, 'admin', amount, balance, note)
        )
        conn.commit(); conn.close()
        self._json(200, {"ok": True, "gmail": gmail, "credits_added": amount, "new_balance": balance})


    def _admin_ai_wallets(self):
        """GET /api/admin/ai/wallets — Admin: list all user AI wallets"""
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute(
            """SELECT w.gmail, w.credits, w.total_purchased, w.total_used, w.created_at,
                      l.name
               FROM ai_wallets w
               LEFT JOIN licenses l ON l.gmail = w.gmail
               ORDER BY w.credits DESC"""
        ).fetchall()
        conn.close()
        self._json(200, {"ok": True, "wallets": [dict(r) for r in rows]})

    def _admin_ai_topups(self):
        """GET /api/admin/ai/topups — Admin: list topup requests"""
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute(
            """SELECT id, gmail, credits_requested, note, status, created_at
               FROM ai_topup_requests ORDER BY created_at DESC LIMIT 100"""
        ).fetchall()
        conn.close()
        self._json(200, {"ok": True, "topups": [dict(r) for r in rows]})

    def _admin_approve_topup(self):
        """POST /api/admin/ai/topups/approve — Admin: approve a topup"""
        if not self._require_admin(): return
        body   = self._body()
        req_id = body.get("id", "").strip()
        note   = body.get("note", "Approved topup")
        if not req_id:
            self._json(400, {"ok": False, "error": "id required"}); return
        conn = get_db()
        row = conn.execute(
            "SELECT gmail, credits_requested, status FROM ai_topup_requests WHERE id=?", (req_id,)
        ).fetchone()
        if not row:
            conn.close(); self._json(404, {"ok": False, "error": "Request not found"}); return
        if row['status'] != 'pending':
            conn.close(); self._json(400, {"ok": False, "error": "Request already " + row['status']}); return
        gmail  = row['gmail']
        amount = row['credits_requested']
        conn.execute("INSERT OR IGNORE INTO ai_wallets (gmail, credits) VALUES (?, 0)", (gmail,))
        conn.execute("UPDATE ai_wallets SET credits = credits + ?, total_purchased = total_purchased + ? WHERE gmail=?", (amount, amount, gmail))
        balance = conn.execute("SELECT credits FROM ai_wallets WHERE gmail=?", (gmail,)).fetchone()['credits']
        conn.execute("INSERT INTO ai_transactions (gmail, type, amount, balance_after, description) VALUES (?,?,?,?,?)", (gmail, 'topup', amount, balance, note))
        conn.execute("UPDATE ai_topup_requests SET status='approved' WHERE id=?", (req_id,))
        conn.commit(); conn.close()
        self._json(200, {"ok": True, "gmail": gmail, "credits_added": amount, "new_balance": balance})

    # ═══════════════════════════════════════════════════════════════════════════
    # SOCIAL / COMMUNITY  handlers  (Supabase-backed)
    # ═══════════════════════════════════════════════════════════════════════════

    def _social_config(self):
        """GET /api/social/config — return public Supabase config for browser-side client."""
        self._json(200, {
            "ok":       True,
            "url":      SUPABASE_URL,
            "anon_key": SUPABASE_ANON_KEY,
        })

    # ── Feed ────────────────────────────────────────────────────────────────────

    def _social_notifications_get(self):
        """GET /api/social/notifications — unread notifications for caller."""
        payload = self._require_auth()
        if not payload:
            return
        gmail = payload.get('sub', '')
        try:
            notifs = _supa('notifications', params={
                'user_id': f'eq.{gmail}',
                'read':    'eq.false',
                'order':   'created_at.desc',
                'limit':   '50',
            })
            self._json(200, {"ok": True, "notifications": notifs})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def _social_feed_get(self):
        """GET /api/social/feed — paginated community posts."""
        payload = self._require_auth()
        if not payload:
            return
        from urllib.parse import urlparse, parse_qs
        qs     = parse_qs(urlparse(self.path).query)
        limit  = int(qs.get('limit',  ['20'])[0])
        offset = int(qs.get('offset', ['0'])[0])
        try:
            posts = _supa('community_posts', params={
                'select': 'id,author_id,content,image_url,likes_count,created_at,profiles(display_name,avatar_url)',
                'order':  'created_at.desc',
                'limit':  str(limit),
                'offset': str(offset),
            })
            self._json(200, {"ok": True, "posts": posts})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def _social_post_create(self):
        """POST /api/social/feed/post — create a new community post."""
        payload = self._require_auth()
        if not payload:
            return
        gmail   = payload.get('sub', '')
        _ensure_profile(gmail)
        body    = self._body()
        content   = body.get('content', '').strip()
        image_url = body.get('image_url', '')
        org_id    = body.get('org_id', '')
        if not content:
            self._json(400, {"ok": False, "error": "content required"})
            return
        try:
            row  = _supa('community_posts', method='POST',
                         body={'author_id': gmail, 'org_id': org_id,
                               'content': content, 'image_url': image_url},
                         use_service_key=True)
            post = row[0] if isinstance(row, list) and row else row
            self._json(200, {"ok": True, "post": post})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def _social_post_like(self):
        """POST /api/social/feed/like — toggle like on a post."""
        payload = self._require_auth()
        if not payload:
            return
        gmail   = payload.get('sub', '')
        body    = self._body()
        post_id = body.get('post_id', '')
        if not post_id:
            self._json(400, {"ok": False, "error": "post_id required"})
            return
        try:
            existing = _supa('post_likes',
                             params={'post_id': f'eq.{post_id}', 'user_id': f'eq.{gmail}'})
            if existing:
                _supa('post_likes', method='DELETE',
                      params={'post_id': f'eq.{post_id}', 'user_id': f'eq.{gmail}'},
                      use_service_key=True)
                liked = False
            else:
                _supa('post_likes', method='POST',
                      body={'post_id': post_id, 'user_id': gmail},
                      use_service_key=True)
                liked = True
            post  = _supa('community_posts',
                          params={'id': f'eq.{post_id}', 'select': 'id,likes_count'})
            count = post[0]['likes_count'] if post else 0
            self._json(200, {"ok": True, "liked": liked, "likes_count": count})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    # ── Friends ─────────────────────────────────────────────────────────────────

    def _social_friends_list(self):
        """POST /api/social/friends — list sent + received friendship rows."""
        payload = self._require_auth()
        if not payload:
            return
        gmail = payload.get('sub', '')
        try:
            sent     = _supa('friendships',
                             params={'requester_id': f'eq.{gmail}',
                                     'select': 'id,addressee_id,status,created_at'})
            received = _supa('friendships',
                             params={'addressee_id': f'eq.{gmail}',
                                     'select': 'id,requester_id,status,created_at'})
            self._json(200, {"ok": True, "sent": sent, "received": received})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def _social_friend_action(self):
        """POST /api/social/friends/add — request / accept / reject / remove."""
        payload = self._require_auth()
        if not payload:
            return
        gmail  = payload.get('sub', '')
        body   = self._body()
        action = body.get('action', 'request')   # 'request'|'accept'|'reject'|'remove'
        target = body.get('target_id', '')
        if not target:
            self._json(400, {"ok": False, "error": "target_id required"})
            return
        try:
            if action == 'request':
                _supa('friendships', method='POST',
                      body={'requester_id': gmail, 'addressee_id': target, 'status': 'pending'},
                      use_service_key=True,
                      extra_headers={'Prefer': 'return=representation,resolution=ignore-duplicates'})
                _supa('notifications', method='POST',
                      body={'user_id': target, 'type': 'friend_request',
                            'content': f'{gmail} sent you a friend request',
                            'related_id': gmail, 'read': False},
                      use_service_key=True)
                self._json(200, {"ok": True, "action": "requested"})
            elif action == 'accept':
                _supa('friendships', method='PATCH',
                      params={'requester_id': f'eq.{target}', 'addressee_id': f'eq.{gmail}'},
                      body={'status': 'accepted'},
                      use_service_key=True)
                _supa('notifications', method='POST',
                      body={'user_id': target, 'type': 'friend_accepted',
                            'content': f'{gmail} accepted your friend request',
                            'related_id': gmail, 'read': False},
                      use_service_key=True)
                self._json(200, {"ok": True, "action": "accepted"})
            elif action in ('reject', 'remove'):
                # Delete whichever direction the row exists
                _supa('friendships', method='DELETE',
                      params={'requester_id': f'eq.{gmail}', 'addressee_id': f'eq.{target}'},
                      use_service_key=True)
                _supa('friendships', method='DELETE',
                      params={'requester_id': f'eq.{target}', 'addressee_id': f'eq.{gmail}'},
                      use_service_key=True)
                self._json(200, {"ok": True, "action": action})
            else:
                self._json(400, {"ok": False, "error": f"Unknown action: {action}"})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    # ── Chat ────────────────────────────────────────────────────────────────────

    def _social_chat_rooms(self):
        """POST /api/social/chat/rooms — chat rooms the caller belongs to."""
        payload = self._require_auth()
        if not payload:
            return
        gmail = payload.get('sub', '')
        try:
            memberships = _supa('chat_room_members', params={
                'user_id': f'eq.{gmail}',
                'select':  'room_id,chat_rooms(id,name,type,created_by,created_at)',
            })
            rooms = [m.get('chat_rooms') for m in memberships if m.get('chat_rooms')]
            self._json(200, {"ok": True, "rooms": rooms})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def _social_chat_send(self):
        """POST /api/social/chat/send — send a message to a chat room."""
        payload = self._require_auth()
        if not payload:
            return
        gmail    = payload.get('sub', '')
        body     = self._body()
        room_id  = body.get('room_id', '')
        content  = body.get('content', '').strip()
        msg_type = body.get('message_type', 'text')
        if not room_id or not content:
            self._json(400, {"ok": False, "error": "room_id and content required"})
            return
        try:
            membership = _supa('chat_room_members',
                               params={'room_id': f'eq.{room_id}', 'user_id': f'eq.{gmail}'})
            if not membership:
                self._json(403, {"ok": False, "error": "Not a member of this room"})
                return
            row = _supa('messages', method='POST',
                        body={'room_id': room_id, 'sender_id': gmail,
                              'content': content, 'message_type': msg_type},
                        use_service_key=True)
            msg = row[0] if isinstance(row, list) and row else row
            self._json(200, {"ok": True, "message": msg})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    # ── Org members ─────────────────────────────────────────────────────────────

    def _social_org_members(self):
        """POST /api/social/org/members — list all members in the caller's org."""
        payload = self._require_auth()
        if not payload:
            return
        gmail  = payload.get('sub', '')
        body   = self._body()
        org_id = body.get('org_id', '')
        try:
            if not org_id:
                me     = _supa('profiles', params={'id': f'eq.{gmail}', 'select': 'org_id'})
                org_id = me[0].get('org_id', '') if me else ''
            if not org_id:
                self._json(200, {"ok": True, "members": []})
                return
            members = _supa('profiles', params={
                'org_id': f'eq.{org_id}',
                'select': 'id,display_name,avatar_url,status,org_id',
            })
            self._json(200, {"ok": True, "members": members})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    # ── Notifications ────────────────────────────────────────────────────────────

    def _social_notif_read(self):
        """POST /api/social/notifications/read — mark notifications read."""
        payload = self._require_auth()
        if not payload:
            return
        gmail     = payload.get('sub', '')
        body      = self._body()
        notif_ids = body.get('ids', [])   # empty list → mark ALL unread
        try:
            if notif_ids:
                for nid in notif_ids:
                    _supa('notifications', method='PATCH',
                          params={'id': f'eq.{nid}', 'user_id': f'eq.{gmail}'},
                          body={'read': True}, use_service_key=True)
            else:
                _supa('notifications', method='PATCH',
                      params={'user_id': f'eq.{gmail}', 'read': 'eq.false'},
                      body={'read': True}, use_service_key=True)
            self._json(200, {"ok": True})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    # ── Project Media Hub ────────────────────────────────────────────────────────

    def _social_media_save(self):
        """POST /api/social/media/save — save a generated render to project media."""
        payload = self._require_auth()
        if not payload:
            return
        gmail = payload.get('sub', '')
        body  = self._body()
        media_url   = body.get('media_url', '')
        title       = body.get('title', 'Untitled')
        description = body.get('description', '')
        media_type  = body.get('media_type', 'image')
        tags        = body.get('tags', [])
        org_id      = body.get('org_id', '')
        if not media_url:
            self._json(400, {"ok": False, "error": "media_url required"})
            return
        try:
            row  = _supa('project_media', method='POST',
                         body={'user_id': gmail, 'org_id': org_id, 'title': title,
                               'description': description, 'media_url': media_url,
                               'media_type': media_type, 'tags': tags},
                         use_service_key=True)
            item = row[0] if isinstance(row, list) and row else row
            self._json(200, {"ok": True, "media": item})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})

    def _social_media_list(self):
        """POST /api/social/media/list — list project media for user or org."""
        payload = self._require_auth()
        if not payload:
            return
        gmail = payload.get('sub', '')
        body  = self._body()
        org_id     = body.get('org_id', '')
        media_type = body.get('media_type', '')
        limit      = int(body.get('limit', 40))
        offset     = int(body.get('offset', 0))
        try:
            params = {
                'select': 'id,user_id,title,description,media_url,media_type,tags,created_at',
                'order':  'created_at.desc',
                'limit':  str(limit),
                'offset': str(offset),
            }
            if org_id:
                params['org_id']  = f'eq.{org_id}'
            else:
                params['user_id'] = f'eq.{gmail}'
            if media_type:
                params['media_type'] = f'eq.{media_type}'
            items = _supa('project_media', params=params)
            self._json(200, {"ok": True, "items": items, "count": len(items)})
        except Exception as e:
            self._json(500, {"ok": False, "error": str(e)})


if __name__ == "__main__":
    init_db()
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[AbimconStudio V3] Server v3 running â http://0.0.0.0:{PORT}")
    print(f"[AbimconStudio V3] Admin panel       â http://localhost:{PORT}")
    print(f"[AbimconStudio V3] Admin password    â {ADMIN_PASS}")
    print(f"[AbimconStudio V3] R2 configured     â {'YES' if all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]) else 'NO (set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME)'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[AbimconStudio V3] Stopped.")
