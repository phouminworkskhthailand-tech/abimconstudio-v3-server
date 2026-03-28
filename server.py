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

import os, sys, json, sqlite3, hmac, hashlib, base64, time, re, uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta

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
DB_PATH    = os.environ.get("DB_PATH",         "/tmp/abimcon_v3.db")

# Cloudflare R2 credentials (set these in Railway environment variables)
R2_ACCOUNT_ID        = os.environ.get("R2_ACCOUNT_ID",        "")
R2_ACCESS_KEY_ID     = os.environ.get("R2_ACCESS_KEY_ID",     "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME       = os.environ.get("R2_BUCKET_NAME",       "abimcon-models")   # bucket for .skp files
R2_ASSETS_BUCKET     = os.environ.get("R2_ASSETS_BUCKET",     "abimcon-assets")   # bucket for thumbnails/images
R2_SIGNED_URL_TTL    = int(os.environ.get("R2_SIGNED_URL_TTL", 300))  # seconds (default 5 min)

# Plan defaults
PLAN_DAILY_LIMITS = {"free": 5, "pro": 100}

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
        elif path == "/api/admin/verify":
            p = self._require_admin()
            if p: self._json(200, {"ok": True})
        elif path == "/api/admin/licenses":       self._get_licenses()
        elif path == "/api/admin/logs":           self._get_logs()
        elif path == "/api/admin/models":         self._get_models()
        elif path == "/api/models":               self._get_user_models()
        elif path == "/api/assets":               self._get_assets()   # â AssetBrowser JS endpoint
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

        if   path == "/api/admin/login":      self._admin_login()
        elif path == "/api/validate":          self._validate_license()
        elif path == "/api/download-model":    self._download_model()
        elif path == "/api/download":          self._download()        # â AssetBrowser JS endpoint
        elif path == "/api/admin/licenses":    self._add_license()
        elif path == "/api/admin/models":      self._add_model()
        else: self._json(404, {"error": "Not found"})

    # ââ PUT âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_PUT(self):
        path = urlparse(self.path).path.rstrip("/")
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

        conn.execute("UPDATE licenses SET last_login=? WHERE id=?", (now_ts, row["id"]))
        conn.commit(); conn.close()
        self._log_activity(gmail, "sketchup_login", True, hwid)

        session_token = make_token(
            {"sub": gmail, "role": row["role"], "lid": row["id"], "plan": row["plan_type"]},
            expires_in=28800
        )
        self._json(200, {
            "ok":    True,
            "name":  row["name"],
            "role":  row["role"],
            "plan":  row["plan_type"],
            "token": session_token,
        })

    # ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    # ââ Secure Download Endpoint ââââââââââââââââââââââââââââââââââââââââââââââ
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

        # ââ Get model ââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        model = conn.execute("SELECT * FROM models WHERE model_id=?", (model_id,)).fetchone()
        if not model:
            conn.close()
            self._json(404, {"ok": False, "error": "Model not found."}); return

        # ââ Premium model check ââââââââââââââââââââââââââââââââââââââââââââââ
        if model["is_premium"] and lic["plan_type"] == "free":
            conn.close()
            self._json(403, {
                "ok":    False,
                "error": "This model requires a Pro plan. Please upgrade.",
                "code":  "PLAN_REQUIRED"
            }); return

        # ââ Generate signed URL ââââââââââââââââââââââââââââââââââââââââââââââ
        signed_url = generate_r2_signed_url(model["r2_path"])
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
            "filename":   model["name"] + ".skp",
            "expires_in": R2_SIGNED_URL_TTL,
            "model_name": model["name"],
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

    def _delete_license(self, lid):
        if not self._require_admin(): return
        conn = get_db()
        conn.execute("DELETE FROM registered_hwids WHERE license_id=?", (lid,))
        conn.execute("DELETE FROM download_logs     WHERE license_id=?", (lid,))
        conn.execute("DELETE FROM licenses          WHERE id=?",         (lid,))
        conn.commit(); conn.close()
        self._json(200, {"ok": True})

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

        # ââ HWID: register new devices, enforce device limit ââââââââââââââââ
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
        payload = self._require_auth()
        if not payload: return
        plan = payload.get("plan", "free")
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
        models_out = []
        for r in rows:
            d = dict(r)
            d["thumbnail"] = _sign_thumbnail(d.get("thumbnail", ""))
            models_out.append(d)
        self._json(200, {"ok": True, "plan": plan, "models": models_out})

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
        for col in ("name", "r2_path", "description"):
            if col in body:
                fields.append(f"{col}=?"); vals.append(body[col].strip())
        if "is_premium" in body:
            fields.append("is_premium=?"); vals.append(1 if body["is_premium"] else 0)
        if "file_size" in body:
            fields.append("file_size=?"); vals.append(int(body["file_size"]))
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
