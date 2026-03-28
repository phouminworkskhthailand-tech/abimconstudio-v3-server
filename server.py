#!/usr/bin/env python3
"""
AbimconStudio V3 — License Management Server  v3
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

# — boto3 for Cloudflare R2 signed URLs —
try:
      import boto3
      from botocore.config import Config as BotoConfig
      HAS_BOTO3 = True
except ImportError:
      HAS_BOTO3 = False
      print("[WARN] boto3 not installed — /api/download-model will be disabled. Run: pip install boto3")

# — Config —
PORT       = int(os.environ.get("PORT", 8080))
ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "abimcon_admin_2026")
SECRET_KEY = os.environ.get("SECRET_KEY",     "abimcon_secret_key_v3_change_me")
DB_PATH    = os.environ.get("DB_PATH",         "/tmp/abimcon_v3.db")

# Cloudflare R2 credentials (set these in Railway environment variables)
R2_ACCOUNT_ID        = os.environ.get("R2_ACCOUNT_ID",        "")
R2_ACCESS_KEY_ID     = os.environ.get("R2_ACCESS_KEY_ID",     "")
R2_SECRET_ACCESS_KEY = os.environ.get("R2_SECRET_ACCESS_KEY", "")
R2_BUCKET_NAME       = os.environ.get("R2_BUCKET_NAME",       "abimcon-models")
R2_SIGNED_URL_TTL    = int(os.environ.get("R2_SIGNED_URL_TTL", 300))  # seconds (default 5 min)

# Plan defaults
PLAN_DAILY_LIMITS = {"free": 5, "pro": 100}

# — Database —
def get_db():
      conn = sqlite3.connect(DB_PATH)
      conn.row_factory = sqlite3.Row
      conn.execute("PRAGMA foreign_keys = ON")
      return conn

def init_db():
      conn = get_db()
      c = conn.cursor()

    # — Licenses —
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

    # — Registered HWIDs —
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

    # — Models —
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

    # — Download Logs (for daily limit tracking) —
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

    # — Activity log —
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

    # — Idempotent migrations for existing DBs —
    migrations = [
              "ALTER TABLE licenses ADD COLUMN max_devices         INTEGER NOT NULL DEFAULT 2",
              "ALTER TABLE licenses ADD COLUMN plan_type           TEXT    NOT NULL DEFAULT 'free'",
              "ALTER TABLE licenses ADD COLUMN expiry_date         TEXT",
              "ALTER TABLE licenses ADD COLUMN daily_download_limit INTEGER",
              "ALTER TABLE activity_log ADD COLUMN hwid TEXT",
              # v3.1 — new model columns for AssetBrowser UI
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

    # — Seed licenses —
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

          conn.commit()
    conn.close()
    print(f"[DB] Initialised at {DB_PATH}")

# — Cloudflare R2 signed URL —
def generate_r2_signed_url(r2_path: str, ttl: int = R2_SIGNED_URL_TTL) -> str | None:
      if not HAS_BOTO3:
                return None
            if not all([R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY]):
                      print("[R2] Missing credentials — set R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY")
                      return None
                  try:
                            s3 = boto3.client(
                                          "s3",
                                          endpoint_url        = f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
                                          aws_access_key_id   = R2_ACCESS_KEY_ID,
                                          aws_secret_access_key = R2_SECRET_ACCESS_KEY,
                                          config              = BotoConfig(signature_version="s3v4"),
                                          region_name         = "auto",
                            )
                            url = s3.generate_presigned_url(
                                "get_object",
                                Params  = {"Bucket": R2_BUCKET_NAME, "Key": r2_path},
                                ExpiresIn = ttl,
                            )
                            return url
except Exception as e:
        print(f"[R2] Signed URL error: {e}")
        return None

# — JWT helpers —
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

# — Admin Panel HTML —
_admin_path = os.path.join(os.path.dirname(__file__), "admin.html")
ADMIN_HTML  = open(_admin_path).read() if os.path.exists(_admin_path) \
              else "<h1>Admin panel not found</h1>"

# — HTTP Handler —
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

    # — CORS preflight —
    def do_OPTIONS(self):
              self.send_response(204)
              self.send_header("Access-Control-Allow-Origin",  "*")
              self.send_header("Access-Control-Allow-Headers", "Authorization,Content-Type")
              self.send_header("Access-Control-Allow-Methods", "GET,POST,PUT,DELETE,OPTIONS")
              self.end_headers()

    # — GET —
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
elif path == "/api/assets":               self._get_assets()   # AssetBrowser JS endpoint
else:
            m_hw  = re.match(r"^/api/admin/licenses/([^/]+)/hwids$",      path)
              m_dl  = re.match(r"^/api/admin/licenses/([^/]+)/downloads$",   path)
            m_mod = re.match(r"^/api/admin/models/([^/]+)$",               path)
            if   m_hw:  self._get_hwids(m_hw.group(1))
elif m_dl:  self._get_download_stats(m_dl.group(1))
else:       self._json(404, {"error": "Not found"})

    # — POST —
    def do_POST(self):
              path = urlparse(self.path).path.rstrip("/")

        if   path == "/api/admin/login":      self._admin_login()
elif path == "/api/validate":          self._validate_license()
elif path == "/api/download-model":    self._download_model()
elif path == "/api/download":          self._download()        # AssetBrowser JS endpoint
elif path == "/api/admin/licenses":    self._add_license()
elif path == "/api/admin/models":      self._add_model()
else: self._json(404, {"error": "Not found"})

    # — PUT —
    def do_PUT(self):
              path = urlparse(self.path).path.rstrip("/")
              m = re.match(r"^/api/admin/licenses/([^/]+)$", path)
              if m: self._update_license(m.group(1))
else:
            m2 = re.match(r"^/api/admin/models/([^/]+)$", path)
            if m2: self._update_model(m2.group(1))
else: self._json(404, {"error": "Not found"})

    # — DELETE —
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

    # ════════════════════════════════════════════════════════════════════════════
    # — Auth Handlers —
    # ════════════════════════════════════════════════════════════════════════════

    def _admin_login(self):
              body = self._body()
              if body.get("password", "") != ADMIN_PASS:
                            self._log_activity("admin", "admin_login", Fals
