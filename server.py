#!/usr/bin/env python3
"""
AbimconStudio V3 â License Management Server  v2
Pure Python stdlib only. No pip required.
Runs on Railway / Render / any VPS.

New in v2:
  - HWID tracking + device-limit enforcement
  - Real public IP via x-forwarded-for (Railway proxy)
  - max_devices per license
  - registered_hwids table
  - Reset-devices endpoint: DELETE /api/admin/licenses/{id}/hwids
  - Session token returned on successful SketchUp login
"""

import os, sys, json, sqlite3, hmac, hashlib, base64, time, re, uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# ââ Config âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
PORT       = int(os.environ.get("PORT", 8080))
ADMIN_PASS = os.environ.get("ADMIN_PASSWORD", "abimcon_admin_2026")
SECRET_KEY = os.environ.get("SECRET_KEY",    "abimcon_secret_key_v3_change_me")
DB_PATH    = os.environ.get("DB_PATH",        "/tmp/abimcon_v3.db")

# ââ Database âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    # ââ Licenses ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("""
        CREATE TABLE IF NOT EXISTS licenses (
            id          TEXT PRIMARY KEY,
            gmail       TEXT UNIQUE NOT NULL,
            license_key TEXT UNIQUE NOT NULL,
            name        TEXT NOT NULL,
            role        TEXT NOT NULL DEFAULT 'editor',
            active      INTEGER NOT NULL DEFAULT 1,
            max_devices INTEGER NOT NULL DEFAULT 2,
            last_login  TEXT,
            created_at  TEXT NOT NULL
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

    # ââ Migrate existing DBs (idempotent ALTER TABLE) âââââââââââââââââââââââââ
    for sql in [
        "ALTER TABLE licenses     ADD COLUMN max_devices INTEGER NOT NULL DEFAULT 2",
        "ALTER TABLE activity_log ADD COLUMN hwid TEXT",
    ]:
        try:
            c.execute(sql)
        except sqlite3.OperationalError:
            pass  # column already exists â fine

    # ââ Seed ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    c.execute("SELECT COUNT(*) FROM licenses")
    if c.fetchone()[0] == 0:
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        seeds = [
            ("abimcon.user01@gmail.com","ABIM-4K9L-MN2P-X001","Abimcon User 01","admin"),
            ("abimcon.user02@gmail.com","ABIM-7R3T-QW5X-X002","Abimcon User 02","editor"),
            ("abimcon.user03@gmail.com","ABIM-1ZBP-HV8C-X003","Abimcon User 03","editor"),
            ("abimcon.user04@gmail.com","ABIM-9NKW-DF4J-X004","Abimcon User 04","viewer"),
            ("abimcon.user05@gmail.com","ABIM-5MQE-TU6G-X005","Abimcon User 05","editor"),
            ("abimcon.user06@gmail.com","ABIM-2XVC-SB3H-X006","Abimcon User 06","viewer"),
            ("abimcon.user07@gmail.com","ABIM-8FGD-PW7Y-X007","Abimcon User 07","editor"),
            ("abimcon.user08@gmail.com","ABIM-3LRJ-CZ9M-X008","Abimcon User 08","viewer"),
            ("abimcon.user09@gmail.com","ABIM-6THN-AX1K-X009","Abimcon User 09","editor"),
            ("abimcon.user10@gmail.com","ABIM-0YAF-LK2R-X010","Abimcon User 10","admin"),
        ]
        for gmail, key, name, role in seeds:
            c.execute(
                "INSERT INTO licenses (id,gmail,license_key,name,role,active,max_devices,created_at) "
                "VALUES (?,?,?,?,?,1,2,?)",
                (str(uuid.uuid4()), gmail, key, name, role, now)
            )
    conn.commit()
    conn.close()
    print(f"[DB] Initialised at {DB_PATH}")

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

# ââ Admin Panel HTML âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
_admin_path = os.path.join(os.path.dirname(__file__), "admin.html")
ADMIN_HTML  = open(_admin_path).read() if os.path.exists(_admin_path) \
              else "<h1>Admin panel not found</h1>"

# ââ HTTP Handler âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
class Handler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"[HTTP] {self.address_string()} - {fmt % args}")

    # ââ Real public IP âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def _get_client_ip(self):
        """
        On Railway (and most cloud platforms) the real client IP is in the
        X-Forwarded-For header set by the platform's ingress proxy.
        Format: "client_ip, proxy1, proxy2" â leftmost = real client.
        """
        xff = self.headers.get("X-Forwarded-For", "").strip()
        if xff:
            return xff.split(",")[0].strip()
        return self.client_address[0]

    # ââ Response helpers âââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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

    def _log_activity(self, gmail, action, success, hwid=""):
        conn = get_db()
        now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.execute(
            "INSERT INTO activity_log (gmail,action,success,ip_addr,hwid,created_at) "
            "VALUES (?,?,?,?,?,?)",
            (gmail, action, 1 if success else 0, self._get_client_ip(), hwid or None, now)
        )
        conn.commit(); conn.close()

    # ââ CORS preflight âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin",  "*")
        self.send_header("Access-Control-Allow-Headers", "Authorization,Content-Type")
        self.send_header("Access-Control-Allow-Methods", "GET,PORT,PUT,DELETE,OPTIONS")
        self.end_headers()

    # ââ GET ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ  
    def do_GET(self):
        path = urlparse(self.path).path.rstrip("/") or "/"
        if path == "/":
            self._html(200, ADMIN_HTML)
        elif path == "/health":
            self._json(200, {"status": "ok"})
        elif path == "/api/admin/verify":
            p = self._require_admin()
            if p: self._json(200, {"ok": True})
        elif path == "/api/admin/licenses":
            self._get_licenses()
        elif path == "/api/admin/logs":
            self._get_logs()
        else:
            m = re.match(r"^/api/admin/licenses/([^/]+)/wids$", path)
            if m: self._get_hwids(m.group(1))
            else: self._json(404, {"error": "Not found"})

    # ââ POST âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_POST(self):
        path = urlparse(self.path).path.rstrip("/")
        if   path == "/api/admin/login":    self._admin_login()
        elif path == "/api/validate":        self._validate_license()
        elif path == "/api/admin/licenses":  self._add_license()
        else: self._json(404, {"error": "Not found"})

    # ââ PUT ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_PUT(self):
        path = urlparse(self.path).path.rstrip("/")
        m = re.match(r"^/api/admin/licenses/([^/]+)$", path)
        if m: self._update_license(m.group(1))
        else: self._json(404, {"error": "Not found"})

    # ââ DELETE âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
    def do_DELETE(self):
        path = urlparse(self.path).path.rstrip("/")
        m_hw = re.match(r"^/api/admin/licenses/([^/]+)/hwids$", path)
        if m_hw:
            self._reset_hwids(m_hw.group(1)); return
        m = re.match(r"^/api/admin/licenses/([^/]+)$", path)
        if m: self._delete_license(m.group(1))
        else: self._json(404, {"error": "Not found"})

    # ââ Handlers âââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
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

        max_dev    = row["max_devices"] if row["max_devices"] else 2
        client_ip  = self._get_client_ip()
        now_ts     = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

        # ââ HWID enforcement âââââââââââââââââââââââââââââââââââââââââââââââââââ
        if hwid:
            hw_rows    = conn.execute(
                "SELECT hwid FROM registered_hwids WHERE license_id=?", (row["id"],)
            ).fetchall()
            known      = [r["hwid"] for r in hw_rows]

            if hwid in known:
                # Known device â refresh last_seen + IP
                conn.execute(
                    "UPDATE registered_hwids SET last_seen=?, ip_addr=? "
                    "WHERE license_id=? AND hwid=?",
                    (now_ts, client_ip, row["id"], hwid)
                )
            else:
                # New device â check limit
                if len(known) >= max_dev:
                    conn.close()
                    self._log_activity(gmail, "hwid_blocked", False, hwid)
                    self._json(403, {
                        "ok":    False,
                        "error": f"Device limit reached ({max_dev} devices allowed). "
                                 "Ask your admin to reset your devices.",
                        "code":  "DEVICE_LIMIT"
                    }); return
                # Register new device
                conn.execute(
                    "INSERT INTO registered_hwids "
                    "(license_id, hwid, first_seen, last_seen, ip_addr) VALUES (?,?,?,?,?)",
                    (row["id"], hwid, now_ts, now_ts, client_ip)
                )

        # ââ Success ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
        conn.execute("UPDATE licenses SET last_login=? WHERE id=?", (now_ts, row["id"]))
        conn.commit(); conn.close()
        self._log_activity(gmail, "sketchup_login", True, hwid)

        # Short-lived session token for subsequent plugin API calls (8 hours)
        session_token = make_token(
            {"sub": gmail, "role": row["role"], "lid": row["id"]},
            expires_in=28800
        )
        self._json(200, {
            "ok":    True,
            "name":  row["name"],
            "role":  row["role"],
            "token": session_token,
        })

    def _get_licenses(self):
        if not self._require_admin(): return
        conn = get_db()
        rows = conn.execute("SELECT * FROM licenses ORDER BY created_at").fetchall()
        logs = conn.execute(
            "SELECT COUNT(*) as n FROM activity_log "
            "WHERE action='sketchup_login' AND success=1"
        ).fetchone()

        lics = []
        for r in rows:
            d = dict(r)
            d["device_count"] = conn.execute(
                "SELECT COUNT(*) as n FROM registered_hwids WHERE license_id=?", (d["id"],)
            ).fetchone()["n"]
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
        rows = conn.execute(
            "SELECT * FROM activity_log ORDER BY id DESC LIMIT 50"
        ).fetchall()
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

    def _reset_hwids(self, lid):
        if not self._require_admin(): return
        conn = get_db()
        count = conn.execute(
            "SELECT COUNT(*) as n FROM registered_hwids WHERE license_id=?", (lid,)
        ).fetchone()["n"]
        conn.execute("DELETE FROM registered_hwids WHERE license_id=?", (lid,))
        conn.commit(); conn.close()
        self._json(200, {"ok": True, "unbound": count})

    def _add_license(self):
        if not self._require_admin(): return
        body    = self._body()
        name    = body.get("name", "").strip()
        gmail   = body.get("gmail", "").strip().lower()
        key     = body.get("license_key", "").strip().upper()
        role    = body.get("role", "editor")
        max_dev = int(body.get("max_devices", 2))
        if not name or not gmail or not key:
            self._json(400, {"error": "name, gmail, license_key required"}); return
        now  = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO licenses (id,gmail,license_key,name,role,active,max_devices,created_at) "
                "VALUES (?,?,?,?,?,1,?,?)",
                (str(uuid.uuid4()), gmail, key, name, role, max_dev, now)
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
        for col in ("name", "gmail", "license_key", "role"):
            if col in body:
                fields.append(f"{col}=?")
                v = body[col].strip()
                if col == "gmail":       v = v.lower()
                if col == "license_key": v = v.upper()
                vals.append(v)
        if "active" in body:
            fields.append("active=?")
            vals.append(1 if body["active"] else 0)
        if "max_devices" in body:
            fields.append("max_devices=?")
            vals.append(max(1, int(body["max_devices"])))
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
        conn.execute("DELETE FROM licenses WHERE id=?", (lid,))
        conn.commit(); conn.close()
        self._json(200, {"ok": True})

if __name__ == "__main__":
    init_db()
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"[AbimconStudio V3] Server v2 running â http://0.0.0.0:{PORT}")
    print(f"[AbimconStudio V3] Admin panel       â http://localhost:{PORT}")
    print(f"[AbimconStudio V3] Admin password    â {ADMIN_PASS}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[AbimconStudio V3] Stopped.")
