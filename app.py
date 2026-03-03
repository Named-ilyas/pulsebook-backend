"""
PulseBook — Backend API Server v2
Runs on Railway.app (free tier)
New in v2: auth (login/register), comments, added_by tracking
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3, os, time, hashlib

app = Flask(__name__)
CORS(app)

DB = os.path.join(os.path.dirname(__file__), "pulsebook.db")

# ── Database ──────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = sqlite3.connect(DB)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ecgs (
            id          TEXT PRIMARY KEY,
            category    TEXT,
            subcategory TEXT,
            difficulty  TEXT,
            site        TEXT,
            source_url  TEXT,
            image_url   TEXT,
            rate        TEXT,
            rhythm      TEXT,
            axis        TEXT,
            intervals   TEXT,
            summary     TEXT,
            features    TEXT,
            pearls      TEXT,
            scraped_at  TEXT,
            added_by    TEXT
        );
        CREATE TABLE IF NOT EXISTS users (
            id           TEXT PRIMARY KEY,
            username     TEXT UNIQUE,
            password_hash TEXT,
            created_at   TEXT
        );
        CREATE TABLE IF NOT EXISTS progress (
            user_id     TEXT,
            ecg_id      TEXT,
            studied_at  TEXT,
            PRIMARY KEY (user_id, ecg_id)
        );
        CREATE TABLE IF NOT EXISTS comments (
            id          TEXT PRIMARY KEY,
            ecg_id      TEXT,
            user_id     TEXT,
            username    TEXT,
            text        TEXT,
            created_at  TEXT
        );
        -- Migrate: add added_by if column doesn't exist (safe to run on existing DB)
        CREATE INDEX IF NOT EXISTS idx_comments_ecg ON comments(ecg_id);
    """)
    # Safe migration: add columns if they don't exist
    try:
        conn.execute("ALTER TABLE ecgs ADD COLUMN added_by TEXT")
        conn.commit()
    except Exception:
        pass  # Column already exists
    try:
        conn.execute("ALTER TABLE users ADD COLUMN username TEXT")
        conn.commit()
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
        conn.commit()
    except Exception:
        pass
    conn.commit()
    conn.close()

init_db()

def hash_pw(password):
    return hashlib.sha256(password.encode()).hexdigest()

# ── Auth routes ───────────────────────────────────
@app.route("/api/register", methods=["POST"])
def register():
    data     = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if len(username) < 2:
        return jsonify({"error": "Username too short"}), 400
    user_id = hashlib.md5(username.lower().encode()).hexdigest()[:12]
    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
    if existing:
        conn.close()
        return jsonify({"error": "Username already taken"}), 409
    conn.execute(
        "INSERT INTO users (id, username, password_hash, created_at) VALUES (?,?,?,?)",
        (user_id, username, hash_pw(password), time.strftime("%Y-%m-%dT%H:%M:%S"))
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "user_id": user_id, "username": username})

@app.route("/api/login", methods=["POST"])
def login():
    data     = request.json or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    conn = get_db()
    row  = conn.execute(
        "SELECT id, username FROM users WHERE username=? AND password_hash=?",
        (username, hash_pw(password))
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Invalid username or password"}), 401
    return jsonify({"ok": True, "user_id": row["id"], "username": row["username"]})

# ── ECG routes ────────────────────────────────────
@app.route("/api/ecgs")
def get_ecgs():
    cat    = request.args.get("category")
    diff   = request.args.get("difficulty")
    q      = request.args.get("search")
    conn   = get_db()
    sql    = "SELECT * FROM ecgs WHERE 1=1"
    params = []
    if cat:  sql += " AND category=?";  params.append(cat)
    if diff: sql += " AND difficulty=?"; params.append(diff)
    if q:
        sql += " AND (subcategory LIKE ? OR summary LIKE ?)"
        params += [f"%{q}%", f"%{q}%"]
    sql += " ORDER BY scraped_at DESC"
    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/ecgs/<ecg_id>")
def get_ecg(ecg_id):
    conn = get_db()
    row  = conn.execute("SELECT * FROM ecgs WHERE id=?", (ecg_id,)).fetchone()
    conn.close()
    if not row: return jsonify({"error": "Not found"}), 404
    return jsonify(dict(row))

@app.route("/api/categories")
def get_categories():
    conn = get_db()
    rows = conn.execute(
        "SELECT category, COUNT(*) as n FROM ecgs GROUP BY category ORDER BY n DESC"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/stats")
def get_stats():
    conn  = get_db()
    total = conn.execute("SELECT COUNT(*) FROM ecgs").fetchone()[0]
    conn.close()
    return jsonify({"total": total, "version": "2.0.0", "status": "ok"})

# ── Progress routes ───────────────────────────────
@app.route("/api/progress/<user_id>", methods=["GET"])
def get_progress(user_id):
    conn = get_db()
    rows = conn.execute("SELECT ecg_id FROM progress WHERE user_id=?", (user_id,)).fetchall()
    conn.close()
    return jsonify([r["ecg_id"] for r in rows])

@app.route("/api/progress", methods=["POST"])
def save_progress():
    data    = request.json or {}
    user_id = data.get("user_id")
    ecg_id  = data.get("ecg_id")
    if not user_id or not ecg_id:
        return jsonify({"error": "Missing fields"}), 400
    conn = get_db()
    conn.execute(
        "INSERT OR IGNORE INTO progress (user_id, ecg_id, studied_at) VALUES (?,?,?)",
        (user_id, ecg_id, time.strftime("%Y-%m-%dT%H:%M:%S"))
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

# ── Leaderboard ───────────────────────────────────
@app.route("/api/leaderboard")
def get_leaderboard():
    conn = get_db()
    rows = conn.execute("""
        SELECT u.username as name, COUNT(p.ecg_id) as score
        FROM users u
        LEFT JOIN progress p ON u.id = p.user_id
        WHERE u.username IS NOT NULL
        GROUP BY u.id ORDER BY score DESC LIMIT 20
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

# ── Comments routes ───────────────────────────────
@app.route("/api/comments/<ecg_id>", methods=["GET"])
def get_comments(ecg_id):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM comments WHERE ecg_id=? ORDER BY created_at DESC",
        (ecg_id,)
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/comments", methods=["POST"])
def post_comment():
    data = request.json or {}
    ecg_id   = data.get("ecg_id")
    user_id  = data.get("user_id")
    username = data.get("username")
    text     = (data.get("text") or "").strip()
    if not ecg_id or not user_id or not text:
        return jsonify({"error": "Missing fields"}), 400
    comment_id = hashlib.md5(f"{user_id}{ecg_id}{time.time()}".encode()).hexdigest()[:16]
    conn = get_db()
    conn.execute(
        "INSERT INTO comments (id, ecg_id, user_id, username, text, created_at) VALUES (?,?,?,?,?,?)",
        (comment_id, ecg_id, user_id, username, text, time.strftime("%Y-%m-%dT%H:%M:%S"))
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "id": comment_id})

# ── ECG save/scrape ───────────────────────────────
@app.route("/api/scrape", methods=["POST"])
def receive_ecg():
    ecg = request.json
    if not ecg or not ecg.get("id"):
        return jsonify({"error": "Invalid data"}), 400
    conn = get_db()
    conn.execute("""
        INSERT OR REPLACE INTO ecgs
        (id,category,subcategory,difficulty,site,source_url,image_url,
         rate,rhythm,axis,intervals,summary,features,pearls,scraped_at,added_by)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ecg.get("id"),           ecg.get("category"),
        ecg.get("subcategory"),  ecg.get("difficulty","intermediate"),
        ecg.get("site",""),      ecg.get("source_url",""),
        ecg.get("image_url",""), ecg.get("rate",""),
        ecg.get("rhythm",""),    ecg.get("axis",""),
        ecg.get("intervals",""), ecg.get("summary",""),
        ecg.get("features",""),  ecg.get("pearls",""),
        time.strftime("%Y-%m-%dT%H:%M:%S"),
        ecg.get("added_by","")
    ))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/ecgs/all", methods=["DELETE"])
def delete_all_ecgs():
    conn = get_db()
    conn.execute("DELETE FROM ecgs")
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/")
def health():
    return jsonify({"status": "PulseBook API running", "version": "2.0.0"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
