"""
PulseBook — Backend API Server v2.1
Runs on Railway.app (free tier)
Fix v2.1: SQLite WAL mode + connection timeout to prevent locking crashes
         on multi-worker gunicorn deployments.
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3, os, time, hashlib, uuid

app = Flask(__name__)
CORS(app)

DB = os.path.join(os.path.dirname(__file__), "pulsebook.db")

# ── Database ──────────────────────────────────────
def get_db():
    """Open a thread-safe SQLite connection with WAL mode and 30s timeout."""
    conn = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    # WAL mode: allows concurrent reads + one write without locking
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    conn = get_db()
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
            id            TEXT PRIMARY KEY,
            username      TEXT UNIQUE,
            password_hash TEXT,
            created_at    TEXT
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
        CREATE INDEX IF NOT EXISTS idx_comments_ecg ON comments(ecg_id);
    """)
    # Safe migrations for existing DBs
    for sql in [
        "ALTER TABLE ecgs ADD COLUMN added_by TEXT",
        "ALTER TABLE users ADD COLUMN username TEXT",
        "ALTER TABLE users ADD COLUMN password_hash TEXT",
    ]:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass  # Column already exists
    conn.commit()
    conn.close()

init_db()

def hash_pw(password):
    return hashlib.sha256(password.encode()).hexdigest()

# ── Auth routes ───────────────────────────────────
@app.route("/api/register", methods=["POST"])
def register():
    try:
        data     = request.json or {}
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""
        if not username or not password:
            return jsonify({"error": "Username and password required"}), 400
        if len(username) < 2:
            return jsonify({"error": "Username too short (min 2 chars)"}), 400
        # Use UUID-based user_id to guarantee uniqueness
        user_id = uuid.uuid4().hex[:12]
        conn = get_db()
        try:
            existing = conn.execute(
                "SELECT id FROM users WHERE LOWER(username)=LOWER(?)", (username,)
            ).fetchone()
            if existing:
                return jsonify({"error": "Username already taken"}), 409
            conn.execute(
                "INSERT INTO users (id, username, password_hash, created_at) VALUES (?,?,?,?)",
                (user_id, username, hash_pw(password), time.strftime("%Y-%m-%dT%H:%M:%S"))
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "user_id": user_id, "username": username})
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

@app.route("/api/login", methods=["POST"])
def login():
    try:
        data     = request.json or {}
        username = (data.get("username") or "").strip()
        password = data.get("password") or ""
        if not username or not password:
            return jsonify({"error": "Username and password required"}), 400
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT id, username FROM users WHERE LOWER(username)=LOWER(?) AND password_hash=?",
                (username, hash_pw(password))
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return jsonify({"error": "Invalid username or password"}), 401
        return jsonify({"ok": True, "user_id": row["id"], "username": row["username"]})
    except Exception as e:
        return jsonify({"error": f"Server error: {str(e)}"}), 500

# ── ECG routes ────────────────────────────────────
@app.route("/api/ecgs")
def get_ecgs():
    try:
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
        try:
            rows = conn.execute(sql, params).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ecgs/<ecg_id>")
def get_ecg(ecg_id):
    try:
        conn = get_db()
        try:
            row = conn.execute("SELECT * FROM ecgs WHERE id=?", (ecg_id,)).fetchone()
        finally:
            conn.close()
        if not row: return jsonify({"error": "Not found"}), 404
        return jsonify(dict(row))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/categories")
def get_categories():
    try:
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT category, COUNT(*) as n FROM ecgs GROUP BY category ORDER BY n DESC"
            ).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/stats")
def get_stats():
    try:
        conn  = get_db()
        try:
            total = conn.execute("SELECT COUNT(*) FROM ecgs").fetchone()[0]
        finally:
            conn.close()
        return jsonify({"total": total, "version": "2.1.0", "status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Progress routes ───────────────────────────────
@app.route("/api/progress/<user_id>", methods=["GET"])
def get_progress(user_id):
    try:
        conn = get_db()
        try:
            rows = conn.execute("SELECT ecg_id FROM progress WHERE user_id=?", (user_id,)).fetchall()
        finally:
            conn.close()
        return jsonify([r["ecg_id"] for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/progress", methods=["POST"])
def save_progress():
    try:
        data    = request.json or {}
        user_id = data.get("user_id")
        ecg_id  = data.get("ecg_id")
        if not user_id or not ecg_id:
            return jsonify({"error": "Missing fields"}), 400
        conn = get_db()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO progress (user_id, ecg_id, studied_at) VALUES (?,?,?)",
                (user_id, ecg_id, time.strftime("%Y-%m-%dT%H:%M:%S"))
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Leaderboard ───────────────────────────────────
@app.route("/api/leaderboard")
def get_leaderboard():
    try:
        conn = get_db()
        try:
            rows = conn.execute("""
                SELECT u.username as name, COUNT(p.ecg_id) as score
                FROM users u
                LEFT JOIN progress p ON u.id = p.user_id
                WHERE u.username IS NOT NULL
                GROUP BY u.id ORDER BY score DESC LIMIT 20
            """).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Comments routes ───────────────────────────────
@app.route("/api/comments/<ecg_id>", methods=["GET"])
def get_comments(ecg_id):
    try:
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT * FROM comments WHERE ecg_id=? ORDER BY created_at DESC",
                (ecg_id,)
            ).fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/comments", methods=["POST"])
def post_comment():
    try:
        data = request.json or {}
        ecg_id   = data.get("ecg_id")
        user_id  = data.get("user_id")
        username = data.get("username")
        text     = (data.get("text") or "").strip()
        if not ecg_id or not user_id or not text:
            return jsonify({"error": "Missing fields"}), 400
        comment_id = uuid.uuid4().hex[:16]
        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO comments (id, ecg_id, user_id, username, text, created_at) VALUES (?,?,?,?,?,?)",
                (comment_id, ecg_id, user_id, username, text, time.strftime("%Y-%m-%dT%H:%M:%S"))
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "id": comment_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── ECG save/scrape ───────────────────────────────
@app.route("/api/scrape", methods=["POST"])
def receive_ecg():
    try:
        ecg = request.json
        if not ecg or not ecg.get("id"):
            return jsonify({"error": "Invalid data"}), 400
        conn = get_db()
        try:
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
        finally:
            conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/ecgs/all", methods=["DELETE"])
def delete_all_ecgs():
    try:
        conn = get_db()
        try:
            conn.execute("DELETE FROM ecgs")
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/")
def health():
    return jsonify({"status": "PulseBook API running", "version": "2.1.0"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, threaded=True)
