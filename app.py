"""
PulseBook — Backend API Server
Runs on Railway.app (free tier)
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3, os, time

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
            scraped_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS users (
            id          TEXT PRIMARY KEY,
            name        TEXT,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS progress (
            user_id     TEXT,
            ecg_id      TEXT,
            studied_at  TEXT,
            PRIMARY KEY (user_id, ecg_id)
        );
    """)
    conn.commit()
    conn.close()

init_db()

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
    return jsonify({"total": total, "version": "1.0.0", "status": "ok"})

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

@app.route("/api/leaderboard")
def get_leaderboard():
    conn = get_db()
    rows = conn.execute("""
        SELECT u.name, COUNT(p.ecg_id) as score
        FROM users u
        LEFT JOIN progress p ON u.id = p.user_id
        GROUP BY u.id ORDER BY score DESC LIMIT 20
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.route("/api/users", methods=["POST"])
def create_user():
    data    = request.json or {}
    import hashlib
    user_id = hashlib.md5(data.get("name","").encode()).hexdigest()[:12]
    conn    = get_db()
    conn.execute(
        "INSERT OR IGNORE INTO users (id, name, created_at) VALUES (?,?,?)",
        (user_id, data.get("name",""), time.strftime("%Y-%m-%dT%H:%M:%S"))
    )
    conn.commit()
    conn.close()
    return jsonify({"user_id": user_id})

@app.route("/api/scrape", methods=["POST"])
def receive_ecg():
    ecg = request.json
    if not ecg or not ecg.get("id"):
        return jsonify({"error": "Invalid data"}), 400
    conn = get_db()
    conn.execute("""
        INSERT OR REPLACE INTO ecgs
        (id,category,subcategory,difficulty,site,source_url,image_url,
         rate,rhythm,axis,intervals,summary,features,pearls,scraped_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        ecg.get("id"),           ecg.get("category"),
        ecg.get("subcategory"),  ecg.get("difficulty","intermediate"),
        ecg.get("site",""),      ecg.get("source_url",""),
        ecg.get("image_url",""), ecg.get("rate",""),
        ecg.get("rhythm",""),    ecg.get("axis",""),
        ecg.get("intervals",""), ecg.get("summary",""),
        ecg.get("features",""),  ecg.get("pearls",""),
        time.strftime("%Y-%m-%dT%H:%M:%S")
    ))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/")
def health():
    return jsonify({"status": "PulseBook API running", "version": "1.0.0"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
