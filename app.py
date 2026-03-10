"""
PulseBook — Backend API Server v2.2
Runs on Railway.app (free tier)
v2.1: WAL mode, UUID IDs, try/finally (fixes SQLite locking crash)
v2.2: categories table, announcements table, ecg_language field
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
import sqlite3, os, time, hashlib, uuid

app = Flask(__name__)
CORS(app)

# Database Path — Support for Railway Volumes
# If /app/data exists (mounted Volume), we use it. Else we use the local directory.
DATA_DIR = "/app/data" if os.path.exists("/app/data") else os.path.dirname(__file__)
DB = os.path.join(DATA_DIR, "pulsebook.db")

# ── Database ──────────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ecgs (
            id           TEXT PRIMARY KEY,
            category     TEXT,
            subcategory  TEXT,
            difficulty   TEXT,
            site         TEXT,
            source_url   TEXT,
            image_url    TEXT,
            rate         TEXT,
            rhythm       TEXT,
            axis         TEXT,
            intervals    TEXT,
            summary      TEXT,
            features     TEXT,
            pearls       TEXT,
            scraped_at   TEXT,
            added_by     TEXT,
            ecg_language TEXT DEFAULT 'en'
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
        CREATE TABLE IF NOT EXISTS app_config (
            key        TEXT PRIMARY KEY,
            value      TEXT
        );
        CREATE TABLE IF NOT EXISTS comments (
            id          TEXT PRIMARY KEY,
            ecg_id      TEXT,
            user_id     TEXT,
            username    TEXT,
            text        TEXT,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS categories (
            id    TEXT PRIMARY KEY,
            name  TEXT UNIQUE,
            icon  TEXT DEFAULT '📋',
            color TEXT DEFAULT '#1a5fb4'
        );
        CREATE TABLE IF NOT EXISTS announcements (
            id         TEXT PRIMARY KEY,
            text       TEXT,
            created_at TEXT,
            active     INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS challenges (
            id         TEXT PRIMARY KEY,
            title      TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS challenge_questions (
            id         TEXT PRIMARY KEY,
            challenge_id TEXT,
            image_url  TEXT,
            clinical_text TEXT,
            options_json TEXT,
            correct_indices_json TEXT
        );
        CREATE TABLE IF NOT EXISTS lesson_categories (
            id    TEXT PRIMARY KEY,
            name  TEXT UNIQUE,
            icon  TEXT DEFAULT '📚',
            color TEXT DEFAULT '#1a5fb4'
        );
        CREATE TABLE IF NOT EXISTS lessons (
            id           TEXT PRIMARY KEY,
            category_id  TEXT,
            title        TEXT,
            content_html TEXT,
            image_urls   TEXT DEFAULT '[]',
            pdf_url      TEXT,
            created_at   TEXT,
            FOREIGN KEY (category_id) REFERENCES lesson_categories(id)
        );
        CREATE INDEX IF NOT EXISTS idx_comments_ecg ON comments(ecg_id);
    """)
    # Safe migrations for existing DBs
    for sql in [
        "ALTER TABLE ecgs ADD COLUMN added_by TEXT",
        "ALTER TABLE ecgs ADD COLUMN ecg_language TEXT DEFAULT 'en'",
        "ALTER TABLE ecgs ADD COLUMN clinical_text TEXT",
        "ALTER TABLE users ADD COLUMN username TEXT",
        "ALTER TABLE users ADD COLUMN password_hash TEXT",
    ]:
        try:
            conn.execute(sql)
            conn.commit()
        except Exception:
            pass
    # Add image_urls to lessons if missing
    try:
        conn.execute("ALTER TABLE lessons ADD COLUMN image_urls TEXT DEFAULT '[]'")
        conn.commit()
    except Exception:
        pass
    # Add color to lesson_categories if missing
    try:
        conn.execute("ALTER TABLE lesson_categories ADD COLUMN color TEXT DEFAULT '#1a5fb4'")
        conn.commit()
    except Exception:
        pass
    # Seed default categories if empty
    existing = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    if existing == 0:
        defaults = [
            ('normal',          'Normal',     '✅', '#1e7d4a'),
            ('ischemia_stemi',  'STEMI',      '🚨', '#d93636'),
            ('arrhythmia_afib', 'AFib',       '💓', '#7040b8'),
            ('arrhythmia_vt',   'V-Tach',     '⚡', '#c97c10'),
            ('hypertrophy',     'Hypertrophy','💪', '#1a5fb4'),
            ('other',           'Other',      '📋', '#5a5a5a'),
        ]
        for d in defaults:
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO categories (id,name,icon,color) VALUES (?,?,?,?)", d
                )
            except Exception:
                pass
    conn.commit()

    # Seed Config
    conn.execute("INSERT OR IGNORE INTO app_config (key, value) VALUES ('contact_email', 'admin@example.com')")
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

@app.route("/api/stats")
def get_stats():
    try:
        conn  = get_db()
        try:
            total = conn.execute("SELECT COUNT(*) FROM ecgs").fetchone()[0]
        finally:
            conn.close()
        return jsonify({"total": total, "version": "2.2.0", "status": "ok"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Categories ────────────────────────────────────
@app.route("/api/categories", methods=["GET"])
def get_categories():
    try:
        conn = get_db()
        try:
            rows = conn.execute("SELECT * FROM categories ORDER BY name").fetchall()
        finally:
            conn.close()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/categories", methods=["POST"])
def add_category():
    try:
        data  = request.json or {}
        name  = (data.get("name") or "").strip()
        icon  = data.get("icon", "📋")
        color = data.get("color", "#1a5fb4")
        if not name:
            return jsonify({"error": "Name required"}), 400
        cat_id = name.lower().replace(" ", "_")
        conn = get_db()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO categories (id,name,icon,color) VALUES (?,?,?,?)",
                (cat_id, name, icon, color)
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "id": cat_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/categories/<cat_id>", methods=["PUT"])
def edit_category(cat_id):
    try:
        data = request.json or {}
        name = (data.get("name") or "").strip()
        icon = data.get("icon", "📋")
        if not name: return jsonify({"error": "Name required"}), 400
        conn = get_db()
        try:
            conn.execute(
                "UPDATE categories SET name=?, icon=? WHERE id=?",
                (name, icon, cat_id)
            )
            # Update all ECGs matching this category to the new name? Let's rely on ID relation
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/categories/<cat_id>", methods=["DELETE"])
def delete_category(cat_id):
    try:
        conn = get_db()
        try:
            conn.execute("DELETE FROM categories WHERE id=?", (cat_id,))
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Announcements ─────────────────────────────────
@app.route("/api/announcements", methods=["GET"])
def get_announcements():
    try:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT * FROM announcements WHERE active=1 ORDER BY created_at DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return jsonify(None)
        return jsonify(dict(row))
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/announcements", methods=["POST"])
def post_announcement():
    try:
        data = request.json or {}
        text = (data.get("text") or "").strip()
        if not text:
            return jsonify({"error": "Text required"}), 400
        ann_id = uuid.uuid4().hex[:12]
        conn = get_db()
        try:
            # Deactivate all previous announcements
            conn.execute("UPDATE announcements SET active=0")
            conn.execute(
                "INSERT INTO announcements (id,text,created_at,active) VALUES (?,?,?,1)",
                (ann_id, text, time.strftime("%Y-%m-%dT%H:%M:%S"))
            )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "id": ann_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/announcements/dismiss", methods=["POST"])
def dismiss_announcement():
    try:
        conn = get_db()
        try:
            conn.execute("UPDATE announcements SET active=0")
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True})
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
                SELECT u.username as name, u.id as user_id, COUNT(p.ecg_id) as score
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
                "SELECT * FROM comments WHERE ecg_id=? ORDER BY created_at ASC",
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
                 rate,rhythm,axis,intervals,summary,features,pearls,scraped_at,added_by,ecg_language,clinical_text)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ecg.get("id"),           ecg.get("category"),
                ecg.get("subcategory"),  ecg.get("difficulty","intermediate"),
                ecg.get("site",""),      ecg.get("source_url",""),
                ecg.get("image_url",""), ecg.get("rate",""),
                ecg.get("rhythm",""),    ecg.get("axis",""),
                ecg.get("intervals",""), ecg.get("summary",""),
                ecg.get("features",""),  ecg.get("pearls",""),
                time.strftime("%Y-%m-%dT%H:%M:%S"),
                ecg.get("added_by",""),
                ecg.get("ecg_language","en"),
                ecg.get("clinical_text","")
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

# ── Challenges ────────────────────────────────────
import json

@app.route("/api/challenges", methods=["GET"])
def get_challenges():
    try:
        conn = get_db()
        try:
            c_rows = conn.execute("SELECT * FROM challenges ORDER BY created_at DESC").fetchall()
            q_rows = conn.execute("SELECT * FROM challenge_questions").fetchall()
            
            challenges = []
            for c in c_rows:
                cdict = dict(c)
                cdict["questions"] = [
                    dict(q) for q in q_rows if q["challenge_id"] == c["id"]
                ]
                # parse JSON fields
                for q in cdict["questions"]:
                    try: q["options"] = json.loads(q["options_json"])
                    except: q["options"] = []
                    try: q["correct_indices"] = json.loads(q["correct_indices_json"])
                    except: q["correct_indices"] = []
                    # Keep raw payload lean without stringified json
                    del q["options_json"]
                    del q["correct_indices_json"]
                    
                challenges.append(cdict)
        finally:
            conn.close()
        return jsonify(challenges)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/challenges", methods=["POST"])
def create_challenge():
    try:
        data = request.json or {}
        title = (data.get("title") or "").strip()
        questions = data.get("questions", [])
        if not title or not questions:
            return jsonify({"error": "Title and questions required"}), 400
            
        challenge_id = uuid.uuid4().hex[:12]
        conn = get_db()
        try:
            conn.execute(
                "INSERT INTO challenges (id, title, created_at) VALUES (?,?,?)",
                (challenge_id, title, time.strftime("%Y-%m-%dT%H:%M:%S"))
            )
            for q in questions:
                q_id = uuid.uuid4().hex[:12]
                conn.execute(
                    "INSERT INTO challenge_questions (id, challenge_id, image_url, clinical_text, options_json, correct_indices_json) VALUES (?,?,?,?,?,?)",
                    (q_id, challenge_id, q.get("image_url",""), q.get("clinical_text",""), json.dumps(q.get("options",[])), json.dumps(q.get("correct_indices",[])))
                )
            conn.commit()
        finally:
            conn.close()
        return jsonify({"ok": True, "id": challenge_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Configuration ────────────────────────────────

@app.route("/api/config", methods=["GET"])
def get_config():
    try:
        conn = get_db()
        try:
            rows = conn.execute("SELECT key, value FROM app_config").fetchall()
            config = {r["key"]: r["value"] for r in rows}
            return jsonify(config)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/config", methods=["POST"])
def update_config():
    try:
        data = request.json or {}
        conn = get_db()
        try:
            for k, v in data.items():
                conn.execute(
                    "INSERT INTO app_config (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (k, v)
                )
            conn.commit()
            return jsonify({"ok": True})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ── Lessons ────────────────────────────────────────
@app.route("/api/lesson_categories", methods=["GET", "POST"])
def manage_lesson_categories():
    try:
        conn = get_db()
        try:
            if request.method == "POST":
                data = request.json
                cid = str(uuid.uuid4())[:8]
                conn.execute("INSERT INTO lesson_categories (id, name, icon, color) VALUES (?, ?, ?, ?)",
                            (cid, data.get("name"), data.get("icon", "📚"), data.get("color", "#1a5fb4")))
                conn.commit()
                return jsonify({"id": cid})
            rows = conn.execute("SELECT * FROM lesson_categories").fetchall()
            return jsonify([dict(r) for r in rows])
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/lesson_categories/<cid>", methods=["DELETE", "PUT"])
def sub_manage_lesson_category(cid):
    try:
        conn = get_db()
        try:
            if request.method == "DELETE":
                conn.execute("DELETE FROM lesson_categories WHERE id=?", (cid,))
                conn.execute("DELETE FROM lessons WHERE category_id=?", (cid,))
                conn.commit()
                return jsonify({"ok": True})
            if request.method == "PUT":
                data = request.json
                conn.execute("UPDATE lesson_categories SET name=?, icon=?, color=? WHERE id=?",
                            (data.get("name"), data.get("icon"), data.get("color"), cid))
                conn.commit()
                return jsonify({"ok": True})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/lessons", methods=["GET", "POST"])
def manage_lessons():
    try:
        conn = get_db()
        try:
            if request.method == "POST":
                data = request.json
                lid = str(uuid.uuid4())[:8]
                image_urls = data.get("image_urls", [])
                import json as _json
                if isinstance(image_urls, list):
                    image_urls = _json.dumps(image_urls)
                conn.execute(
                    "INSERT INTO lessons (id, category_id, title, content_html, image_urls, pdf_url, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (lid, data.get("category_id"), data.get("title"), data.get("content_html"),
                     image_urls, data.get("pdf_url", ""), time.strftime("%Y-%m-%d %H:%M:%S"))
                )
                conn.commit()
                row = conn.execute("SELECT * FROM lessons WHERE id=?", (lid,)).fetchone()
                return jsonify(dict(row))
            cat_id = request.args.get("category_id")
            if cat_id:
                rows = conn.execute("SELECT * FROM lessons WHERE category_id=? ORDER BY created_at DESC", (cat_id,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM lessons ORDER BY created_at DESC").fetchall()
            return jsonify([dict(r) for r in rows])
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/lessons/<lid>", methods=["GET", "DELETE", "PUT"])
def sub_manage_lesson(lid):
    try:
        conn = get_db()
        try:
            if request.method == "GET":
                row = conn.execute("SELECT * FROM lessons WHERE id=?", (lid,)).fetchone()
                if not row:
                    return jsonify({"error": "Not found"}), 404
                return jsonify(dict(row))
            if request.method == "DELETE":
                conn.execute("DELETE FROM lessons WHERE id=?", (lid,))
                conn.commit()
                return jsonify({"ok": True})
            if request.method == "PUT":
                data = request.json
                import json as _json
                image_urls = data.get("image_urls", None)
                if isinstance(image_urls, list):
                    image_urls = _json.dumps(image_urls)
                # Build dynamic update
                fields = []
                vals = []
                for k in ["title", "content_html", "pdf_url"]:
                    if k in data:
                        fields.append(f"{k}=?")
                        vals.append(data[k])
                if image_urls is not None:
                    fields.append("image_urls=?")
                    vals.append(image_urls)
                if not fields:
                    return jsonify({"error": "Nothing to update"}), 400
                vals.append(lid)
                conn.execute(f"UPDATE lessons SET {', '.join(fields)} WHERE id=?", vals)
                conn.commit()
                row = conn.execute("SELECT * FROM lessons WHERE id=?", (lid,)).fetchone()
                return jsonify(dict(row) if row else {"error": "Not found"})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/admin/backup", methods=["GET"])
def backup_db():
    try:
        # Check for simple admin code check via query param for security
        pw = request.args.get("pw")
        if pw != "PULSE2024":
            return jsonify({"error": "Unauthorized"}), 403
            
        from flask import send_file
        if os.path.exists(DB):
            return send_file(DB, as_attachment=True, download_name="pulsebook_backup.db")
        else:
            return jsonify({"error": "Database file not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/")
def health():
    return jsonify({"status": "PulseBook Backend Online", "version": "2.3 (Persistent Volume Support)"})

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), threaded=True)
