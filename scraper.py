"""
PulseBook Scraper v3
- LITFL: directly scrapes all 150 ECG cases (ecg-case-001 to ecg-case-150)
- Source URL stored for every ECG so you can verify/edit later
- Clears old junk data before running

Usage:
    python scraper.py

Requirements:
    pip install requests beautifulsoup4
"""

import requests
import hashlib
import time
import json
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

SERVER_URL = "https://web-production-52aae.up.railway.app"
HEADERS    = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36"}

# ── CLASSIFIER ────────────────────────────────────
RULES = [
    ("normal",                  ["normal sinus rhythm", "normal ecg", "normal 12-lead"]),
    ("arrhythmia_afib",         ["atrial fibrillation", "a-fib", "af with"]),
    ("arrhythmia_aflutter",     ["atrial flutter", "flutter waves", "sawtooth"]),
    ("arrhythmia_svt",          ["supraventricular tachycardia", "svt", "avnrt", "avrt", "psvt"]),
    ("arrhythmia_vt",           ["ventricular tachycardia", "v-tach", "vtach", "monomorphic vt"]),
    ("arrhythmia_vfib",         ["ventricular fibrillation", "v-fib", "vfib"]),
    ("arrhythmia_bradycardia",  ["sinus bradycardia", "bradycardia"]),
    ("arrhythmia_heart_block",  ["heart block", "av block", "bundle branch", "lbbb", "rbbb", "mobitz", "wenckebach"]),
    ("arrhythmia_wpw",          ["wolff-parkinson-white", "wpw", "delta wave", "pre-excitation"]),
    ("arrhythmia_pvc",          ["premature ventricular", "pvc", "ventricular ectopic", "bigeminy", "trigeminy"]),
    ("arrhythmia_pac",          ["premature atrial", "atrial ectopic"]),
    ("ischemia_stemi",          ["st elevation mi", "stemi", "st-elevation myocardial", "anterior stemi", "inferior stemi"]),
    ("ischemia_nstemi",         ["nstemi", "non-st elevation", "subendocardial"]),
    ("ischemia_angina",         ["angina", "ischaemia", "ischemia", "st depression"]),
    ("hypertrophy",             ["left ventricular hypertrophy", "lvh", "right ventricular hypertrophy", "rvh"]),
    ("electrolyte",             ["hyperkalemia", "hypokalaemia", "hypokalemia", "hyperkalaemia", "electrolyte"]),
    ("pericarditis",            ["pericarditis", "pericardial"]),
]

def classify(text):
    t = text.lower()
    for category, keywords in RULES:
        for kw in keywords:
            if kw in t:
                return category
    return "other"

def extract_difficulty(text):
    t = text.lower()
    if any(w in t for w in ["complete heart block", "torsades", "brugada", "wpw", "third degree", "polymorphic"]):
        return "advanced"
    if any(w in t for w in ["normal sinus", "sinus bradycardia"]):
        return "beginner"
    return "intermediate"

def extract_rate(text):
    m = re.search(r'rate[^\d]{0,20}(\d{2,3})\s*(bpm|beats)', text, re.IGNORECASE)
    if m: return m.group(1) + " bpm"
    m = re.search(r'(\d{2,3})\s*(bpm|beats per minute)', text, re.IGNORECASE)
    if m: return m.group(1) + " bpm"
    if "bradycardia" in text.lower(): return "< 60 bpm"
    if "tachycardia" in text.lower(): return "> 100 bpm"
    return ""

def extract_rhythm(text):
    t = text.lower()
    if "irregularly irregular" in t: return "Irregularly irregular"
    if "regularly irregular" in t:   return "Regularly irregular"
    if "irregular" in t:             return "Irregular"
    if "regular" in t:               return "Regular"
    return ""

def extract_axis(text):
    t = text.lower()
    if "left axis deviation" in t:  return "Left axis deviation"
    if "right axis deviation" in t: return "Right axis deviation"
    if "normal axis" in t:          return "Normal"
    return ""

def extract_intervals(text):
    parts = []
    m = re.search(r'pr[^\d]{0,10}(\d{3})\s*ms', text, re.IGNORECASE)
    if m: parts.append(f"PR {m.group(1)}ms")
    m = re.search(r'qrs[^\d]{0,10}(\d{2,3})\s*ms', text, re.IGNORECASE)
    if m: parts.append(f"QRS {m.group(1)}ms")
    m = re.search(r'qt[c]?[^\d]{0,10}(\d{3})\s*ms', text, re.IGNORECASE)
    if m: parts.append(f"QTc {m.group(1)}ms")
    return " · ".join(parts) if parts else ""

def build_ecg_id(url, image_url):
    return hashlib.md5((url + image_url).encode()).hexdigest()[:16]

# ── JUNK TEXT CLEANER ─────────────────────────────
JUNK_PATTERNS = [
    r'\[\d+\]',                           # [1] [2] citation numbers
    r'[A-Z][a-z]+ [A-Z]{1,2},.*\d{4}',  # Author, Year citations
    r'Am J[\w\s]+\d{4}.*',               # Journal references
    r'BMJ.*\d{4}.*',
    r'doi:.*',
    r'https?://\S+',
    r'©.*',
    r'Click here.*',
    r'Read more.*',
    r'See also.*',
    r'Share this.*',
    r'References\b.*',
    r'Further reading.*',
    r'Related Posts.*',
]

def clean_text(raw):
    text = raw
    for p in JUNK_PATTERNS:
        text = re.sub(p, '', text, flags=re.IGNORECASE | re.MULTILINE)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def good_sentences(text, max_chars=1000):
    """Keep only clinical interpretation sentences."""
    ref_markers = ["et al", "j med", "bmj", "lancet", "nejm", "pubmed",
                   "doi:", "vol.", "ed.", "isbn", "pp.", "2nd ed"]
    sentences = re.split(r'(?<=[.!?])\s+', text)
    good = []
    for s in sentences:
        s = s.strip()
        if len(s) < 25: continue
        if any(r in s.lower() for r in ref_markers): continue
        if re.search(r'[A-Z][a-z]+ [A-Z]{1,2},', s): continue  # author pattern
        good.append(s)
        if sum(len(x) for x in good) >= max_chars:
            break
    return good

# ── LITFL SCRAPER ─────────────────────────────────
def scrape_litfl_case(case_num):
    """
    Scrape a single LITFL case page e.g. /ecg-case-001/
    LITFL structure:
      - ECG image is in .wp-block-image or <figure> near top of post
      - Interpretation is under a heading like 'ECG Features' or 'Interpretation'
      - Answer/diagnosis is in a toggle/reveal section
    """
    url = f"https://litfl.com/ecg-case-{case_num:03d}/"
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        if r.status_code == 404:
            return None   # case doesn't exist
        r.raise_for_status()
    except Exception as e:
        print(f"    ✗ Case {case_num:03d}: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # ── Get page title ──
    h1 = soup.find("h1")
    page_title = h1.get_text(strip=True) if h1 else f"ECG Case {case_num:03d}"

    # ── Find ECG image ──
    # LITFL uses high-res images uploaded with "ECG" in filename
    image_url = ""
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src") or ""
        if not src: continue
        src_lower = src.lower()
        # Must look like an ECG image — not logos, avatars, icons
        if any(bad in src_lower for bad in ["logo", "icon", "avatar", "gravatar", "banner", "social"]):
            continue
        if any(good in src_lower for good in ["ecg", "ekg", "case", "tracing", "rhythm", "litfl"]):
            # Prefer larger images — check width attribute
            w = img.get("width", "0")
            try:
                if int(w) < 200: continue
            except: pass
            image_url = urljoin(url, src)
            break

    if not image_url:
        # Try first large image in post content
        content = soup.find("div", class_=lambda c: c and "entry-content" in (c or ""))
        if content:
            for img in content.find_all("img"):
                src = img.get("src") or img.get("data-src") or ""
                if src and "logo" not in src.lower():
                    image_url = urljoin(url, src)
                    break

    # ── Get interpretation text ──
    content = soup.find("div", class_=lambda c: c and "entry-content" in (c or ""))
    if not content:
        content = soup.find("article")
    if not content:
        return None

    # Remove navigation, sidebars, footers, reference sections
    for tag in content.find_all(["nav", "footer", "aside"]):
        tag.decompose()
    for tag in content.find_all(class_=lambda c: c and any(
            x in (c or "").lower() for x in ["related", "share", "comment", "sidebar", "widget", "reference", "author-bio"])):
        tag.decompose()

    # LITFL often has a "Description" or "Interpretation" or "ECG Features" section
    # Try to find the answer/interpretation heading specifically
    interp_text = ""
    for heading in content.find_all(["h2", "h3", "h4"]):
        heading_text = heading.get_text().lower()
        if any(k in heading_text for k in ["description", "interpretation", "ecg features", "answer", "diagnosis", "findings"]):
            # Collect the paragraphs that follow this heading
            parts = []
            for sib in heading.find_next_siblings():
                if sib.name in ["h2", "h3", "h4"]: break  # stop at next heading
                parts.append(sib.get_text(separator=" ", strip=True))
            interp_text = " ".join(parts)
            if len(interp_text) > 100:
                break

    # Fallback: use all paragraph text from content
    if len(interp_text) < 100:
        paras = content.find_all(["p", "li"])
        interp_text = " ".join(p.get_text(separator=" ", strip=True) for p in paras)

    cleaned   = clean_text(interp_text)
    sentences = good_sentences(cleaned)

    if not sentences:
        print(f"    ⚠ Case {case_num:03d}: no clean interpretation found")
        return None

    summary   = " ".join(sentences[:6])
    features  = sentences[:7]
    category  = classify(summary + " " + page_title)

    return {
        "id":          build_ecg_id(url, image_url),
        "category":    category,
        "subcategory": page_title,
        "difficulty":  extract_difficulty(summary),
        "site":        "LITFL",
        "source_url":  url,          # ← link to original for verification
        "image_url":   image_url,
        "rate":        extract_rate(summary),
        "rhythm":      extract_rhythm(summary),
        "axis":        extract_axis(summary),
        "intervals":   extract_intervals(summary),
        "summary":     summary,
        "features":    json.dumps(features),
        "pearls":      "",
    }

# ── PUSH TO SERVER ────────────────────────────────
def push_to_server(ecg):
    try:
        r = requests.post(f"{SERVER_URL}/api/scrape", json=ecg, timeout=15)
        return r.status_code == 200
    except Exception as e:
        print(f"    ✗ Upload failed: {e}")
        return False

def clear_database():
    try:
        r = requests.delete(f"{SERVER_URL}/api/ecgs/all", timeout=15)
        print("✓ Old data cleared" if r.status_code == 200 else f"⚠ Clear returned {r.status_code}")
    except Exception as e:
        print(f"⚠ Could not clear: {e}")

# ── MAIN ──────────────────────────────────────────
def run():
    print("=" * 60)
    print("  PulseBook Scraper v3 — LITFL 150 Cases")
    print("=" * 60)

    # Check server
    try:
        r     = requests.get(f"{SERVER_URL}/api/stats", timeout=10)
        stats = r.json()
        print(f"\n✓ Server connected — {stats.get('total', 0)} ECGs currently stored")
    except:
        print(f"\n✗ Cannot reach server at {SERVER_URL}")
        return

    print("\n🗑  Clearing old data for a fresh start...")
    clear_database()

    success = 0
    skipped = 0
    failed  = 0

    print("\n📋 Scraping LITFL ECG Cases 001–150...\n")

    for num in range(1, 151):
        print(f"  [{num:03d}/150] https://litfl.com/ecg-case-{num:03d}/", end=" ")

        ecg = scrape_litfl_case(num)

        if ecg is None:
            print("✗ skipped")
            skipped += 1
        elif push_to_server(ecg):
            print(f"✓ {ecg['subcategory'][:45]} [{ecg['category']}]")
            success += 1
        else:
            print("✗ upload failed")
            failed += 1

        time.sleep(1.5)  # be polite — 1.5s between requests

    print(f"\n{'='*60}")
    print(f"  ✅ Done!")
    print(f"     Uploaded : {success}")
    print(f"     Skipped  : {skipped} (no image or no interpretation)")
    print(f"     Failed   : {failed}")
    print(f"     Each ECG has its source_url stored for verification")
    print(f"{'='*60}")

if __name__ == "__main__":
    run()
