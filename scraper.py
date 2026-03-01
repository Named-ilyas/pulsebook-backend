"""
PulseBook Scraper
Run this on your computer whenever you want to add new ECGs.
It scrapes ECG websites and sends them to your Render server.

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
from urllib.parse import urljoin, urlparse

# ── CONFIGURATION ─────────────────────────────────
# Paste your Render server URL here after deployment:
SERVER_URL = "https://YOUR-APP-NAME.onrender.com"

SITES = [
    {"url": "https://litfl.com/ecg-library/",            "name": "LITFL"},
    {"url": "https://ecgwaves.com/topic/ecg-rhythm/",    "name": "ECGWaves"},
    {"url": "https://ecgpedia.org/index.php?title=Main_Page", "name": "ECGPedia"},
]

HEADERS = {"User-Agent": "PulseBook-Scraper/1.0 (Medical Education)"}

# ── KEYWORD CLASSIFIER ────────────────────────────
RULES = [
    ("normal",                  ["normal sinus rhythm", "normal ecg", "normal 12-lead"]),
    ("arrhythmia_afib",         ["atrial fibrillation", "a-fib", "af with", "af pattern"]),
    ("arrhythmia_aflutter",     ["atrial flutter", "flutter waves", "sawtooth"]),
    ("arrhythmia_svt",          ["supraventricular tachycardia", "svt", "avnrt", "avrt", "psvt"]),
    ("arrhythmia_vt",           ["ventricular tachycardia", "v-tach", "vtach", "monomorphic vt", "polymorphic vt"]),
    ("arrhythmia_vfib",         ["ventricular fibrillation", "v-fib", "vfib", "vf "]),
    ("arrhythmia_bradycardia",  ["sinus bradycardia", "bradycardia"]),
    ("arrhythmia_heart_block",  ["heart block", "av block", "atrioventricular block", "bundle branch", "lbbb", "rbbb", "mobitz", "wenckebach"]),
    ("arrhythmia_wpw",          ["wolff-parkinson-white", "wpw", "delta wave", "pre-excitation"]),
    ("arrhythmia_pvc",          ["premature ventricular", "pvc", "ventricular ectopic", "bigeminy", "trigeminy"]),
    ("arrhythmia_pac",          ["premature atrial", "pac ", "atrial ectopic"]),
    ("ischemia_stemi",          ["st elevation mi", "stemi", "st-elevation myocardial"]),
    ("ischemia_nstemi",         ["nstemi", "non-st elevation", "non st elevation", "subendocardial"]),
    ("ischemia_angina",         ["angina", "ischaemia", "ischemia", "st depression"]),
    ("hypertrophy",             ["left ventricular hypertrophy", "lvh", "right ventricular hypertrophy", "rvh", "sokolow"]),
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
    if any(w in t for w in ["complete", "advanced", "complex", "third degree", "torsades", "brugada", "wpw"]):
        return "advanced"
    if any(w in t for w in ["normal", "sinus", "bradycardia", "pac", "pvc"]):
        return "beginner"
    return "intermediate"

def extract_rate(text):
    match = re.search(r'(\d{2,3})\s*(bpm|beats per minute|/min)', text, re.IGNORECASE)
    if match:
        return match.group(1) + " bpm"
    if "bradycardia" in text.lower(): return "< 60 bpm"
    if "tachycardia" in text.lower(): return "> 100 bpm"
    return "See interpretation"

def extract_rhythm(text):
    t = text.lower()
    if "irregularly irregular" in t: return "Irregularly irregular"
    if "irregular" in t:             return "Irregular"
    if "regular" in t:               return "Regular"
    return "See interpretation"

def build_ecg_id(url, image_url):
    return hashlib.md5((url + image_url).encode()).hexdigest()[:16]

# ── PAGE SCRAPER ──────────────────────────────────
def scrape_page(url, site_name):
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        r.raise_for_status()
    except Exception as e:
        print(f"    ✗ Cannot fetch {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    # Get page title / H1
    h1 = soup.find("h1")
    page_title = h1.get_text(strip=True) if h1 else (soup.title.string if soup.title else "")

    # Find images that look like ECGs
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        alt = (img.get("alt") or "").lower()
        if not src:
            continue

        # Check if this looks like an ECG image
        ecg_words = ["ecg", "ekg", "rhythm", "strip", "tracing", "lead", "waveform", "cardio"]
        src_lower = src.lower()
        looks_like_ecg = any(w in src_lower or w in alt for w in ecg_words)

        if not looks_like_ecg:
            # Check parent/nearby text
            parent_text = ""
            p = img.parent
            for _ in range(3):
                if p:
                    parent_text += p.get_text()
                    p = p.parent
            if not any(w in parent_text.lower() for w in ecg_words):
                continue

        full_img_url = urljoin(url, src)

        # Gather interpretation text near the image
        text_parts = []

        # figcaption
        fig = img.find_parent("figure")
        if fig:
            cap = fig.find("figcaption")
            if cap:
                text_parts.append(cap.get_text(strip=True))

        # Nearby paragraphs and headings
        parent = img.parent
        for _ in range(4):
            if not parent:
                break
            for tag in parent.find_next_siblings(["p","ul","ol","h2","h3","h4","div"])[:5]:
                text_parts.append(tag.get_text(strip=True))
            for tag in parent.find_previous_siblings(["p","h2","h3"])[:3]:
                text_parts.append(tag.get_text(strip=True))
            parent = parent.parent

        raw_text = " ".join(filter(None, text_parts))[:3000]
        if len(raw_text) < 40:
            continue

        category   = classify(raw_text)
        difficulty = extract_difficulty(raw_text)
        rate       = extract_rate(raw_text)
        rhythm     = extract_rhythm(raw_text)

        # Build feature list from first sentences
        sentences = [s.strip() for s in raw_text.split(".") if len(s.strip()) > 20][:5]

        results.append({
            "id":          build_ecg_id(url, full_img_url),
            "category":    category,
            "subcategory": page_title or category.replace("_"," ").title(),
            "difficulty":  difficulty,
            "site":        site_name,
            "source_url":  url,
            "image_url":   full_img_url,
            "rate":        rate,
            "rhythm":      rhythm,
            "axis":        "See interpretation",
            "intervals":   "See interpretation",
            "summary":     raw_text[:500],
            "features":    json.dumps(sentences),
            "pearls":      "",
        })

    return results

# ── SITE CRAWLER ──────────────────────────────────
def crawl_site(site, max_pages=40):
    base_url  = site["url"]
    site_name = site["name"]
    visited   = set()
    to_visit  = [base_url]
    all_ecgs  = []
    base_domain = urlparse(base_url).netloc

    print(f"\n🔍 Crawling {site_name} ({base_url})")

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        print(f"  → {url[:70]}")

        ecgs = scrape_page(url, site_name)
        all_ecgs.extend(ecgs)

        # Find more ECG-related links
        try:
            r = requests.get(url, timeout=15, headers=HEADERS)
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = urljoin(url, a["href"])
                if urlparse(href).netloc != base_domain:
                    continue
                if href in visited:
                    continue
                link_text = (a.get_text() + href).lower()
                if any(k in link_text for k in ["ecg","ekg","arrhythmia","rhythm","ischemi","stemi","block","tachycardia","fibrillation"]):
                    to_visit.append(href)
        except:
            pass

        time.sleep(1.2)  # be polite to servers

    print(f"  ✓ Found {len(all_ecgs)} ECGs on {site_name}")
    return all_ecgs

# ── PUSH TO SERVER ────────────────────────────────
def push_to_server(ecg):
    try:
        r = requests.post(
            f"{SERVER_URL}/api/scrape",
            json=ecg,
            timeout=15
        )
        return r.status_code == 200
    except Exception as e:
        print(f"    ✗ Upload failed: {e}")
        return False

# ── MAIN ──────────────────────────────────────────
def run():
    print("=" * 55)
    print("  PulseBook Scraper")
    print("=" * 55)

    if "YOUR-APP-NAME" in SERVER_URL:
        print("\n⚠️  ERROR: Please edit scraper.py and replace")
        print("   YOUR-APP-NAME with your actual Render URL")
        return

    # Check server is reachable
    try:
        r = requests.get(f"{SERVER_URL}/api/stats", timeout=10)
        stats = r.json()
        print(f"\n✓ Connected to server — {stats.get('total',0)} ECGs currently in database\n")
    except:
        print(f"\n✗ Cannot reach server at {SERVER_URL}")
        print("  Make sure your Render app is deployed and running.")
        return

    total_new = 0
    for site in SITES:
        ecgs = crawl_site(site)
        print(f"\n  Uploading to server…")
        for ecg in ecgs:
            if push_to_server(ecg):
                total_new += 1
                print(f"    ✓ {ecg['subcategory'][:50]} [{ecg['category']}]")
            time.sleep(0.3)

    print(f"\n{'='*55}")
    print(f"  ✅ Done! {total_new} ECGs uploaded to PulseBook")
    print(f"{'='*55}")

if __name__ == "__main__":
    run()
