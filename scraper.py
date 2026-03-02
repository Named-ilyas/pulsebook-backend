"""
PulseBook Scraper v2 — Smarter extraction
- Only grabs real ECG images (strict filtering)
- Extracts clean clinical interpretation (skips citations/references)
- Includes source URL for verification
- Clears junk data before re-scraping

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

SERVER_URL = "https://web-production-52aae.up.railway.app"

SITES = [
    {"url": "https://litfl.com/ecg-library/",         "name": "LITFL"},
    {"url": "https://ecgwaves.com/topic/ecg-rhythm/",  "name": "ECGWaves"},
]

HEADERS = {"User-Agent": "PulseBook-Scraper/2.0 (Medical Education)"}

# ── CLASSIFIER ────────────────────────────────────
RULES = [
    ("normal",                  ["normal sinus rhythm", "normal ecg", "normal 12-lead"]),
    ("arrhythmia_afib",         ["atrial fibrillation", "a-fib", "af with", "af pattern"]),
    ("arrhythmia_aflutter",     ["atrial flutter", "flutter waves", "sawtooth"]),
    ("arrhythmia_svt",          ["supraventricular tachycardia", "svt", "avnrt", "avrt", "psvt"]),
    ("arrhythmia_vt",           ["ventricular tachycardia", "v-tach", "vtach", "monomorphic vt", "polymorphic vt"]),
    ("arrhythmia_vfib",         ["ventricular fibrillation", "v-fib", "vfib"]),
    ("arrhythmia_bradycardia",  ["sinus bradycardia", "bradycardia"]),
    ("arrhythmia_heart_block",  ["heart block", "av block", "atrioventricular block", "bundle branch", "lbbb", "rbbb", "mobitz", "wenckebach"]),
    ("arrhythmia_wpw",          ["wolff-parkinson-white", "wpw", "delta wave", "pre-excitation"]),
    ("arrhythmia_pvc",          ["premature ventricular", "pvc", "ventricular ectopic", "bigeminy", "trigeminy"]),
    ("arrhythmia_pac",          ["premature atrial", "atrial ectopic"]),
    ("ischemia_stemi",          ["st elevation mi", "stemi", "st-elevation myocardial"]),
    ("ischemia_nstemi",         ["nstemi", "non-st elevation", "non st elevation", "subendocardial"]),
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
    if any(w in t for w in ["normal sinus", "sinus bradycardia", "pac", "pvc"]):
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
    if "left axis deviation" in t or "lad" in t: return "Left axis deviation"
    if "right axis deviation" in t or "rad" in t: return "Right axis deviation"
    if "normal axis" in t: return "Normal"
    return ""

def extract_intervals(text):
    parts = []
    m = re.search(r'pr interval[^\d]{0,10}(\d+)\s*ms', text, re.IGNORECASE)
    if m: parts.append(f"PR {m.group(1)}ms")
    m = re.search(r'qrs[^\d]{0,10}(\d+)\s*ms', text, re.IGNORECASE)
    if m: parts.append(f"QRS {m.group(1)}ms")
    m = re.search(r'qt[c]?[^\d]{0,10}(\d+)\s*ms', text, re.IGNORECASE)
    if m: parts.append(f"QTc {m.group(1)}ms")
    return " · ".join(parts) if parts else ""

def build_ecg_id(url, image_url):
    return hashlib.md5((url + image_url).encode()).hexdigest()[:16]

# ── IMAGE VALIDATOR ───────────────────────────────
# Only accept images that are almost certainly real ECGs

ECG_IMAGE_KEYWORDS = ["ecg", "ekg", "tracing", "rhythm", "strip", "lead", "12lead", "waveform"]
JUNK_IMAGE_KEYWORDS = ["logo", "icon", "avatar", "banner", "thumb", "social", "facebook",
                        "twitter", "echo", "ultrasound", "xray", "ct-", "mri", "avatar",
                        "author", "profile", "badge", "button", "arrow", "sponsor", "ad-"]
JUNK_EXTENSIONS = [".gif", ".svg"]
MIN_IMAGE_SIZE_HINT = 200  # ignore tiny images (width/height attribute < 200px)

def is_ecg_image(img_tag, img_url):
    url_lower  = img_url.lower()
    alt_lower  = (img_tag.get("alt") or "").lower()
    src_lower  = (img_tag.get("src") or "").lower()

    # Reject known junk
    for junk in JUNK_IMAGE_KEYWORDS:
        if junk in url_lower or junk in alt_lower:
            return False

    # Reject junk extensions
    for ext in JUNK_EXTENSIONS:
        if url_lower.endswith(ext):
            return False

    # Reject tiny images
    for dim_attr in ["width", "height"]:
        val = img_tag.get(dim_attr, "")
        try:
            if int(val) < MIN_IMAGE_SIZE_HINT:
                return False
        except:
            pass

    # Must have ECG keyword in URL, alt, or nearby text
    for kw in ECG_IMAGE_KEYWORDS:
        if kw in url_lower or kw in alt_lower:
            return True

    # Check parent figure/div for ECG context
    parent = img_tag.parent
    for _ in range(3):
        if not parent: break
        parent_text = parent.get_text().lower()
        if any(kw in parent_text for kw in ECG_IMAGE_KEYWORDS):
            return True
        parent = parent.parent

    return False

# ── TEXT CLEANER ──────────────────────────────────
JUNK_PATTERNS = [
    r'\[\d+\]',                          # citation numbers [1], [2]
    r'\b(doi|pmid|isbn)\b.*',            # DOI/PMID lines
    r'[A-Z][a-z]+\s[A-Z]{1,2},.*\d{4}', # Author citations like "Smith SW, 2005"
    r'Am J.*\d{4}.*:\d+',               # Journal references
    r'BMJ.*\d{4}',
    r'https?://\S+',                     # URLs
    r'©.*\d{4}',                         # Copyright lines
    r'Creative Commons.*',
    r'Click here.*',
    r'Read more.*',
    r'See also.*',
    r'References\s*$',
    r'Further reading.*',
]

def clean_text(raw):
    text = raw
    for pattern in JUNK_PATTERNS:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)
    # Remove very short lines (likely nav/UI fragments)
    lines = [l.strip() for l in text.split('\n') if len(l.strip()) > 30]
    text = ' '.join(lines)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_clinical_sentences(text, max_chars=800):
    """Extract only clinical interpretation sentences, skip references."""
    sentences = re.split(r'(?<=[.!?])\s+', text)
    good = []
    ref_keywords = ["et al", "j med", "bmj", "lancet", "nejm", "pubmed",
                    "doi:", "vol.", "pp.", "ed.", "2nd ed", "textbook"]
    for s in sentences:
        s = s.strip()
        if len(s) < 25:
            continue
        s_lower = s.lower()
        if any(r in s_lower for r in ref_keywords):
            continue
        if re.search(r'[A-Z][a-z]+ [A-Z]{1,2},', s):  # author pattern
            continue
        good.append(s)
        if sum(len(x) for x in good) > max_chars:
            break
    return good

# ── LITFL SCRAPER (specialized) ───────────────────
def scrape_litfl_page(url):
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        r.raise_for_status()
    except Exception as e:
        print(f"    ✗ {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    h1 = soup.find("h1")
    page_title = h1.get_text(strip=True) if h1 else ""

    # LITFL puts ECG images in .wp-block-image or figure tags
    for fig in soup.find_all(["figure", "div"], class_=lambda c: c and any(
            x in c for x in ["wp-block-image", "entry-content", "post-content"])):
        for img in fig.find_all("img"):
            src = img.get("src") or img.get("data-src") or ""
            if not src: continue
            full_url = urljoin(url, src)
            if not is_ecg_image(img, full_url): continue

            # Get the main article content — NOT sidebar, NOT references
            content_div = soup.find("div", class_=lambda c: c and "entry-content" in c)
            if not content_div:
                content_div = soup.find("article")
            if not content_div:
                continue

            # Remove reference sections
            for ref_section in content_div.find_all(["div", "section"],
                    class_=lambda c: c and any(x in (c or "").lower() for x in
                    ["reference", "citation", "further-reading", "related"])):
                ref_section.decompose()

            # Get paragraphs from main content
            paras = content_div.find_all(["p", "li", "h2", "h3"])
            raw_parts = [p.get_text(separator=" ", strip=True) for p in paras]
            raw_text  = " ".join(raw_parts)
            clean     = clean_text(raw_text)
            sentences = extract_clinical_sentences(clean)

            if len(sentences) < 1:
                continue

            summary  = " ".join(sentences[:5])
            features = sentences[:6]
            category = classify(summary + " " + page_title)

            results.append({
                "id":          build_ecg_id(url, full_url),
                "category":    category,
                "subcategory": page_title or category.replace("_", " ").title(),
                "difficulty":  extract_difficulty(summary),
                "site":        "LITFL",
                "source_url":  url,
                "image_url":   full_url,
                "rate":        extract_rate(summary),
                "rhythm":      extract_rhythm(summary),
                "axis":        extract_axis(summary),
                "intervals":   extract_intervals(summary),
                "summary":     summary,
                "features":    json.dumps(features),
                "pearls":      "",
            })
            break  # one ECG per page on LITFL

    return results

# ── ECGWAVES SCRAPER (specialized) ────────────────
def scrape_ecgwaves_page(url):
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        r.raise_for_status()
    except Exception as e:
        print(f"    ✗ {url}: {e}")
        return []

    soup = BeautifulSoup(r.text, "html.parser")
    results = []

    h1 = soup.find("h1")
    page_title = h1.get_text(strip=True) if h1 else ""

    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if not src: continue
        full_url = urljoin(url, src)
        if not is_ecg_image(img, full_url): continue

        # Get surrounding content
        content = soup.find("div", class_=lambda c: c and any(
            x in (c or "") for x in ["post-content", "entry", "article-body", "content"]))
        if not content:
            content = soup.find("article")
        if not content:
            continue

        raw_text  = content.get_text(separator=" ", strip=True)
        clean     = clean_text(raw_text)
        sentences = extract_clinical_sentences(clean)

        if len(sentences) < 1:
            continue

        summary  = " ".join(sentences[:5])
        features = sentences[:6]
        category = classify(summary + " " + page_title)

        results.append({
            "id":          build_ecg_id(url, full_url),
            "category":    category,
            "subcategory": page_title or category.replace("_", " ").title(),
            "difficulty":  extract_difficulty(summary),
            "site":        "ECGWaves",
            "source_url":  url,
            "image_url":   full_url,
            "rate":        extract_rate(summary),
            "rhythm":      extract_rhythm(summary),
            "axis":        extract_axis(summary),
            "intervals":   extract_intervals(summary),
            "summary":     summary,
            "features":    json.dumps(features),
            "pearls":      "",
        })
        break  # one ECG per page

    return results

# ── CRAWLER ───────────────────────────────────────
def crawl_site(site, max_pages=60):
    base_url    = site["url"]
    site_name   = site["name"]
    visited     = set()
    to_visit    = [base_url]
    all_ecgs    = []
    base_domain = urlparse(base_url).netloc

    print(f"\n🔍 Crawling {site_name}")

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited: continue
        visited.add(url)
        print(f"  → {url[:80]}")

        if site_name == "LITFL":
            ecgs = scrape_litfl_page(url)
        else:
            ecgs = scrape_ecgwaves_page(url)

        all_ecgs.extend(ecgs)

        # Find more ECG links
        try:
            r    = requests.get(url, timeout=15, headers=HEADERS)
            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                href = urljoin(url, a["href"])
                if urlparse(href).netloc != base_domain: continue
                if href in visited: continue
                link_text = (a.get_text() + href).lower()
                if any(k in link_text for k in [
                    "ecg-", "ekg-", "arrhythmia", "rhythm", "ischemi",
                    "stemi", "block", "tachycardia", "fibrillation",
                    "bradycardia", "infarction", "hypertrophy", "pericarditis",
                    "flutter", "wpw", "pvc", "pac", "interval", "waveform"
                ]):
                    to_visit.append(href)
        except:
            pass

        time.sleep(1.2)

    print(f"  ✓ Found {len(all_ecgs)} valid ECGs on {site_name}")
    return all_ecgs

# ── PUSH TO SERVER ────────────────────────────────
def push_to_server(ecg):
    try:
        r = requests.post(f"{SERVER_URL}/api/scrape", json=ecg, timeout=15)
        return r.status_code == 200
    except Exception as e:
        print(f"    ✗ Upload failed: {e}")
        return False

def clear_database():
    """Wipe all existing ECGs so we start fresh with clean data."""
    try:
        r = requests.delete(f"{SERVER_URL}/api/ecgs/all", timeout=15)
        if r.status_code == 200:
            print("✓ Database cleared — starting fresh")
        else:
            print(f"⚠ Could not clear database (status {r.status_code}) — will overwrite duplicates")
    except Exception as e:
        print(f"⚠ Could not clear database: {e} — will overwrite duplicates")

# ── MAIN ──────────────────────────────────────────
def run():
    print("=" * 60)
    print("  PulseBook Scraper v2 — Clean ECGs Only")
    print("=" * 60)

    try:
        r     = requests.get(f"{SERVER_URL}/api/stats", timeout=10)
        stats = r.json()
        print(f"\n✓ Connected — {stats.get('total', 0)} ECGs currently in database")
    except:
        print(f"\n✗ Cannot reach server at {SERVER_URL}")
        return

    print("\n⚠  Clearing old data to start fresh with clean ECGs...")
    clear_database()

    total_new = 0
    for site in SITES:
        ecgs = crawl_site(site)
        print(f"\n  Uploading {len(ecgs)} ECGs from {site['name']}…")
        for ecg in ecgs:
            if push_to_server(ecg):
                total_new += 1
                print(f"    ✓ {ecg['subcategory'][:50]} [{ecg['category']}] — {ecg['source_url']}")
            time.sleep(0.3)

    print(f"\n{'='*60}")
    print(f"  ✅ Done! {total_new} clean ECGs in PulseBook")
    print(f"{'='*60}")

if __name__ == "__main__":
    run()
