"""
PulseBook Scraper v4
- LITFL: directly scrapes all 150 ECG cases
- Extracts hidden interpretation from gb-accordion-text dropdown
- Source URL stored for every ECG for verification/editing later

Usage:  python scraper.py
Needs:  pip install requests beautifulsoup4
"""

import requests, hashlib, time, json, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

SERVER_URL = "https://web-production-52aae.up.railway.app"
HEADERS    = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

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
    if any(w in t for w in ["complete heart block","torsades","brugada","wpw","third degree","polymorphic"]):
        return "advanced"
    if any(w in t for w in ["normal sinus","sinus bradycardia"]):
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


def fix_unicode(text):
    """Fix unicode escape sequences and curly quotes."""
    replacements = {
        '\u2018': "'", '\u2019': "'",
        '\u201c': '"', '\u201d': '"',
        '\u2013': '-', '\u2014': ' - ',
        '\u2026': '...',
        '‘': "'", '’': "'",
        '“': '"', '”': '"',
        '–': '-', '—': ' - ',
        '…': '...',
    }
    for k, v in replacements.items():
        text = text.replace(k, v)
    return text

def clean_text(raw):
    raw = fix_unicode(raw)
    junk = [
        r'\[\d+\]',
        r'[A-Z][a-z]+ [A-Z]{1,2},.*\d{4}',
        r'Am J[\w\s]+\d{4}.*',
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
        r'LITFL.*Review.*',
        r'Emergency Medicine.*',
    ]
    for p in junk:
        raw = re.sub(p, '', raw, flags=re.IGNORECASE | re.MULTILINE)
    return re.sub(r'\s+', ' ', raw).strip()

def good_sentences(text, max_chars=1200):
    """Keep clinical sentences, skip reference/junk sentences."""
    skip_markers = ["et al", "j med", "bmj", "lancet", "nejm", "pubmed",
                    "doi:", "vol.", "isbn", "pp.", "2nd ed", "copyright",
                    "subscribe", "newsletter", "follow us", "social media"]
    sentences = re.split(r'(?<=[.!?])\s+', text)
    good = []
    for s in sentences:
        s = s.strip()
        if len(s) < 20: continue
        if any(m in s.lower() for m in skip_markers): continue
        if re.search(r'[A-Z][a-z]+ [A-Z]{1,2},\s*\d{4}', s): continue
        good.append(s)
        if sum(len(x) for x in good) >= max_chars: break
    return good

# ── LITFL SCRAPER ─────────────────────────────────
def scrape_litfl_case(case_num):
    url = f"https://litfl.com/ecg-case-{case_num:03d}/"
    try:
        r = requests.get(url, timeout=25, headers=HEADERS)
        if r.status_code == 404: return None
        r.raise_for_status()
    except Exception as e:
        print(f"  ✗ error: {e}")
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    # Page title
    h1 = soup.find("h1")
    page_title = h1.get_text(strip=True) if h1 else f"ECG Case {case_num:03d}"

    # ── Find ECG image ──
    # LITFL structure: <figure class="wp-block-image"> > <a href="IMAGE_URL"> > <img src="...">
    image_url = ""

    # Strategy 1: get full-res URL from <a href> inside wp-block-image figure
    for fig in soup.find_all("figure", class_=lambda c: c and "wp-block-image" in (c or "")):
        a = fig.find("a", href=True)
        if a:
            href = a["href"]
            if any(ext in href.lower() for ext in [".jpg",".jpeg",".png",".gif",".webp"]):
                image_url = href
                break
        # fallback: img inside figure
        img = fig.find("img")
        if img:
            src = (img.get("data-orig-file") or img.get("src") or
                   img.get("data-large-file") or img.get("data-src") or "")
            if src:
                image_url = urljoin(url, src)
                break

    # Strategy 2: any img with ECG/LITFL in URL from content area
    if not image_url:
        content_area = (soup.find("div", class_=lambda c: "entry-content" in (c or ""))
                        or soup.find("article"))
        if content_area:
            for img in content_area.find_all("img"):
                src = (img.get("data-orig-file") or img.get("src") or
                       img.get("data-large-file") or img.get("data-lazy-src") or "")
                if not src: continue
                s = src.lower()
                if any(b in s for b in ["logo","icon","avatar","gravatar","banner","author","social","spinner","comment"]): continue
                if any(good in s for good in ["ecg","ekg","litfl","quiz","upload","wp-content"]):
                    image_url = urljoin(url, src)
                    break

    # ── Get interpretation text ──
    # STRATEGY 1: gb-accordion-text — LITFL's collapsible answer section
    interp_text = ""
    accordion = soup.find_all(class_=lambda c: c and "gb-accordion-text" in (c or ""))
    if accordion:
        interp_text = " ".join(d.get_text(separator=" ", strip=True) for d in accordion)

    # STRATEGY 2: Any <details> element (HTML5 accordion)
    if len(interp_text) < 80:
        details_texts = []
        for det in soup.find_all("details"):
            # Remove the summary title from the text
            for summ in det.find_all("summary"):
                summ.decompose()
            details_texts.append(det.get_text(separator=" ", strip=True))
        if details_texts:
            interp_text = " ".join(details_texts)

    # STRATEGY 3: wp-block-genesis-blocks-gb-accordion
    if len(interp_text) < 80:
        accordion2 = soup.find_all(class_=lambda c: c and "gb-accordion" in (c or "") and "gb-accordion-title" not in (c or ""))
        if accordion2:
            interp_text = " ".join(d.get_text(separator=" ", strip=True) for d in accordion2)

    # STRATEGY 4: Headings like Interpretation / Answer / ECG Features
    if len(interp_text) < 80 and content_area:
        for heading in content_area.find_all(["h2","h3","h4"]):
            ht = heading.get_text().lower()
            if any(k in ht for k in ["description","interpretation","ecg feature","answer","diagnosis","finding","result"]):
                parts = []
                for sib in heading.find_next_siblings():
                    if sib.name in ["h2","h3","h4"]: break
                    parts.append(sib.get_text(separator=" ", strip=True))
                candidate = " ".join(parts)
                if len(candidate) > len(interp_text):
                    interp_text = candidate

    # STRATEGY 5: All paragraphs (last resort)
    if len(interp_text) < 80 and content_area:
        for rm in content_area.find_all(class_=lambda c: c and any(
                x in (c or "").lower() for x in ["related","share","comment","author-bio","sidebar"])):
            rm.decompose()
        interp_text = " ".join(p.get_text(separator=" ", strip=True)
                               for p in content_area.find_all(["p","li"]))

    cleaned   = clean_text(interp_text)
    sentences = good_sentences(cleaned)

    if not sentences:
        return None

    summary  = " ".join(sentences[:6])
    features = sentences[:7]
    category = classify(summary + " " + page_title)

    return {
        "id":          build_ecg_id(url, image_url),
        "category":    category,
        "subcategory": page_title,
        "difficulty":  extract_difficulty(summary),
        "site":        "LITFL",
        "source_url":  url,
        "image_url":   image_url,
        "rate":        extract_rate(summary),
        "rhythm":      extract_rhythm(summary),
        "axis":        extract_axis(summary),
        "intervals":   extract_intervals(summary),
        "summary":     summary,
        "features":    json.dumps(features),
        "pearls":      "",
    }

# ── SERVER ────────────────────────────────────────
def push_to_server(ecg):
    try:
        r = requests.post(f"{SERVER_URL}/api/scrape", json=ecg, timeout=15)
        return r.status_code == 200
    except Exception as e:
        print(f"  ✗ upload failed: {e}")
        return False

def clear_database():
    try:
        r = requests.delete(f"{SERVER_URL}/api/ecgs/all", timeout=15)
        print("✓ Database cleared" if r.status_code == 200 else f"⚠ status {r.status_code}")
    except Exception as e:
        print(f"⚠ Could not clear: {e}")

# ── MAIN ──────────────────────────────────────────
def run():
    print("=" * 60)
    print("  PulseBook Scraper v4 — LITFL 150 Cases")
    print("=" * 60)

    try:
        stats = requests.get(f"{SERVER_URL}/api/stats", timeout=10).json()
        print(f"\n✓ Server connected — {stats.get('total',0)} ECGs currently stored")
    except:
        print(f"\n✗ Cannot reach server"); return

    print("\n🗑  Clearing old data...")
    clear_database()

    success = skipped = failed = 0
    print("\n📋 Scraping LITFL ECG Cases 001–150...\n")

    for num in range(1, 151):
        print(f"  [{num:03d}/150]", end=" ", flush=True)
        ecg = scrape_litfl_case(num)

        if ecg is None:
            print("⚠ skipped (no image or no interpretation)")
            skipped += 1
        elif push_to_server(ecg):
            print(f"✓ {ecg['subcategory'][:45]} [{ecg['category']}]")
            success += 1
        else:
            print("✗ upload failed")
            failed += 1

        time.sleep(1.5)

    print(f"\n{'='*60}")
    print(f"  ✅ Finished!")
    print(f"     ✓ Uploaded : {success}")
    print(f"     ⚠ Skipped  : {skipped}")
    print(f"     ✗ Failed   : {failed}")
    print(f"  Each ECG has source_url saved for verification")
    print(f"{'='*60}")

if __name__ == "__main__":
    run()
