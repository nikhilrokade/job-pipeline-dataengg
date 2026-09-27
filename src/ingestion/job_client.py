import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# Broad search criteria for Data Engineering roles
ROLE_PATTERNS = [
    r"data\s*eng",
    r"pyspark",
    r"databricks",
    r"big\s*data",
    r"etl",
    r"data\s*infra",
    r"data\s*platform",
    r"analytics\s*eng",
    r"data\s*architect",
    r"pipeline",
    r"data\s*lake",
]

# Locations matching your radius
LOCATION_PATTERNS = [
    r"mumbai",
    r"navi\s*mumbai",
    r"thane",
    r"pune",
    r"maharashtra",
    r"india",
    r"remote",
    r"anywhere",
    r"hybrid",
]

# Targeted Greenhouse boards
GREENHOUSE_BOARDS = [
    ("pubmatic", "PubMatic"),
    ("avalara", "Avalara"),
    ("thoughtworks", "Thoughtworks"),
    ("elastic", "Elastic"),
    ("fractal", "Fractal Analytics"),
    ("databricks", "Databricks"),
    ("canonical", "Canonical"),
    ("gitlab", "GitLab"),
]

# Targeted Lever boards
LEVER_SITES = [
    ("atlan", "Atlan"),
    ("clevertap", "CleverTap"),
    ("browserstack", "BrowserStack"),
    ("razorpay", "Razorpay"),
]


def matches_filter(text: str, patterns: List[str]) -> bool:
    if not text:
        return False
    return any(re.search(pat, text.lower()) for pat in patterns)


def generate_fingerprint(company: str, external_id: str, title: str, location: str) -> str:
    raw = f"{company.strip().lower()}|{str(external_id).strip()}|{title.strip().lower()}|{location.strip().lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def fetch_greenhouse_board(board_slug: str, display_name: str, now_utc: str) -> List[Dict[str, Any]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_slug}/jobs"
    headers = {"User-Agent": "JobMarketDataPipeline/3.0"}
    records = []
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            jobs = resp.json().get("jobs", [])
            for j in jobs:
                title = j.get("title", "")
                loc = j.get("location", {}).get("name", "")
                
                # Check role and location criteria
                if matches_filter(title, ROLE_PATTERNS) and (not loc or matches_filter(loc, LOCATION_PATTERNS)):
                    fp = generate_fingerprint(display_name, str(j.get("id")), title, loc)
                    records.append({
                        "dedup_fingerprint": fp,
                        "company_board": board_slug,
                        "source": "greenhouse",
                        "ingested_at_utc": now_utc,
                        "raw_payload": {
                            "id": j.get("id"),
                            "title": title,
                            "company_name": display_name,
                            "location": {"name": loc if loc else "Remote / Flexible"},
                            "absolute_url": j.get("absolute_url"),
                            "updated_at": j.get("updated_at") or now_utc,
                            "first_published": j.get("first_published") or now_utc,
                        },
                    })
    except Exception as e:
        logger.warning(f"Error fetching Greenhouse board '{board_slug}': {e}")
    return records


def fetch_lever_site(site_slug: str, display_name: str, now_utc: str) -> List[Dict[str, Any]]:
    url = f"https://api.lever.co/v0/postings/{site_slug}?mode=json"
    headers = {"User-Agent": "JobMarketDataPipeline/3.0"}
    records = []
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code == 200:
            postings = resp.json()
            for p in postings:
                title = p.get("text", "")
                loc = p.get("categories", {}).get("location", "")
                workplace_type = p.get("workplaceType", "")
                full_loc = f"{loc} ({workplace_type})" if workplace_type else loc

                if matches_filter(title, ROLE_PATTERNS) and (not full_loc or matches_filter(full_loc, LOCATION_PATTERNS)):
                    ext_id = p.get("id", "")
                    fp = generate_fingerprint(display_name, ext_id, title, full_loc)
                    # Convert string UUID to long integer ID for consistent schema matching
                    numeric_id = int(hashlib.md5(ext_id.encode()).hexdigest()[:12], 16)
                    records.append({
                        "dedup_fingerprint": fp,
                        "company_board": site_slug,
                        "source": "lever",
                        "ingested_at_utc": now_utc,
                        "raw_payload": {
                            "id": numeric_id,
                            "title": title,
                            "company_name": display_name,
                            "location": {"name": full_loc if full_loc else "Remote / Flexible"},
                            "absolute_url": p.get("hostedUrl"),
                            "updated_at": now_utc,
                            "first_published": now_utc,
                        },
                    })
    except Exception as e:
        logger.warning(f"Error fetching Lever site '{site_slug}': {e}")
    return records


def main():
    now_utc = datetime.now(timezone.utc).isoformat()
    all_records = []

    logger.info("Scanning Greenhouse tech hubs (PubMatic, Avalara, Thoughtworks, Elastic, etc.)...")
    for slug, name in GREENHOUSE_BOARDS:
        batch = fetch_greenhouse_board(slug, name, now_utc)
        logger.info(f" -> {name}: found {len(batch)} matching DE roles")
        all_records.extend(batch)

    logger.info("Scanning Lever tech hubs (Atlan, CleverTap, BrowserStack, Razorpay)...")
    for slug, name in LEVER_SITES:
        batch = fetch_lever_site(slug, name, now_utc)
        logger.info(f" -> {name}: found {len(batch)} matching DE roles")
        all_records.extend(batch)

    logger.info(f"Total matching Data Engineering jobs discovered: {len(all_records)}")

    envelope = {
        "metadata": {
            "source_pipeline": "regional_de_ats_ingestion",
            "ingested_at_utc": now_utc,
            "record_count": len(all_records),
            "companies_scanned": [b[1] for b in GREENHOUSE_BOARDS + LEVER_SITES],
        },
        "records": all_records,
    }

    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data", "bronze"))
    os.makedirs(output_dir, exist_ok=True)

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_file = os.path.join(output_dir, f"bronze_jobs_{today_str}.json")

    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(envelope, f, indent=2)

    print(f"\n[SUCCESS] Saved {len(all_records)} jobs to {out_file}")


if __name__ == "__main__":
    main()