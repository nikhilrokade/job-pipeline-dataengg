import hashlib
from datetime import datetime, timezone
import json
import logging
import os
import re
from typing import Any, Dict, List
import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# Target roles for Data Engineering
TARGET_ROLE_KEYWORDS = [
    "data engineer",
    "data platform",
    "pyspark",
    "databricks",
    "big data",
    "etl",
    "data warehouse",
    "analytics engineer",
    "data infrastructure",
]

# Preferred locations
TARGET_LOCATIONS = [
    "navi mumbai",
    "mumbai",
    "thane",
    "pune",
    "india",
    "remote",
    "hybrid",
]

# High-volume Greenhouse companies with India/Remote engineering hubs
GREENHOUSE_TARGETS = [
    "pubmatic",  # Major engineering center in Pune
    "avalara",  # Significant Pune operations
    "elastic",  # Pune & Remote India
    "thoughtworks",  # Pune / Mumbai
    "fractal",  # Mumbai / Pune / Remote
    "databricks",  # India Remote / Bangalore
    "stripe",  # India Remote
    "canonical",  # Global Remote (Hire in India)
    "gitlab",  # 100% Remote India
]

# Lever companies hiring Data Engineers in India
LEVER_TARGETS = [
    "atlan",  # Modern Data Workspace (Remote / India)
    "browserstack",  # Mumbai Tech Hub
    "clevertap",  # Mumbai Data Platform
]


def generate_fingerprint(
    company: str, external_id: str, title: str, location: str
) -> str:
  raw = f"{company.strip().lower()}|{str(external_id).strip()}|{title.strip().lower()}|{location.strip().lower()}"
  return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def is_data_engineering_role(title: str) -> bool:
  title_lower = title.lower()
  return any(kw in title_lower for kw in TARGET_ROLE_KEYWORDS)


def is_target_location(location_name: str) -> bool:
  if not location_name:
    return True
  loc_lower = location_name.lower()
  return any(target in loc_lower for target in TARGET_LOCATIONS)


def fetch_greenhouse_jobs(board_token: str) -> List[Dict[str, Any]]:
  url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
  headers = {"User-Agent": "JobMarketPipeline/2.0 (DataEngineeringTracker)"}
  try:
    resp = requests.get(url, headers=headers, timeout=12)
    if resp.status_code == 200:
      return resp.json().get("jobs", [])
  except Exception as e:
    logger.warning(f"Greenhouse board {board_token} error: {e}")
  return []


def fetch_lever_jobs(site_token: str) -> List[Dict[str, Any]]:
  url = f"https://api.lever.co/v0/postings/{site_token}?mode=json"
  headers = {"User-Agent": "JobMarketPipeline/2.0 (DataEngineeringTracker)"}
  try:
    resp = requests.get(url, headers=headers, timeout=12)
    if resp.status_code == 200:
      return resp.json()
  except Exception as e:
    logger.warning(f"Lever board {site_token} error: {e}")
  return []


def run_targeted_ingestion() -> Dict[str, Any]:
  now_utc = datetime.now(timezone.utc).isoformat()
  matched_records = []

  # 1. Ingest Greenhouse Boards
  for board in GREENHOUSE_TARGETS:
    jobs = fetch_greenhouse_jobs(board)
    for j in jobs:
      title = j.get("title", "")
      loc = j.get("location", {}).get("name", "")
      if is_data_engineering_role(title) and is_target_location(loc):
        fingerprint = generate_fingerprint(board, str(j.get("id")), title, loc)
        matched_records.append({
            "dedup_fingerprint": fingerprint,
            "company_board": board,
            "source": "greenhouse",
            "ingested_at_utc": now_utc,
            "raw_payload": {
                "id": j.get("id"),
                "title": title,
                "company_name": board.capitalize(),
                "location": {"name": loc},
                "absolute_url": j.get("absolute_url"),
                "updated_at": j.get("updated_at") or now_utc,
                "first_published": j.get("first_published") or now_utc,
            },
        })

  # 2. Ingest Lever Boards
  for site in LEVER_TARGETS:
    jobs = fetch_lever_jobs(site)
    for j in jobs:
      title = j.get("text", "")
      loc = j.get("categories", {}).get("location", "Remote")
      if is_data_engineering_role(title) and is_target_location(loc):
        ext_id = j.get("id", "")
        fingerprint = generate_fingerprint(site, ext_id, title, loc)
        matched_records.append({
            "dedup_fingerprint": fingerprint,
            "company_board": site,
            "source": "lever",
            "ingested_at_utc": now_utc,
            "raw_payload": {
                "id": int(hashlib.md5(ext_id.encode()).hexdigest()[:8], 16),
                "title": title,
                "company_name": site.capitalize(),
                "location": {"name": loc},
                "absolute_url": j.get("hostedUrl"),
                "updated_at": now_utc,
                "first_published": now_utc,
            },
        })

  logger.info(
      f"Discovered {len(matched_records)} targeted Data Engineering"
      " opportunities."
  )

  envelope = {
      "metadata": {
          "source_pipeline": "enterprise_ats_ingestion",
          "ingested_at_utc": now_utc,
          "record_count": len(matched_records),
          "companies_scanned": GREENHOUSE_TARGETS + LEVER_TARGETS,
      },
      "records": matched_records,
  }
  return envelope


if __name__ == "__main__":
  data = run_targeted_ingestion()
  today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
  output_dir = os.path.join(
      os.path.dirname(__file__), "..", "..", "data", "bronze"
  )
  os.makedirs(output_dir, exist_ok=True)
  output_file = os.path.join(output_dir, f"bronze_jobs_{today_str}.json")

  with open(output_file, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2)

  print(f"[SUCCESS] Staged {len(data['records'])} matching roles to Bronze.")