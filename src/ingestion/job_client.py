import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
import requests


class PublicJobIngestor:
    """Ingests real Data Engineering postings from public company ATS endpoints into Bronze staging."""

    COMPANIES = [
        "gitlab",
        "canonical",
        "elastic",
        "cloudflare",
        "databricks",
        "airbnb",
        "stripe",
        "reddit",
        "pinterest",
        "cockroachlabs",
        "mongodb"
        ]

    def __init__(self, output_dir: str = "data/bronze"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = requests.Session()

    def _generate_fingerprint(self, company: str, title: str, location: str) -> str:
        """Deterministic SHA-256 hash to deduplicate jobs across daily pipeline runs."""
        raw_key = f"{company.strip().lower()}|{title.strip().lower()}|{location.strip().lower()}"
        return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

    def fetch_company_jobs(self, company: str) -> List[Dict[str, Any]]:
        """Fetches public postings directly from official Greenhouse ATS feed."""
        url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            return data.get("jobs", [])
        except requests.exceptions.RequestException as exc:
            print(f"[WARN] Failed to fetch jobs for {company}: {exc}")
            return []

    def run_ingestion(self) -> Path:
        """Collects postings, filters for Data/Python roles, and stages to Bronze layer."""
        ingested_at = datetime.now(timezone.utc).isoformat()
        all_raw_jobs: List[Dict[str, Any]] = []

        target_role_patterns = [
            "data engineer",
            "data platform",
            "data infrastructure",
            "analytics engineer",
            "pyspark",
            "big data",
            "etl",
            "data pipeline",
            "python developer",
            "database engineer",
        ]

        print("[INFO] Starting raw ingestion from public endpoints...")
        for comp in self.COMPANIES:
            jobs = self.fetch_company_jobs(comp)
            print(f"[INFO] Fetched {len(jobs)} total openings from {comp.title()}")
            for j in jobs:
                title = j.get("title", "").lower()
                if any(role in title for role in target_role_patterns):
                    loc = j.get("location", {}).get("name", "Unknown")
                    fingerprint = self._generate_fingerprint(comp, j.get("title", ""), loc)
                    all_raw_jobs.append({
                        "dedup_fingerprint": fingerprint,
                        "company_board": comp,
                        "source": "greenhouse_public_ats",
                        "ingested_at_utc": ingested_at,
                        "raw_payload": j,
                    })

        batch_envelope = {
            "metadata": {
                "source_pipeline": "job_market_bronze_ingestion",
                "ingested_at_utc": ingested_at,
                "record_count": len(all_raw_jobs),
                "companies_scanned": self.COMPANIES,
            },
            "records": all_raw_jobs,
        }

        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        output_file = self.output_dir / f"bronze_jobs_{date_str}.json"

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(batch_envelope, f, indent=2, ensure_ascii=False)

        print(f"[SUCCESS] Staged {len(all_raw_jobs)} matching records into: {output_file}")
        return output_file


if __name__ == "__main__":
    ingestor = PublicJobIngestor()
    ingestor.run_ingestion()