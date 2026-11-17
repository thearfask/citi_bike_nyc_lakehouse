"""Citi Bike ZIP discovery and extraction. No Spark, Delta or audit-table writes."""
import hashlib
import json
import re
import shutil
import tempfile
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import quote, urlencode
from urllib.request import urlopen
import xml.etree.ElementTree as ET

NS = {"s": "http://s3.amazonaws.com/doc/2006-03-01/"}


def advance_month_config(config_path):
    config_path = Path(config_path)

    with open(config_path, "r") as f:
        config = json.load(f)

    current_month = config["end_month"]

    year, month = map(int, current_month.split("-"))

    if month == 12:
        next_month = f"{year + 1}-01"
    else:
        next_month = f"{year}-{month + 1:02d}"

    config["start_month"] = next_month
    config["end_month"] = next_month

    with open(config_path, "w") as f:
        json.dump(config, f, indent=2)

    print(f"Next extraction month: {next_month}")

def now():
    return datetime.now(timezone.utc).isoformat()


def new_id():
    return str(uuid.uuid4())


def load_config(path="config.json"):
    cfg = dict(path) if isinstance(path, dict) else json.loads(Path(path).read_text(encoding="utf-8"))
    if cfg.get("run_type", "manual") not in {"manual", "auto"}:
        raise ValueError("run_type must be manual or auto")
    if cfg["mode"] not in {"historical", "incremental", "backfill"}:
        raise ValueError("mode must be historical, incremental or backfill")
    start, end = cfg.get("start_month"), cfg.get("end_month")
    for month in (start, end):
        if month:
            if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
                raise ValueError("Months must be YYYY-MM")
    if cfg.get("run_type") == "auto" and cfg.get("force", False):
        raise ValueError("force=true is only allowed for manual runs")
    if cfg["mode"] in {"historical", "backfill"} and not (start and end):
        raise ValueError("historical/backfill requires start_month and end_month")
    if start and end and start > end:
        raise ValueError("start_month cannot exceed end_month")
    if not isinstance(cfg["dry_run"], bool):
        raise ValueError("dry_run must be boolean")
    if not isinstance(cfg.get("force", False), bool):
        raise ValueError("force must be boolean")
    if int(cfg.get("max_files_per_run", 24)) < 1:
        raise ValueError("max_files_per_run must be positive")
    for key in ("source_name", "source_url", "volume_path"):
        if not cfg.get(key):
            raise ValueError(f"Missing config setting: {key}")
    return cfg


def make_key(*parts):
    identity = "|".join(str(part).strip().lower() for part in parts)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def generate_zip_key(source_name, zip_filename):
    return make_key(source_name, PurePosixPath(zip_filename).name)


def generate_source_key(source_name, zip_filename, csv_filename):
    return make_key(source_name, PurePosixPath(zip_filename).name, PurePosixPath(csv_filename).name)


def discover_archives(config):
    """List date-filtered public S3 ZIPs; return small JSON-compatible records."""
    cfg = load_config(config)
    url = cfg["source_url"].rstrip("/") + "/"
    token = None
    archives = []
    months_seen = set()
    while True:
        params = {"list-type": "2", "prefix": cfg.get("s3_prefix", "JC-"), "max-keys": 1000}
        if token:
            params["continuation-token"] = token
        with urlopen(url + "?" + urlencode(params), timeout=90) as response:
            page = ET.fromstring(response.read())
        for obj in page.findall("s:Contents", NS):
            filename = obj.findtext("s:Key", namespaces=NS) or ""
            match = re.fullmatch(cfg.get("zip_pattern", r"JC-(\d{4})(\d{2})-citibike-tripdata(?:\.csv)?\.zip"), filename)
            if not match:
                continue
            month = f"{match[1]}-{match[2]}"
            if (cfg.get("start_month") and month < cfg["start_month"]) or (cfg.get("end_month") and month > cfg["end_month"]):
                continue
            if month in months_seen:
                raise ValueError(f"Multiple ZIP archives found for month {month}")
            months_seen.add(month)
            archives.append({
                "zip_key": generate_zip_key(cfg["source_name"], filename),
                "source_zip_name": filename,
                "source_url": url + quote(filename),
                "data_month": month,
                "zip_size_bytes": int(obj.findtext("s:Size", namespaces=NS) or 0),
            })
        if page.findtext("s:IsTruncated", namespaces=NS) != "true":
            break
        token = page.findtext("s:NextContinuationToken", namespaces=NS)
        if not token:
            raise ValueError("Missing S3 continuation token")
    return sorted(archives, key=lambda row: row["source_zip_name"])


def audit_event(batch_id, zip_info, stage, status, started_at, *, target_path=None,
                size_bytes=None, file_count=None, source_key=None, error_message=None):
    return {
        "event_id": new_id(), "batch_id": batch_id,
        "zip_key": zip_info["zip_key"], "source_key": source_key,
        "stage": stage, "status": status,
        "source_zip_name": zip_info["source_zip_name"],
        "source_url": zip_info["source_url"],
        "data_month": zip_info["data_month"],
        "target_path": target_path, "size_bytes": size_bytes,
        "file_count": file_count, "started_at": started_at,
        "finished_at": now(), "error_message": error_message,
    }


def extract_archives(config, approved_zips, batch_id):
    """Download approved ZIPs, publish CSVs and return audit JSON. No table writes."""
    cfg = load_config(config)
    approved_zips = list(approved_zips)
    if len(approved_zips) > int(cfg.get("max_files_per_run", 24)):
        raise ValueError("Too many approved ZIPs; narrow dates or increase max_files_per_run")
    result = {"batch_id": batch_id, "status": "DRY_RUN" if cfg["dry_run"] else "SUCCESS",
              "audit_events": [], "audit_files": [], "error_message": None}
    if cfg["dry_run"]:
        return result
    root = Path(cfg["volume_path"])
    for item in approved_zips:
        zip_name = item["source_zip_name"]
        expected_key = generate_zip_key(cfg["source_name"], zip_name)
        if expected_key != item["zip_key"]:
            raise ValueError(f"ZIP key mismatch: {zip_name}")
        folder = root / item["data_month"].replace("-", "") / batch_id
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "source.zip"
            started = now()
            try:
                with urlopen(item["source_url"], timeout=120) as response:
                    with zip_path.open("wb") as output:
                        shutil.copyfileobj(response, output, 1024 * 1024)
                size = zip_path.stat().st_size
                if size != item["zip_size_bytes"]:
                    raise ValueError(f"Incomplete or changed ZIP download: {zip_name}")
                result["audit_events"].append(audit_event(
                    batch_id, item, "DOWNLOAD", "SUCCESS", started,
                    target_path=str(zip_path), size_bytes=size))
            except Exception as error:
                result["audit_events"].append(audit_event(
                    batch_id, item, "DOWNLOAD", "FAILED", started,
                    target_path=str(zip_path), error_message=str(error)))
                result.update(status="FAILED", error_message=str(error))
                return result

            started = now()
            try:
                extracted = []
                with zipfile.ZipFile(zip_path) as archive:
                    members = [m for m in archive.infolist() if not m.is_dir()
                               and m.filename.lower().endswith(".csv")
                               and "__MACOSX" not in m.filename]
                    names = [PurePosixPath(m.filename).name for m in members]
                    if not members or len(names) != len(set(names)):
                        raise ValueError("Missing CSV or duplicate CSV names")
                    if sum(m.file_size for m in members) > 2 * 1024**3:
                        raise ValueError("Extracted data exceeds 2 GiB limit")
                    # Extract to temp first; ZipFile validates CRC on full read.
                    for member, name in zip(members, names):
                        local = Path(tmp) / new_id()
                        with archive.open(member) as source, local.open("wb") as output:
                            shutil.copyfileobj(source, output, 1024 * 1024)
                        extracted.append((name, local))

                folder.mkdir(parents=True, exist_ok=True)
                for name, _ in extracted:
                    if (folder / name).exists():
                        raise ValueError(f"CSV already exists in batch landing folder: {folder / name}")
                records = []
                for name, local in extracted:
                    target = folder / name
                    with local.open("rb") as source, target.open("wb") as output:
                        shutil.copyfileobj(source, output, 1024 * 1024)
                    size = target.stat().st_size
                    if size != local.stat().st_size:
                        raise ValueError(f"Incomplete CSV copy: {target}")
                    records.append({
                        "file_id": new_id(), "batch_id": batch_id,
                        "zip_key": item["zip_key"],
                        "source_key": generate_source_key(cfg["source_name"], zip_name, name),
                        "source_zip_name": zip_name, "source_url": item["source_url"],
                        "data_month": item["data_month"], "csv_name": name,
                        "csv_path": str(target), "file_size_bytes": size,
                        "extracted_at": now(),
                    })
                # Only report files if every CSV in this ZIP has been copied.
                unzip_event = audit_event(
                    batch_id, item, "UNZIP", "SUCCESS", started,
                    target_path=str(folder), size_bytes=sum(r["file_size_bytes"] for r in records),
                    file_count=len(records))
                for record in records:
                    record["extraction_event_id"] = unzip_event["event_id"]
                # Publish audit JSON only after every CSV in this ZIP succeeds.
                result["audit_files"].extend(records)
                result["audit_events"].append(unzip_event)
                for record in records:
                    result["audit_events"].append(audit_event(
                        batch_id, item, "CSV_EXTRACT", "SUCCESS", started,
                        target_path=record["csv_path"],
                        size_bytes=record["file_size_bytes"], file_count=1,
                        source_key=record["source_key"],
                    ))
            except Exception as error:
                result["audit_events"].append(audit_event(
                    batch_id, item, "UNZIP", "FAILED", started,
                    target_path=str(folder), error_message=str(error)))
                result.update(status="FAILED", error_message=str(error))
                return result
    return result
