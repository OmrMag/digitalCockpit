import requests
import json
import os
import time
import gzip
import tempfile

from Forma_MasterScript import get_3legged_token as _get_3legged_token, get_token as get_2legged_token
from Forma_Config import ACCOUNT_ID, PROJECT_ID, MODEL_ITEM_ID, MODEL_NAME, EXPORTS_DIR

BASE_URL  = "https://developer.api.autodesk.com"
INDEX_URL = f"{BASE_URL}/construction/index/v2"

# ── Download tuning ─────────────────────────────────────────────────────────
# CONNECT_TIMEOUT: how long to wait for the TCP/TLS handshake before giving up.
# READ_TIMEOUT:    inactivity timeout — how long we'll wait between bytes
#                  before assuming the connection is dead. The S3 download was
#                  hanging silently for ~6 minutes before the cockpit's outer
#                  layer killed it; with a per-chunk read timeout we detect
#                  staleness ourselves and can retry cleanly.
# CHUNK_SIZE:      bytes streamed per iteration. 1 MB strikes a balance
#                  between memory footprint and syscall/loop overhead for
#                  100MB+ properties files.
# PROGRESS_EVERY:  print a dot every N MB so the cockpit terminal shows that
#                  the download is alive without flooding the log.
CONNECT_TIMEOUT = 30
READ_TIMEOUT    = 60
CHUNK_SIZE      = 1024 * 1024          # 1 MB
PROGRESS_EVERY  = 5 * 1024 * 1024      # dot per 5 MB

# Retry policy (conservative — 3 attempts, longer waits)
RETRY_ATTEMPTS  = 3
RETRY_BACKOFF   = [5, 15, 30]          # seconds before attempt 2, 3, 4


# ── Output shaping ──────────────────────────────────────────────────────────
# Categories whose full element data we keep in the grouped output.
# Everything else from Forma is counted (in *_RawCounts.json) but the row
# data is dropped to keep the main output file manageable. This list mirrors
# Proccesed_Model_Properties.ELEMENT_CATEGORIES; if you change one, change
# the other too. Project Information and Document are kept as well so the
# downstream report can read project metadata from the same file.
KEPT_CATEGORIES = {
    "Revit Walls", "Revit Floors", "Revit Roofs", "Revit Ceilings",
    "Revit Doors", "Revit Windows", "Revit Curtain Panels",
    "Revit Curtain Wall Mullions", "Revit Curtain Systems",
    "Revit Stairs", "Revit Railings", "Revit Ramps",
    "Revit Landings", "Revit Runs", "Revit Columns",
    "Revit Generic Models", "Revit Mass", "Revit Topography",
    "Revit Parking", "Revit Planting", "Revit Site",
    "Revit Slab Edges", "Revit Shaft Openings",
    "Revit Rectangular Straight Wall Opening",
    "Revit Structural opening cut",
    "Revit Structural Framing", "Revit Structural Columns",
    "Revit Structural Foundations", "Revit Structural Beam Systems",
    "Revit Structural Trusses", "Revit Structural Connections",
    "Revit Structural Fabric Reinforcement", "Revit Structural Rebar",
    "Revit Structural Path Reinforcement",
    "Revit Mechanical Equipment", "Revit Air Terminals",
    "Revit Ducts", "Revit Duct Fittings", "Revit Duct Accessories",
    "Revit Flex Ducts", "Revit Pipes", "Revit Pipe Fittings",
    "Revit Pipe Accessories", "Revit Flex Pipes",
    "Revit Plumbing Fixtures", "Revit Sprinklers",
    "Revit Electrical Equipment", "Revit Electrical Fixtures",
    "Revit Lighting Fixtures", "Revit Lighting Devices",
    "Revit Fire Alarm Devices", "Revit Communication Devices",
    "Revit Data Devices", "Revit Security Devices",
    "Revit Cable Trays", "Revit Conduits",
    "Revit Furniture", "Revit Furniture Systems", "Revit Casework",
    "Revit Specialty Equipment", "Revit Assemblies", "Revit Parts",
    # Project metadata
    "Revit Project Information", "Revit Document",
}


def _safe_filename(name: str) -> str:
    """Strip filesystem-unsafe characters from a model name. Keeps spaces,
    parens, hyphens, dots — strips slashes, colons, quotes, etc."""
    if not name:
        return "model"
    # Drop the .rvt/.ifc/.nwc/.nwd extension if present
    base, ext = os.path.splitext(name)
    if ext.lower() in (".rvt", ".ifc", ".nwc", ".nwd"):
        name = base
    bad = '/\\:*?"<>|\r\n\t'
    for ch in bad:
        name = name.replace(ch, "_")
    # Collapse runs of underscores / spaces
    while "  " in name: name = name.replace("  ", " ")
    while "__" in name: name = name.replace("__", "_")
    return name.strip(" _.") or "model"


def get_token():
    """Get best available token: 3-legged if cached, otherwise 2-legged."""
    try:
        return _get_3legged_token()
    except Exception as e:
        print(f"[Forma] 3-legged token unavailable ({e}), falling back to 2-legged.")
        return get_2legged_token()


def get_top_folders(project_id):
    token = get_token()
    url = f"{BASE_URL}/project/v1/hubs/b.{ACCOUNT_ID}/projects/b.{project_id}/topFolders"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if response.status_code == 200:
        return response.json().get("data", [])
    print(f"[Forma] Error getting top folders: {response.status_code} {response.text}")
    return []


def get_folder_contents(project_id, folder_id):
    token = get_token()
    url = f"{BASE_URL}/data/v1/projects/b.{project_id}/folders/{folder_id}/contents"
    headers = {"Authorization": f"Bearer {token}"}
    all_items = []
    page = 0
    while True:
        params = {"page[number]": page, "page[limit]": 200}
        response = requests.get(url, headers=headers, params=params,
                                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if response.status_code == 200:
            data = response.json()
            batch = data.get("data", [])
            if not batch:
                break
            all_items.extend(batch)
            if not data.get("links", {}).get("next"):
                break
            page += 1
        else:
            break
    return all_items


def find_model_item(project_id, folder_id, model_name):
    """Recursively search folders for a file matching model_name."""
    contents = get_folder_contents(project_id, folder_id)
    for item in contents:
        item_type = item.get("type")
        attrs     = item.get("attributes", {})
        name      = attrs.get("displayName") or attrs.get("name", "")

        if item_type == "folders":
            result = find_model_item(project_id, item.get("id"), model_name)
            if result:
                return result
        elif item_type == "items":
            clean_name = name.rsplit(".", 1)[0] if "." in name else name
            if clean_name.lower() == model_name.lower():
                return item
    return None


def get_version_urn(project_id, item_id):
    token = get_token()
    url = f"{BASE_URL}/data/v1/projects/b.{project_id}/items/{item_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if response.status_code == 200:
        return (response.json()
                        .get("data", {})
                        .get("relationships", {})
                        .get("tip", {})
                        .get("data", {})
                        .get("id"))
    print(f"[Forma] Error getting version URN: {response.status_code} {response.text}")
    return None


def get_index_id(project_id, version_urn):
    token = get_token()
    url = f"{INDEX_URL}/projects/{project_id}/indexes:batch-status"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    body = {"versions": [{"versionUrn": version_urn}]}
    response = requests.post(url, headers=headers, json=body,
                             timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    if response.status_code == 200:
        indexes = response.json().get("indexes", [])
        if indexes:
            return indexes[0].get("indexId"), indexes[0].get("state")
    print(f"[Forma] Error getting index: {response.status_code} {response.text}")
    return None, None


def poll_index(project_id, index_id):
    token = get_token()
    url = f"{INDEX_URL}/projects/{project_id}/indexes/{index_id}"
    headers = {"Authorization": f"Bearer {token}"}

    print("  Waiting for index to be ready", end="", flush=True)
    while True:
        response = requests.get(url, headers=headers,
                                timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
        if response.status_code == 200:
            data  = response.json()
            state = data.get("state")
            print(".", end="", flush=True)
            if state == "FINISHED":
                print(" done!")
                return data
            elif state == "FAILED":
                print(f" failed! {data.get('errors')}")
                return None
        time.sleep(5)


# ── Streaming downloader ────────────────────────────────────────────────────
# The previous implementation used `requests.get(...).content` which loads
# the full response into memory before returning. For a model with 3000+
# fields the properties.ldjson.gz is large enough to either OOM the B1
# Azure tier or stall long enough for an upstream proxy to drop the
# connection. The mid-download "network error" you saw was almost certainly
# a connection idle-timeout fired by Azure's outbound NAT / S3.
#
# This streamed version:
#   - downloads to a temp file in CHUNK_SIZE pieces (constant memory)
#   - per-chunk read_timeout means stalls fail fast, not after 6 minutes
#   - retries the whole download up to RETRY_ATTEMPTS times with backoff
#   - prints a dot per PROGRESS_EVERY bytes so it's obvious it's progressing
#   - decompresses by line from the temp file using gzip.open() — keeps
#     memory low even when the parsed list of objects ends up large

def _stream_to_tempfile(url, headers, label):
    """Stream `url` to a temp .gz file with retries; return the path.

    Caller is responsible for deleting the temp file after parsing."""
    last_err = None
    for attempt in range(1, RETRY_ATTEMPTS + 1):
        try:
            with requests.get(url, headers=headers, stream=True, allow_redirects=True,
                              timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)) as response:
                if response.status_code != 200:
                    print(f"[Forma] Error downloading {label}: HTTP {response.status_code}")
                    if 500 <= response.status_code < 600:
                        # Server error → retry. Other codes (4xx) are unlikely
                        # to fix themselves; bail out immediately.
                        last_err = f"HTTP {response.status_code}"
                        continue
                    return None

                total_bytes = int(response.headers.get("Content-Length", 0) or 0)
                if total_bytes:
                    print(f"  Size: {total_bytes / (1024*1024):.1f} MB", flush=True)
                else:
                    print(f"  Size: unknown (no Content-Length)", flush=True)

                fd, tmp_path = tempfile.mkstemp(prefix="forma_", suffix=".gz")
                bytes_written = 0
                next_dot      = PROGRESS_EVERY
                print("  Downloading", end="", flush=True)
                try:
                    with os.fdopen(fd, "wb") as out:
                        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                            if not chunk:
                                continue
                            out.write(chunk)
                            bytes_written += len(chunk)
                            while bytes_written >= next_dot:
                                print(".", end="", flush=True)
                                next_dot += PROGRESS_EVERY
                    print(f" done ({bytes_written / (1024*1024):.1f} MB)")
                    return tmp_path
                except Exception as e:
                    # Partial download — clean up the temp file so we don't
                    # leak it on the worker, then fall through to retry.
                    try: os.unlink(tmp_path)
                    except Exception: pass
                    raise
        except (requests.exceptions.RequestException,
                requests.exceptions.ChunkedEncodingError,
                requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout) as e:
            last_err = f"{type(e).__name__}: {e}"
            print(f"\n  ✗ Attempt {attempt}/{RETRY_ATTEMPTS} failed: {last_err}", flush=True)

        if attempt < RETRY_ATTEMPTS:
            wait = RETRY_BACKOFF[min(attempt - 1, len(RETRY_BACKOFF) - 1)]
            print(f"  Retrying in {wait}s...", flush=True)
            time.sleep(wait)

    print(f"[Forma] All {RETRY_ATTEMPTS} download attempts failed for {label}. Last error: {last_err}")
    return None


def _read_gzipped_ndjson_file(path):
    """Yield decoded lines from a .gz file or a plain text file."""
    # Try gzipped first; if magic bytes don't match, fall back to plain text.
    with open(path, "rb") as probe:
        magic = probe.read(2)
    is_gz = (magic == b"\x1f\x8b")
    opener = (lambda p: gzip.open(p, "rt", encoding="utf-8")) if is_gz else \
             (lambda p: open(p, "r", encoding="utf-8"))
    with opener(path) as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


def download_fields(fields_url, token):
    is_s3 = "s3.amazonaws.com" in fields_url
    headers = {} if is_s3 else {"Authorization": f"Bearer {token}"}
    tmp_path = _stream_to_tempfile(fields_url, headers, "fields")
    if not tmp_path:
        return {}
    fields = {}
    try:
        for line in _read_gzipped_ndjson_file(tmp_path):
            try:
                field = json.loads(line)
                fields[field["key"]] = field.get("name", field["key"])
            except (json.JSONDecodeError, KeyError):
                continue
    finally:
        try: os.unlink(tmp_path)
        except Exception: pass
    return fields


def download_properties_to_file(properties_url, token):
    """Download the properties ndjson to a local temp file and return its path.

    Returns None on failure. Caller is responsible for deleting the file.

    Note: we deliberately do NOT parse into a list here. For models with
    thousands of fields the decompressed ndjson can be several hundred MB,
    and turning that into a Python list of dicts blows past the B1 tier's
    1.75 GB RAM. Callers should iterate the file with
    _read_gzipped_ndjson_file() instead.
    """
    is_s3 = "s3.amazonaws.com" in properties_url
    headers = {} if is_s3 else {"Authorization": f"Bearer {token}"}
    return _stream_to_tempfile(properties_url, headers, "properties")


def _build_row(obj, fields):
    """Convert one parsed properties object into a flat row dict."""
    row = {
        "lmvId":      obj.get("lmvId"),
        "externalId": obj.get("externalId"),
    }
    for key, value in (obj.get("props") or {}).items():
        row[fields.get(key, key)] = value
    return row


# Backward-compat shim — kept so any other caller of download_properties
# continues working. Internally it now reads from disk and parses lazily.
def download_properties(properties_url, token):
    """Compat wrapper. Prefer download_properties_to_file + streaming reads
    for large models — this materialises the entire list in RAM."""
    tmp_path = download_properties_to_file(properties_url, token)
    if not tmp_path:
        return []
    try:
        return [json.loads(line) for line in _read_gzipped_ndjson_file(tmp_path)
                if line]
    finally:
        try: os.unlink(tmp_path)
        except Exception: pass


if __name__ == "__main__":
    if not PROJECT_ID:
        print("[Forma] No active project set. Select a Forma/ACC project in the sidebar first.")
        exit(1)

    if not MODEL_ITEM_ID and not MODEL_NAME:
        print("[Forma] No model configured. Select a model on the Extractions page first.")
        exit(1)

    if MODEL_ITEM_ID:
        item_id = MODEL_ITEM_ID
        print(f"Using item ID from config: {item_id}")
    else:
        print(f"Searching for '{MODEL_NAME}' in project {PROJECT_ID}...\n")
        top_folders = get_top_folders(PROJECT_ID)
        model_item  = None
        for folder in top_folders:
            model_item = find_model_item(PROJECT_ID, folder.get("id"), MODEL_NAME)
            if model_item:
                break

        if not model_item:
            print(f"Model '{MODEL_NAME}' not found. Check the name in Forma_Config.py and try again.")
            exit()

        item_id = model_item.get("id")
        print(f"Found: {model_item['attributes'].get('displayName')}  (item_id: {item_id})")

    version_urn = get_version_urn(PROJECT_ID, item_id)
    if not version_urn:
        print("Could not retrieve version URN.")
        exit()
    print(f"Version URN: {version_urn}\n")

    print("Requesting index...")
    index_id, state = get_index_id(PROJECT_ID, version_urn)
    if not index_id:
        print("Could not get index ID.")
        exit()
    print(f"Index ID: {index_id}  (state: {state})\n")

    index_data = poll_index(PROJECT_ID, index_id)
    if not index_data:
        exit()

    fields_url     = index_data.get("fieldsUrl")
    properties_url = index_data.get("propertiesUrl")

    print("\nDownloading fields...")
    token  = get_token()
    fields = download_fields(fields_url, token)
    print(f"  {len(fields)} fields found")

    print(f"\nProperties URL: {properties_url}")
    print("Downloading properties...")
    properties_tmp = download_properties_to_file(properties_url, token)
    if not properties_tmp:
        print("[Forma] No properties downloaded — aborting before file write.")
        exit(1)

    # ── Categorized streaming write ────────────────────────────────────
    # Output layout: a single grouped JSON keyed by Revit Category, with
    # the full row data only for categories in KEPT_CATEGORIES. Every
    # category Forma returns is also tallied into a small *_RawCounts.json
    # side file so the downstream report (L4) can show the full picture
    # — including categories whose row data we dropped.
    #
    # Implementation: we open one ndjson temp file per kept category as
    # rows arrive (lazy), append rows to those temp files line by line,
    # then stitch them into the final grouped JSON at the end. Memory
    # stays bounded at one in-flight row plus a small fd map; disk holds
    # the data instead of RAM.

    # Resolve a model name to use as the output prefix. Prefer the live
    # MODEL_NAME from config; if that's missing (item_id was supplied
    # without name), use the displayName we resolved during folder search.
    model_label = MODEL_NAME or "model"
    model_label = _safe_filename(model_label)

    grouped_path     = os.path.join(EXPORTS_DIR, f"{model_label}.json")
    raw_counts_path  = os.path.join(EXPORTS_DIR, f"{model_label}_RawCounts.json")

    print(f"Writing categorized JSON → {os.path.basename(grouped_path)} …", flush=True)

    row_count       = 0
    kept_count      = 0
    bad_lines       = 0
    progress_step   = 50000
    next_dot        = progress_step

    category_counts = {}                # name -> int — every Category seen
    category_files  = {}                # name -> open temp file handle (kept categories only)
    category_paths  = {}                # name -> temp file path
    category_first  = {}                # name -> bool, True until first row written

    def _open_category_tmp(cat):
        """Lazily open a temp file for a kept category. Returns the open fh."""
        if cat in category_files:
            return category_files[cat]
        fd, path = tempfile.mkstemp(prefix=f"forma_cat_", suffix=".ndjson")
        fh = os.fdopen(fd, "w", encoding="utf-8")
        category_files[cat] = fh
        category_paths[cat] = path
        category_first[cat] = True
        return fh

    try:
        for line in _read_gzipped_ndjson_file(properties_tmp):
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                bad_lines += 1
                continue

            row_count += 1
            if row_count >= next_dot:
                print(".", end="", flush=True)
                next_dot += progress_step

            row = _build_row(obj, fields)
            cat = row.get("Category", "<no Category>")
            category_counts[cat] = category_counts.get(cat, 0) + 1

            if cat in KEPT_CATEGORIES:
                fh = _open_category_tmp(cat)
                # Append one ndjson line per row — cheap, no JSON-array
                # bookkeeping until we stitch at the end.
                fh.write(json.dumps(row, ensure_ascii=False))
                fh.write("\n")
                kept_count += 1

        # Close all the per-category temp files so we can read them back.
        for fh in category_files.values():
            fh.close()

        suffix = f" ({bad_lines} bad lines skipped)" if bad_lines else ""
        print(f" {row_count:,} rows scanned, {kept_count:,} kept across "
              f"{len(category_files)} categories.{suffix}", flush=True)

        # ── Stitch ndjson temp files into one grouped JSON ─────────────
        # We write `{` then for each kept category that has rows, write
        # `"<cat>": [`, stream the temp file's lines as JSON array members,
        # then `]`. This pass touches each row exactly once more — there's
        # no parsing here, just text concatenation.
        print("Stitching grouped JSON…", flush=True)
        with open(grouped_path, "w", encoding="utf-8") as out:
            out.write("{\n")
            cat_names = sorted(category_paths.keys())
            for ci, cat in enumerate(cat_names):
                tmp_path = category_paths[cat]
                first_in_cat = True
                # JSON-encode the category name as a key
                out.write(json.dumps(cat))
                out.write(": [\n")
                with open(tmp_path, "r", encoding="utf-8") as src:
                    for ln in src:
                        ln = ln.rstrip("\n")
                        if not ln:
                            continue
                        if first_in_cat:
                            out.write("    ")
                            first_in_cat = False
                        else:
                            out.write(",\n    ")
                        out.write(ln)
                out.write("\n]")
                if ci < len(cat_names) - 1:
                    out.write(",")
                out.write("\n")
            out.write("}\n")
        print(f"Saved {grouped_path}")

        # ── Raw counts side artifact ────────────────────────────────────
        # Sorted by count desc so it's easy to eyeball top categories.
        ordered = dict(sorted(category_counts.items(), key=lambda x: -x[1]))
        with open(raw_counts_path, "w", encoding="utf-8") as f:
            json.dump({
                "model_name":        MODEL_NAME or "",
                "total_objects":     row_count,
                "kept_objects":      kept_count,
                "unique_categories": len(category_counts),
                "kept_categories":   len(category_files),
                "counts":            ordered,
            }, f, indent=2, ensure_ascii=False)
        print(f"Saved {raw_counts_path}")

        exit_code = 0
    except Exception as e:
        print(f"[Forma] Categorized write failed: {type(e).__name__}: {e}")
        exit_code = 1
    finally:
        # Clean up per-category temp files and the source download
        for path in category_paths.values():
            try: os.unlink(path)
            except Exception: pass
        try: os.unlink(properties_tmp)
        except Exception: pass
    exit(exit_code)