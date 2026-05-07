"""Forma — full model properties extraction (no filtering).

Sister script to Forma_GET_Model_Properties.py. Where the existing script
filters down to ELEMENT/REFERENCE categories and groups by Revit Category,
this script keeps EVERY element and writes them as a flat JSON array.

Output: <model>_FullProperties.json — a single JSON document, one element
per array entry, with human-named property keys (lmvId, externalId, plus
all properties Forma returned for that element). No category filter, no
grouping, no shape transformation beyond the same fields-join the existing
script already does.

Why split this out vs. adding a flag to the existing script:
  - The existing script's output shape is load-bearing for L4 reports and
    other downstream consumers. Adding a "raw" mode would mean two output
    contracts in one entry point, which is fragile.
  - This script can be scheduled independently when teams want a raw dump
    for ad-hoc Power BI / Python analysis, without disturbing the L3→L4
    pipeline.

The data-fetching helpers (download_fields, download_properties_to_file,
_read_gzipped_ndjson_file, _build_row, _safe_filename, get_token, etc.)
are imported directly from Forma_GET_Model_Properties — single source of
truth for all the timeout/retry/streaming logic.
"""

import os
import json
import tempfile

# Reuse every helper from the existing extraction module. This keeps the
# streaming download, retry policy, and field/property parsing identical
# across both scripts — fix once, benefit twice.
from Forma_GET_Model_Properties import (
    get_token,
    get_top_folders, find_model_item, get_version_urn,
    get_index_id, poll_index,
    download_fields, download_properties_to_file,
    _read_gzipped_ndjson_file, _build_row, _safe_filename,
)
from Forma_Config import PROJECT_ID, MODEL_ITEM_ID, MODEL_NAME, EXPORTS_DIR


def _write_json_array_streaming(rows_iter, out_path: str) -> int:
    """Write an iterable of dicts as a single JSON array to disk, one row
    at a time. Avoids materialising the full list in memory.

    Returns: number of rows written.

    The output is valid JSON: opens with `[`, separates entries with `,\n`,
    closes with `\n]`. Each element is rendered with json.dumps() with
    ensure_ascii=False so unicode (e.g. UAE place names with diacritics)
    is preserved. We use 2-space indent to keep the file readable in a
    text editor — important since the user's stated reason for choosing
    a single-document format was browser/editor inspection."""
    n = 0
    with open(out_path, "w", encoding="utf-8") as out:
        out.write("[\n")
        for row in rows_iter:
            if n > 0:
                out.write(",\n")
            # Render each row with 2-space indent so the output is readable
            # in a text editor (matching the user's example format). Since
            # each row is nested inside the array, prefix all its newlines
            # with two extra spaces so the whole array is consistently
            # indented at one level.
            block = json.dumps(row, ensure_ascii=False, indent=2)
            indented = block.replace("\n", "\n  ")
            out.write("  ")        # opening indent for the object
            out.write(indented)
            n += 1
        out.write("\n]\n")
    return n


def main():
    if not PROJECT_ID:
        print("[Forma] No active project set. Select a Forma/ACC project in the sidebar first.")
        return 1

    if not MODEL_ITEM_ID and not MODEL_NAME:
        print("[Forma] No model configured. Select a model on the Extractions page first.")
        return 1

    # ── Resolve item ID (same logic as the existing script) ────────────
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
            print(f"Model '{MODEL_NAME}' not found.")
            return 1
        item_id = model_item.get("id")
        print(f"Found item_id: {item_id}")

    # ── Get the index ─────────────────────────────────────────────────
    version_urn = get_version_urn(PROJECT_ID, item_id)
    if not version_urn:
        print("Could not retrieve version URN.")
        return 1
    print(f"Version URN: {version_urn}\n")

    print("Requesting index...")
    index_id, state = get_index_id(PROJECT_ID, version_urn)
    if not index_id:
        print("Could not get index ID.")
        return 1
    print(f"Index ID: {index_id}  (state: {state})\n")

    index_data = poll_index(PROJECT_ID, index_id)
    if not index_data:
        return 1

    fields_url     = index_data.get("fieldsUrl")
    properties_url = index_data.get("propertiesUrl")

    # ── Download fields + properties ──────────────────────────────────
    print("\nDownloading fields...")
    token  = get_token()
    fields = download_fields(fields_url, token)
    print(f"  {len(fields)} fields found")

    print(f"\nProperties URL: {properties_url}")
    print("Downloading properties...")
    properties_tmp = download_properties_to_file(properties_url, token)
    if not properties_tmp:
        print("[Forma] No properties downloaded — aborting before file write.")
        return 1

    # ── Stream every row into a flat JSON array ────────────────────────
    # No category filter, no grouping, no transformation beyond the
    # standard fields-join. This is the user-requested "raw response shape
    # but human-readable" output.
    model_label = _safe_filename(MODEL_NAME or "model")
    out_path    = os.path.join(EXPORTS_DIR, f"{model_label}_FullProperties.json")

    print(f"\nWriting flat JSON array → {os.path.basename(out_path)} …", flush=True)

    row_count   = 0
    bad_lines   = 0
    progress_step = 50000
    next_dot      = progress_step

    def _row_iter():
        """Generator: one flattened row per call, parsed lazily from the
        gzipped ndjson on disk. Skips malformed lines silently (counted)."""
        nonlocal row_count, bad_lines, next_dot
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
            yield _build_row(obj, fields)

    try:
        n_written = _write_json_array_streaming(_row_iter(), out_path)
        suffix = f" ({bad_lines} bad lines skipped)" if bad_lines else ""
        size_mb = os.path.getsize(out_path) / (1024 * 1024)
        print(f" {n_written:,} rows written, {size_mb:.1f} MB.{suffix}", flush=True)
        print(f"Saved {out_path}")
        return 0
    except Exception as e:
        print(f"[Forma] Full-properties write failed: {type(e).__name__}: {e}")
        return 1
    finally:
        try: os.unlink(properties_tmp)
        except Exception: pass


if __name__ == "__main__":
    exit(main())