"""
Proccesed_Model_Properties.py

L4 reporting against the categorized output of Forma_GET_Model_Properties.

Inputs:
  EXPORTS_DIR/<ModelName>.json
      Grouped JSON keyed by Revit Category. Only the kept categories
      (KEPT_CATEGORIES in L3) carry full element rows; everything else
      Forma returned was counted but not preserved row-wise.
  EXPORTS_DIR/<ModelName>_RawCounts.json
      {category_name: count} for every category Forma returned, plus
      total/kept tallies. This is the source of truth for category
      counts in the report — including categories L3 dropped.

Outputs:
  EXPORTS_DIR/<ModelName>_Report.json
      Health summary (Project Info, Total Elements, Sheets, Views,
      Schedules, Worksets, suspicious flags, demolished, missing-level).
      Same content as the previous forma_model_report.json, just
      renamed and using the categorized data as input.
  EXPORTS_DIR/<ModelName>_Aggregate.json
      Convenience pass-through of the RawCounts data, exposed as a
      named L4 output so the cockpit can present it as a deliverable.

Memory: never calls json.load() on the big grouped file. Streams it
category-by-category using a small line-based scanner (the L3 writer
emits one element per line, so this works reliably). Peak memory is
bounded by one in-flight element plus the report accumulators.
"""

import json
import os
from collections import defaultdict

from Forma_Config import MODEL_NAME, EXPORTS_DIR


# ── Same set as KEPT_CATEGORIES in L3 (without project metadata, since
# we treat that separately for reporting). Kept here so suspicious /
# no-level / demolished checks know which categories to inspect.
ELEMENT_CATEGORIES = {
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
}

NO_LEVEL_EXEMPT = {
    "Revit Topography", "Revit Site", "Revit Mass",
    "Revit Shaft Openings", "Revit Structural Foundations",
}

QUALITY_FLAG_KEYWORDS = [
    "void", "cut rect", "cut trench", "opening", "jumpform",
    "temporary", "temp ", "formwork", "builder",
]


def _safe_filename(name: str) -> str:
    """Mirror of L3's _safe_filename. Keep these in sync."""
    if not name:
        return "model"
    base, ext = os.path.splitext(name)
    if ext.lower() in (".rvt", ".ifc", ".nwc", ".nwd"):
        name = base
    bad = '/\\:*?"<>|\r\n\t'
    for ch in bad:
        name = name.replace(ch, "_")
    while "  " in name: name = name.replace("  ", " ")
    while "__" in name: name = name.replace("__", "_")
    return name.strip(" _.") or "model"


def is_suspicious(obj):
    rfn = (obj.get("_RFN") or obj.get("Type Name") or obj.get("name") or "").lower()
    return any(kw in rfn for kw in QUALITY_FLAG_KEYWORDS)


def is_real_workset(ws):
    if not ws:
        return False
    if ws.startswith(('+Hide_', '+Link_', '_User_')):
        return True
    skip_starts = (
        'View "', 'View Template', 'Family  :', 'Elevation "',
        'Work Plane', 'Panel Schedule', 'Site ', 'Pipe ',
        'Duct ', 'Cable ', 'Conduit ', 'Wire ', 'MEP ',
    )
    if any(ws.startswith(p) for p in skip_starts):
        return False
    skip_keywords = (
        'Settings', 'Types', 'Patterns', 'Styles', 'Parameters',
        'Information', 'Classifications', 'Schemes', 'Warnings',
        'Sizes', 'Weights', 'Units', 'Tracker', 'Configuration',
        'Manager', 'Planes', 'Sets', 'Templates', 'Definitions',
        'Sequences', 'Computations', 'Reactions', 'Assembly',
        'Fabrication', 'Circuit', 'Keynoting', 'Reinforcement',
        'Halftone', 'Worksharing', 'Multiple', 'Coordination',
        'Route', 'Default Divide', 'Assembly Code', 'Data Storage',
        'Tracking', 'Demand Factor',
    )
    if any(kw in ws for kw in skip_keywords):
        return False
    skip_exact = {
        'Filters', 'Project Info', 'Reviewable Warnings', 'Object Styles',
        'Loaded Family Information', 'Filled Region Types',
        'Line Patterns', 'Line Styles', 'Shared Parameters',
    }
    if ws in skip_exact:
        return False
    return True


# ── Streaming reader for L3's categorized JSON ──────────────────────────────
# The format L3 writes is:
#   {
#   "<Category>": [
#       <one element JSON object per line>,
#       <one element JSON object per line>
#   ],
#   "<Next Category>": [
#       ...
#   ]
#   }
# Knowing the exact line layout makes the reader trivial: detect
# category headers by line shape, then parse each subsequent indented
# JSON object until we hit a line with `]`. We never have more than
# one element in memory at a time.

def iter_categorized(path):
    """Yield (category_name, element_dict) pairs. Streams the file.

    Robust against:
      - Extra whitespace
      - Trailing commas on objects
      - Long lines (some properties values are huge)
    Not robust against arbitrary JSON formatting — relies on L3's
    one-element-per-line layout. If L3 ever changes its writer, this
    needs to change too.
    """
    current_cat = None
    decoder = json.JSONDecoder()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped in ("{", "}"):
                continue
            # Strip trailing comma (objects use ',\n    ' between rows;
            # category headers also can have trailing commas after `]`)
            if stripped.endswith(","):
                stripped = stripped[:-1].rstrip()
            # Category header? Looks like:    "Revit Walls": [
            if stripped.endswith(": ["):
                # Extract the quoted category name with raw_decode
                try:
                    name, _ = decoder.raw_decode(stripped)
                    current_cat = name
                except (json.JSONDecodeError, ValueError):
                    current_cat = None
                continue
            # End of category array
            if stripped.startswith("]"):
                current_cat = None
                continue
            if current_cat is None:
                continue   # safety: skip unexpected content
            try:
                obj = json.loads(stripped)
            except json.JSONDecodeError:
                continue
            yield current_cat, obj


if __name__ == "__main__":
    if not MODEL_NAME:
        print("[Forma] MODEL_NAME not set in config. Select a model on the Extractions page first.")
        exit(1)

    model_label = _safe_filename(MODEL_NAME)
    raw_path         = os.path.join(EXPORTS_DIR, f"{model_label}.json")
    raw_counts_path  = os.path.join(EXPORTS_DIR, f"{model_label}_RawCounts.json")
    report_path      = os.path.join(EXPORTS_DIR, f"{model_label}_Report.json")
    aggregate_path   = os.path.join(EXPORTS_DIR, f"{model_label}_Aggregate.json")

    if not os.path.exists(raw_path):
        print(f"[Forma] Source file not found: {raw_path}")
        print("[Forma] Run the L3 Model Properties extraction first.")
        exit(1)

    print(f"Model:  {MODEL_NAME}")
    print(f"Source: {raw_path} ({os.path.getsize(raw_path) / (1024*1024):,.1f} MB)")

    # ── Load the small RawCounts file (always small, safe to json.load) ─
    raw_counts = {}
    if os.path.exists(raw_counts_path):
        with open(raw_counts_path, "r", encoding="utf-8") as f:
            raw_counts = json.load(f)
        print(f"RawCounts: {raw_counts.get('total_objects', 0):,} objects, "
              f"{raw_counts.get('unique_categories', 0)} unique categories")
    else:
        print(f"[Forma] Warning: {raw_counts_path} not found. Aggregate will be derived from kept data only.")

    # All-category counts come from RawCounts (includes Views/Sheets/etc
    # that L3 dropped row-wise). If RawCounts is missing, fall back to
    # whatever the streamed pass yields (kept categories only).
    category_count = dict(raw_counts.get("counts", {}))

    # ── Stream the categorized data, building report accumulators ───────
    print("Scanning categorized data…", flush=True)
    project_info     = {}
    suspicious       = []
    level_count      = defaultdict(int)
    workset_count    = defaultdict(int)
    no_level_count   = defaultdict(int)
    demolished_count = defaultdict(int)
    filtered_cat_count = defaultdict(int)   # ELEMENT_CATEGORIES only
    # Three-level breakdown for the Aggregate output:
    #   category_breakdown[category][family_name][type_name] = count
    # Family name comes from _RFN; type name comes from Type Name (with
    # _RFT / name fallbacks). Elements missing either level get a placeholder
    # so every row is still accounted for.
    category_breakdown = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )

    cad_dwg = set(); cad_dxf = set(); cad_dwf = set()
    rvt_links_set = set()       # populated only if L3 kept this category in future
    ifc_links = set()
    pdf_files = set(); img_files = set()

    # New report metrics (populated from the dedicated reference categories
    # we now keep in L3: Project Base Point, Survey Point, Phases, Family Type)
    survey_point        = None
    project_base_point  = None
    phase_names         = []     # in order encountered
    family_type_count   = 0

    elements_count = 0
    rows_seen      = 0
    progress_step  = 50000
    next_dot       = progress_step

    for category, obj in iter_categorized(raw_path):
        rows_seen += 1
        if rows_seen >= next_dot:
            print(".", end="", flush=True)
            next_dot += progress_step

        # Project metadata: capture verbatim. Document and Project Information
        # often carry overlapping but not identical fields, so we merge them
        # additively (Document seen first, then Project Information adds /
        # overrides) instead of letting one obliterate the other.
        if category in ("Revit Document", "Revit Project Information"):
            project_info.update(obj)
            continue

        # Reference points carry survey coordinates + true-north angle.
        # First row wins (these categories should have exactly one each).
        if category == "Revit Survey Point":
            if survey_point is None:
                survey_point = obj
            continue
        if category == "Revit Project Base Point":
            if project_base_point is None:
                project_base_point = obj
            continue

        # Phases: one row per phase. We just collect the names in order
        # so the report can list them.
        if category == "Revit Phases":
            phase_name = obj.get("name") or obj.get("Type Name") or "<unnamed>"
            phase_names.append(phase_name)
            continue

        # Family Types library: count only — names would balloon the report.
        if category == "Revit Family Type":
            family_type_count += 1
            continue

        # Three-level breakdown for the Aggregate. Family name comes from
        # _RFN ("Revit Family Name"); type name from Type Name with _RFT
        # ("Revit Family Type") and 'name' as fallbacks. Missing values
        # collapse to '<no family>' / '<no type>' so the breakdown stays
        # exhaustive and we can spot rows with sparse identity later.
        family_name = obj.get("_RFN") or "<no family>"
        type_name   = (obj.get("Type Name") or obj.get("_RFT") or
                       obj.get("name") or "<no type>")
        category_breakdown[category][family_name][type_name] += 1

        # Levels / worksets are tracked on every element (so they pick up
        # all levels/worksets that physical elements actually use)
        level = (obj.get("Level") or obj.get("Reference Level") or
                 obj.get("Base Constraint") or obj.get("Schedule Level") or
                 obj.get("Base Level") or obj.get("Work Plane") or "")
        if level:
            level_count[level] += 1

        workset = obj.get("Workset", "")
        if workset:
            workset_count[workset] += 1

        if category in ELEMENT_CATEGORIES:
            elements_count += 1
            filtered_cat_count[category] += 1

            if is_suspicious(obj):
                suspicious.append({
                    "category":    category.replace("Revit ", ""),
                    "family_name": obj.get("_RFN") or obj.get("Type Name") or "",
                    "workset":     obj.get("Workset", ""),
                    "level":       level,
                })

            if not level and category not in NO_LEVEL_EXEMPT:
                no_level_count[category.replace("Revit ", "")] += 1

            phase = obj.get("Phase Demolished", "None")
            if phase and phase != "None":
                demolished_count[category.replace("Revit ", "")] += 1

    if rows_seen >= progress_step:
        print(" done.", flush=True)
    print(f"  Physical elements: {elements_count:,}")
    print(f"  Suspicious flags:  {len(suspicious):,}\n")

    # ── Files / link-type tallies derive from RawCounts (since L3 drops
    # the row data for those categories). For each category Forma
    # returned, classify by extension.
    for cat in category_count.keys():
        if not cat.startswith("Revit "):
            continue
        cat_lower = cat.lower()
        if   cat_lower.endswith(".dwg"):  cad_dwg.add(cat)
        elif cat_lower.endswith(".dxf"):  cad_dxf.add(cat)
        elif cat_lower.endswith(".dwf"):  cad_dwf.add(cat)
        elif cat_lower.endswith(".pdf"):  pdf_files.add(cat)
        elif cat_lower.endswith((".png", ".jpg", ".jpeg", ".tif", ".bmp", ".gif")):
            img_files.add(cat)
        elif ".ifc" in cat_lower:         ifc_links.add(cat)

    # RVT links: RawCounts only has the count, not the file names. So we
    # use the count from RawCounts directly for the report.
    rvt_links_count = category_count.get("Revit RVT Links", 0)

    # ── Build the report ────────────────────────────────────────────────
    total_views     = category_count.get("Revit View", 0) + category_count.get("Revit Views", 0)
    views_on_sheets = category_count.get("Revit Viewports", 0)
    real_worksets   = [ws for ws in workset_count if is_real_workset(ws)]

    report = []

    for metric, key in [
        ("Project Name", "Project Name"), ("Project Number", "Project Number"),
        ("Client Name", "Client Name"), ("Project Address", "Project Address"),
        ("Project Status", "Project Status"), ("Project Issue Date", "Project Issue Date"),
        ("Organization Name", "Organization Name"),
        ("Organization Description", "Organization Description"),
        ("Building Name", "Building Name"),
    ]:
        report.append({"Section": "Project Info", "Metric": metric, "Value": project_info.get(key, "N/A")})
    # Append the model name itself for clarity
    report.append({"Section": "Project Info", "Metric": "Model Name", "Value": MODEL_NAME})

    # ── Survey Point + Project Base Point coordinates ───────────────────
    # These come from the dedicated reference categories L3 now keeps.
    # If a model doesn't have them (some Forma exports omit them), we
    # still emit "N/A" rows so the report has a consistent shape.
    if survey_point:
        report.append({"Section": "Project Info", "Metric": "Survey Point E/W",       "Value": survey_point.get("E/W",  "N/A")})
        report.append({"Section": "Project Info", "Metric": "Survey Point N/S",       "Value": survey_point.get("N/S",  "N/A")})
        report.append({"Section": "Project Info", "Metric": "Survey Point Elevation", "Value": survey_point.get("Elev", "N/A")})
        report.append({"Section": "Project Info", "Metric": "Survey Point Latitude",  "Value": survey_point.get("Lat",  "N/A")})
        report.append({"Section": "Project Info", "Metric": "Survey Point Longitude", "Value": survey_point.get("Lon",  "N/A")})
    else:
        for m in ("Survey Point E/W", "Survey Point N/S", "Survey Point Elevation",
                  "Survey Point Latitude", "Survey Point Longitude"):
            report.append({"Section": "Project Info", "Metric": m, "Value": "N/A"})

    if project_base_point:
        report.append({"Section": "Project Info", "Metric": "Project Base Point E/W",
                       "Value": project_base_point.get("E/W", "N/A")})
        report.append({"Section": "Project Info", "Metric": "Project Base Point N/S",
                       "Value": project_base_point.get("N/S", "N/A")})
        report.append({"Section": "Project Info", "Metric": "Project Base Point Elevation",
                       "Value": project_base_point.get("Elev", "N/A")})
        report.append({"Section": "Project Info", "Metric": "Angle to True North",
                       "Value": project_base_point.get("Angle to True North", "N/A")})
    else:
        for m in ("Project Base Point E/W", "Project Base Point N/S",
                  "Project Base Point Elevation", "Angle to True North"):
            report.append({"Section": "Project Info", "Metric": m, "Value": "N/A"})

    for metric, value in [
        ("Total Element Count (raw)", f"{raw_counts.get('total_objects', 0):,}"),
        ("Total Elements",           f"{elements_count:,}"),
        ("Family Types in Library",  family_type_count),
        ("Number of Phases",         len(phase_names)),
        ("Levels",                   category_count.get("Revit Level", 0)),
        ("Grids",                    category_count.get("Revit Grids", 0)),
        ("Sheets",                   category_count.get("Revit Sheet", 0)),
        ("Total Views",              total_views),
        ("Views Placed on Sheets",   views_on_sheets),
        ("Views NOT on Sheets",      total_views - views_on_sheets),
        ("Schedules (Total)",        category_count.get("Revit Schedules", 0)),
        ("Schedules on Sheets",      category_count.get("Revit Schedule Graphics", 0)),
        ("Schedules NOT on Sheets",  category_count.get("Revit Schedules", 0) - category_count.get("Revit Schedule Graphics", 0)),
        ("Revit Links (.rvt)",       rvt_links_count),
        ("IFC Links (.ifc)",         len(ifc_links)),
        ("CAD Files (.dwg)",         len(cad_dwg)),
        ("CAD Files (.dxf)",         len(cad_dxf)),
        ("CAD Files (.dwf)",         len(cad_dwf)),
        ("PDF Files (.pdf)",         len(pdf_files)),
        ("Image Files",              len(img_files)),
        ("Worksets",                 len(real_worksets)),
    ]:
        report.append({"Section": "Model Summary", "Metric": metric, "Value": value})

    # ── Phases section (one row per phase, with its name) ───────────────
    for i, pn in enumerate(phase_names, start=1):
        report.append({"Section": "Phases", "Metric": f"Phase {i}", "Value": pn})

    for cat, count in sorted(filtered_cat_count.items(), key=lambda x: -x[1]):
        report.append({"Section": "Elements by Category", "Metric": cat.replace("Revit ", ""), "Value": count})
    for level, count in sorted(level_count.items(), key=lambda x: -x[1]):
        report.append({"Section": "Elements by Level", "Metric": level, "Value": count})
    for ws in sorted(real_worksets):
        report.append({"Section": "Worksets", "Metric": ws, "Value": workset_count[ws]})
    for link_type, links in [
        ("CAD Files (.dwg)",   sorted(f.replace("Revit ", "") for f in cad_dwg)),
        ("CAD Files (.dxf)",   sorted(f.replace("Revit ", "") for f in cad_dxf)),
        ("IFC Links (.ifc)",   sorted(f.replace("Revit ", "") for f in ifc_links)),
        ("PDF Files (.pdf)",   sorted(f.replace("Revit ", "") for f in pdf_files)),
        ("Image Files",        sorted(f.replace("Revit ", "") for f in img_files)),
    ]:
        for link in links:
            report.append({"Section": link_type, "Metric": link, "Value": 1})

    susp_summary = defaultdict(int)
    for s in suspicious:
        susp_summary[s["family_name"]] += 1
    for fam, count in sorted(susp_summary.items(), key=lambda x: -x[1]):
        report.append({"Section": "Suspicious Elements", "Metric": fam, "Value": count})
    for cat, count in sorted(no_level_count.items(), key=lambda x: -x[1]):
        report.append({"Section": "Elements Without Level", "Metric": cat, "Value": count})
    for cat, count in sorted(demolished_count.items(), key=lambda x: -x[1]):
        report.append({"Section": "Demolished Elements", "Metric": cat, "Value": count})

    # ── Group flat row list into nested dict and write ──────────────────
    grouped = {}
    for row in report:
        grouped.setdefault(row["Section"], {})[row["Metric"]] = row["Value"]

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(grouped, f, indent=4, ensure_ascii=False)
    print(f"Saved {report_path}")

    # ── Aggregate: three-level nested breakdown ─────────────────────────
    # Each category becomes a dict whose top-level keys are:
    #   - "count": total instances of the category
    #   - one key per family name (_RFN), whose value is in turn:
    #       - "count": total instances of that family
    #       - one key per type name (Type Name / _RFT / name), each
    #         {"count": <n>}
    # Categories that L3 dropped row-wise (Views, Sheets, Family, etc.)
    # appear with their count from RawCounts and no family/type breakdown
    # since we don't have row data for them.
    aggregate_categories = {}
    all_cat_names = sorted(set(category_count.keys()) | set(category_breakdown.keys()))
    for cat in all_cat_names:
        families = category_breakdown.get(cat, {})
        # Total count: prefer RawCounts (authoritative) but fall back to
        # the streamed total for kept categories that aren't in RawCounts.
        cat_total = category_count.get(cat) or sum(
            sum(types.values()) for types in families.values()
        )
        cat_block = {"count": cat_total}
        # Sort families alphabetically; types within each family also
        # alphabetically. Easier to scan than count-sorted lists.
        for fam in sorted(families.keys()):
            types = families[fam]
            fam_block = {"count": sum(types.values())}
            for tname in sorted(types.keys()):
                fam_block[tname] = {"count": types[tname]}
            cat_block[fam] = fam_block
        aggregate_categories[cat] = cat_block

    aggregate_payload = {
        "model_name":        MODEL_NAME,
        "total_objects":     raw_counts.get("total_objects", 0),
        "kept_objects":      raw_counts.get("kept_objects", 0),
        "unique_categories": raw_counts.get("unique_categories", len(category_count)),
        "kept_categories":   raw_counts.get("kept_categories", len(filtered_cat_count)),
        "categories":        aggregate_categories,
    }
    with open(aggregate_path, "w", encoding="utf-8") as f:
        json.dump(aggregate_payload, f, indent=4, ensure_ascii=False)
    print(f"Saved {aggregate_path}")

    # ── Terminal summary ────────────────────────────────────────────────
    print("\n" + "="*70)
    print("MODEL HEALTH SUMMARY")
    print("="*70)
    print(f"  Model Name:                   {MODEL_NAME}")
    print(f"  Project Name:                 {project_info.get('Project Name', 'N/A')}")
    print(f"  Project Number:               {project_info.get('Project Number', 'N/A')}")
    print(f"  Client Name:                  {project_info.get('Client Name', 'N/A')}")
    print(f"  Project Address:              {project_info.get('Project Address', 'N/A')}")
    print(f"  Project Status:               {project_info.get('Project Status', 'N/A')}")
    print(f"  Project Issue Date:           {project_info.get('Project Issue Date', 'N/A')}")
    print(f"  Organization Name:            {project_info.get('Organization Name', 'N/A')}")
    print(f"  Organization Desc:            {project_info.get('Organization Description', 'N/A')}")
    if project_info.get('Building Name'):
        print(f"  Building Name:                {project_info.get('Building Name')}")
    if survey_point:
        print(f"  Survey Point E/W:             {survey_point.get('E/W', 'N/A')}")
        print(f"  Survey Point N/S:             {survey_point.get('N/S', 'N/A')}")
        print(f"  Survey Point Elev:            {survey_point.get('Elev', 'N/A')}")
    if project_base_point:
        print(f"  Project Base Point E/W:       {project_base_point.get('E/W', 'N/A')}")
        print(f"  Project Base Point N/S:       {project_base_point.get('N/S', 'N/A')}")
        print(f"  Project Base Point Elev:      {project_base_point.get('Elev', 'N/A')}")
        print(f"  Angle to True North:          {project_base_point.get('Angle to True North', 'N/A')}")
    print(f"  Total Element Count (raw):    {raw_counts.get('total_objects', 0):,}")
    print(f"  Total Elements:               {elements_count:,}")
    print(f"  Family Types in Library:      {family_type_count:,}")
    print(f"  Number of Phases:             {len(phase_names)}")
    if phase_names:
        for i, pn in enumerate(phase_names, start=1):
            print(f"    Phase {i}:                     {pn}")
    print(f"  Levels:                       {category_count.get('Revit Level', 0)}")
    print(f"  Sheets:                       {category_count.get('Revit Sheet', 0)}")
    print(f"  Total Views:                  {total_views}")
    print(f"  Views on Sheets:              {views_on_sheets}")
    print(f"  Views NOT on Sheets:          {total_views - views_on_sheets}")
    print(f"  Schedules (Total):            {category_count.get('Revit Schedules', 0)}")
    print(f"  Schedules on Sheets:          {category_count.get('Revit Schedule Graphics', 0)}")
    print(f"  Schedules NOT on Sheets:      {category_count.get('Revit Schedules', 0) - category_count.get('Revit Schedule Graphics', 0)}")
    print(f"  Revit Links (.rvt):           {rvt_links_count}")
    print(f"  IFC Links (.ifc):             {len(ifc_links)}")
    print(f"  CAD Files (.dwg):             {len(cad_dwg)}")
    print(f"  CAD Files (.dxf):             {len(cad_dxf)}")
    print(f"  CAD Files (.dwf):             {len(cad_dwf)}")
    print(f"  PDF Files (.pdf):             {len(pdf_files)}")
    print(f"  Image Files:                  {len(img_files)}")
    print(f"  Worksets:                     {len(real_worksets)}")
    print("-"*70)
    print("  ELEMENTS BY CATEGORY:")
    for cat, count in sorted(filtered_cat_count.items(), key=lambda x: -x[1]):
        print(f"    {cat.replace('Revit ', ''):<40} {count:>10,}")
    print("-"*70)
    print("  QUALITY FLAGS:")
    print(f"    Suspicious elements:           {len(suspicious):>10,}")
    print(f"    Elements without level:        {sum(no_level_count.values()):>10,}")
    for cat, count in sorted(no_level_count.items(), key=lambda x: -x[1]):
        print(f"      - {cat:<38} {count:>10,}")
    print(f"    Demolished elements included:  {sum(demolished_count.values()):>10,}")
    print("="*70)