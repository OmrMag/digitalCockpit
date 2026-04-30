"""
Proccesed_Model_Properties.py
Reads forma_model_properties.json from EXPORTS_DIR and produces:
  1. forma_model_elements.json/csv  — physical building elements only
  2. forma_model_report.json/csv    — model health summary + quality flags
"""

import json
import csv
import os
from collections import defaultdict

from Forma_Config import EXPORTS_DIR

RAW_FILE = os.path.join(EXPORTS_DIR, "forma_model_properties.json")

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


if __name__ == "__main__":
    if not os.path.exists(RAW_FILE):
        print(f"[Forma] Source file not found: {RAW_FILE}")
        print("[Forma] Run Forma_GET_Model_Properties.py first to generate it.")
        exit(1)

    print(f"Loading {RAW_FILE}...")
    with open(RAW_FILE, "r", encoding="utf-8") as f:
        raw_data = json.load(f)
    print(f"  {len(raw_data):,} total objects loaded\n")

    elements         = []
    suspicious       = []
    category_count   = defaultdict(int)
    level_count      = defaultdict(int)
    workset_count    = defaultdict(int)
    no_level_count   = defaultdict(int)
    demolished_count = defaultdict(int)
    project_info     = {}

    cad_dwg   = set()
    cad_dxf   = set()
    cad_dwf   = set()
    rvt_links = set()
    ifc_links = set()
    pdf_files = set()
    img_files = set()

    for obj in raw_data:
        category  = obj.get("Category", "None")
        cat_lower = category.lower()
        category_count[category] += 1

        level = (obj.get("Level") or obj.get("Reference Level") or
                 obj.get("Base Constraint") or obj.get("Schedule Level") or
                 obj.get("Base Level") or obj.get("Work Plane") or "")
        if level:
            level_count[level] += 1

        workset = obj.get("Workset", "")
        if workset:
            workset_count[workset] += 1

        if category in ("Revit Document", "Revit Project Information"):
            project_info = obj

        if category.startswith("Revit "):
            if cat_lower.endswith(".dwg"):   cad_dwg.add(category)
            elif cat_lower.endswith(".dxf"): cad_dxf.add(category)
            elif cat_lower.endswith(".dwf"): cad_dwf.add(category)
            elif cat_lower.endswith(".pdf"): pdf_files.add(category)
            elif cat_lower.endswith((".png", ".jpg", ".jpeg", ".tif", ".bmp", ".gif")):
                img_files.add(category)
            elif ".ifc" in cat_lower:        ifc_links.add(category)
        if category == "Revit RVT Links":
            file_name = obj.get("File Name") or obj.get("name", "")
            if file_name:
                rvt_links.add(file_name)

        if category in ELEMENT_CATEGORIES:
            elements.append(obj)

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

    filtered_cat_count = defaultdict(int)
    for obj in elements:
        filtered_cat_count[obj.get("Category", "None")] += 1

    total_views     = category_count.get("Revit View", 0) + category_count.get("Revit Views", 0)
    views_on_sheets = category_count.get("Revit Viewports", 0)
    real_worksets   = [ws for ws in workset_count if is_real_workset(ws)]

    print(f"Physical elements: {len(elements):,}")
    print(f"Suspicious flags:  {len(suspicious):,}\n")

    elements_json = os.path.join(EXPORTS_DIR, "forma_model_elements.json")
    with open(elements_json, "w", encoding="utf-8") as f:
        json.dump(elements, f, indent=4)
    print(f"Saved {elements_json}")

    if elements:
        all_keys = list(dict.fromkeys(k for e in elements for k in e.keys()))
        elements_csv = os.path.join(EXPORTS_DIR, "forma_model_elements.csv")
        with open(elements_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(elements)
        print(f"Saved {elements_csv}")

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

    for metric, value in [
        ("Total Elements",           f"{len(elements):,}"),
        ("Levels",                   category_count.get("Revit Level", 0)),
        ("Grids",                    category_count.get("Revit Grids", 0)),
        ("Sheets",                   category_count.get("Revit Sheet", 0)),
        ("Total Views",              total_views),
        ("Views Placed on Sheets",   views_on_sheets),
        ("Views NOT on Sheets",      total_views - views_on_sheets),
        ("Schedules (Total)",        category_count.get("Revit Schedules", 0)),
        ("Schedules on Sheets",      category_count.get("Revit Schedule Graphics", 0)),
        ("Schedules NOT on Sheets",  category_count.get("Revit Schedules", 0) - category_count.get("Revit Schedule Graphics", 0)),
        ("Revit Links (.rvt)",       len(rvt_links)),
        ("IFC Links (.ifc)",         len(ifc_links)),
        ("CAD Files (.dwg)",         len(cad_dwg)),
        ("CAD Files (.dxf)",         len(cad_dxf)),
        ("CAD Files (.dwf)",         len(cad_dwf)),
        ("PDF Files (.pdf)",         len(pdf_files)),
        ("Image Files",              len(img_files)),
        ("Worksets",                 len(real_worksets)),
    ]:
        report.append({"Section": "Model Summary", "Metric": metric, "Value": value})

    for cat, count in sorted(filtered_cat_count.items(), key=lambda x: -x[1]):
        report.append({"Section": "Elements by Category", "Metric": cat.replace("Revit ", ""), "Value": count})
    for level, count in sorted(level_count.items(), key=lambda x: -x[1]):
        report.append({"Section": "Elements by Level", "Metric": level, "Value": count})
    for ws in sorted(real_worksets):
        report.append({"Section": "Worksets", "Metric": ws, "Value": workset_count[ws]})
    for link_type, links in [
        ("Revit Links (.rvt)", sorted(rvt_links)),
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

    # ── Build grouped report dict ─────────────────────────────────────────
    grouped = {}
    for row in report:
        section = row["Section"]
        grouped.setdefault(section, {})[row["Metric"]] = row["Value"]

    report_json = os.path.join(EXPORTS_DIR, "forma_model_report.json")
    with open(report_json, "w", encoding="utf-8") as f:
        json.dump(grouped, f, indent=4)
    print(f"Saved {report_json}")

    # ── CSV: Section | Metric: Value (one row per entry, no header noise) ─
    report_csv = os.path.join(EXPORTS_DIR, "forma_model_report.csv")
    with open(report_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Section", "Metric", "Value"])
        for section, entries in grouped.items():
            for metric, value in entries.items():
                writer.writerow([section, metric, value])
    print(f"Saved {report_csv}")

    print("\n" + "="*55)
    print("MODEL HEALTH SUMMARY")
    print("="*55)
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
    print(f"  Total Elements:               {len(elements):,}")
    print(f"  Levels:                       {category_count.get('Revit Level', 0)}")
    print(f"  Sheets:                       {category_count.get('Revit Sheet', 0)}")
    print(f"  Total Views:                  {total_views}")
    print(f"  Views on Sheets:              {views_on_sheets}")
    print(f"  Views NOT on Sheets:          {total_views - views_on_sheets}")
    print(f"  Schedules (Total):            {category_count.get('Revit Schedules', 0)}")
    print(f"  Schedules on Sheets:          {category_count.get('Revit Schedule Graphics', 0)}")
    print(f"  Schedules NOT on Sheets:      {category_count.get('Revit Schedules', 0) - category_count.get('Revit Schedule Graphics', 0)}")
    print(f"  Revit Links (.rvt):           {len(rvt_links)}")
    print(f"  IFC Links (.ifc):             {len(ifc_links)}")
    print(f"  CAD Files (.dwg):             {len(cad_dwg)}")
    print(f"  CAD Files (.dxf):             {len(cad_dxf)}")
    print(f"  CAD Files (.dwf):             {len(cad_dwf)}")
    print(f"  PDF Files (.pdf):             {len(pdf_files)}")
    print(f"  Image Files:                  {len(img_files)}")
    print(f"  Worksets:                     {len(real_worksets)}")
    print("-"*55)
    print("  ELEMENTS BY CATEGORY:")
    for cat, count in sorted(filtered_cat_count.items(), key=lambda x: -x[1]):
        print(f"    {cat.replace('Revit ', ''):<40} {count:>6,}")
    print("-"*55)
    print("  QUALITY FLAGS:")
    print(f"    Suspicious elements:           {len(suspicious):>6,}")
    print(f"    Elements without level:        {sum(no_level_count.values()):>6,}")
    for cat, count in sorted(no_level_count.items(), key=lambda x: -x[1]):
        print(f"      - {cat:<38} {count:>6,}")
    print(f"    Demolished elements included:  {sum(demolished_count.values()):>6,}")
    print("="*55)