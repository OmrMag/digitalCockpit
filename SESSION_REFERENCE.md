# Session Reference — Digital Construction Cockpit

A handoff document from this session for any future Claude session working
on the cockpit. Pairs with `pending_items.json` (forward-looking work) by
covering the *backward-looking* context — what we built, why, what broke,
what I learned. If you're a future Claude reading this, **read this AND
`pending_items.json` before doing anything substantial**.

Last updated: 2026-05-04 (session 2 — added token-staging architecture; info.html rebuild attempted then reverted at user's request)

---

## 1. The cockpit at a glance

ALEC's Digital Construction Cockpit. FastAPI app deployed to Azure
(`digitalcockpit-alec.azurewebsites.net`, resource group
`digitalcockpit-rg`, B1 tier). Multi-tenant: every admin has their own
config blob (`config_{safe_email}.json` where `safe_email` is
`email.lower().replace("@","_at_").replace(".","_")`).

Integrates three external platforms:
- **Morta** (data platform, hub-scoped invites, tags as user permissions)
- **Revizto** (BIM coordination, two regional API hosts: dubai + ireland)
- **Forma** (Autodesk ACC, project-scoped, dual-token: 2-legged shared
  + 3-legged per-admin OAuth via PKCE)

Owner: `oawad@alec.ae` (the `OWNER_EMAIL` constant). Owner sees and edits
everything; non-owner admins only see projects where their email is in
the project's `admins[]` list.

Repo: `https://github.com/OmrMag/digitalCockpit.git` (PRIVATE). Currently
`main` is well ahead of `origin/main` — see git-history-cleanup section
below.

---

## 2. Repo layout

The cockpit's app root has these critical files at the top level:

```
app.py                          # Main FastAPI server (~6000 lines)
templates/
    base.html                   # Layout with sidebar
    manage.html                 # Manage Users page (largest, most editing)
    home.html, info.html, ...   # Other pages
style.css                       # Global styles (used by all templates)
utils.js                        # hackerText, smartFetch, sync indicators
codeRevizto/vsCode/             # Revizto extraction scripts (CLI tools)
    Revizto_Config.py
    Revizto_MasterScript.py
    Revizto_GET_*.py
    Revizto_POST_*.py
codeForma/                      # Forma extraction scripts
codeMorta/                      # Morta extraction scripts
config.json                     # Owner's shared credentials (gitignored)
config_<safe_email>.json        # Per-admin configs (gitignored)
forma_tokens*.json              # Token blobs (gitignored)
revizto_tokens_<region>_<safe_email>.json  # Per-admin per-region tokens
projects_registry.json          # Project metadata (gitignored)
invite_log.json                 # Invite history (gitignored)
portal_requests.json            # Portal submission queue (gitignored)
pending_items.json              # Forward-looking todos
SESSION_REFERENCE.md            # This file (when added to repo)
```

Project files in chat (`/mnt/project/*`) are a **stale snapshot** of
this tree. **Critical rule:** before editing any file, ask if the user
has changed it recently. The user has caught me multiple times shipping
based on a stale snapshot.

---

## 3. Working environment patterns

### Workspace
I do all editing in `/home/claude/work/`. Files in `/mnt/project/` are
read-only. Files staged for the user to download go in
`/mnt/user-data/outputs/deploy/` matching the repo's directory shape
(`templates/foo.html`, `codeRevizto/vsCode/bar.py`, etc.).

### Deploy command
The user's standard PowerShell sequence:
```powershell
cd "C:\Users\oawad\OneDrive - ALEC\digitalCockpit"
git add <files>
git commit -m "..."
git archive --format=zip --output=deploy.zip HEAD
az webapp deploy --resource-group digitalcockpit-rg --name digitalcockpit-alec --src-path deploy.zip --type zip
```

**Critical:** `git archive HEAD` packages whatever's in the latest
commit, NOT uncommitted changes. If the user's `git status` shows files
not staged, they need `git add` + `git commit` before `git archive` or
they'll deploy stale code (this happened once mid-session).

### File path mismatches
Some files live at the repo root in the user's tree but at nested paths
in their local checkout. Notable:
- `Revizto_GET_*.py` files live at `codeRevizto/vsCode/*.py`
- HTML templates live at `templates/*.html`

When git complains "pathspec did not match any files", check `git status`
output for the actual path.

### Validation pattern I use before staging
1. Python parse: `python3 -c "import ast; ast.parse(open(path).read())"`
2. Jinja render: load `base.html` + the modified template via
   `jinja2.DictLoader`, render with stub context, confirm length+key markers
3. JS syntax: extract `<script>` blocks from HTML, run `node -c` on each
4. Behavior tests when feasible: stub out external HTTP, exec the
   helper functions in isolation, assert on inputs/outputs

This validation has saved me multiple times.

---

## 4. Architecture quirks worth knowing

### Per-user blob caching (`_BLOB_CACHE_TTL_SEC = 300`)
All config/registry/log loads go through a per-user 5-min cache. Eager
import of `azure.storage.blob` to avoid cold-start latency.
`_prewarm_cache()` runs at startup. `ServerTimingMiddleware` (pure ASGI)
adds timing headers. `/api/_diag` endpoint reports timings + cache state.

### Active context propagation
`active_context` lives in each user's config and is set when they pick a
project from the sidebar. It carries `active_project_key`,
`revizto_project_uuid`, `revizto_region`, `morta_hub_id`,
`forma_project_id`, etc. The frontend dispatches `cockpit:projectChanged`
events when the sidebar selection changes; pages listen to refresh state.

### Script subprocess execution
Extraction scripts run as `asyncio.create_subprocess_exec` subprocesses.
Before launch, `write_script_config(user, user_temp)` writes the merged
admin config to `<temp>/_cockpit_config.json` and sets these env vars:
- `COCKPIT_USER` — admin's email (used for per-user file naming)
- `COCKPIT_EXPORTS_DIR` — where the script should drop output files
- `COCKPIT_CONFIG` — path to `_cockpit_config.json`

Forma scripts honor `COCKPIT_CONFIG` directly. Revizto scripts go through
`Revizto_Config.py` which doesn't honor env vars — it hardcodes paths
relative to `__file__`. This works because `_THIS_DIR/../..` happens to
land on the same root the cockpit uses.

### Revizto per-admin token files
- Filename: `revizto_tokens_<region>_<safe_email>.json` — for example,
  `revizto_tokens_dubai_oawad_at_alec_ae.json`
- Local-dev fallback: `revizto_tokens_<region>.json` (no email suffix)
- Multi-region scripts (the new ones) check `COCKPIT_USER` env var to
  decide which filename pattern to use.

### Schema of project registry
Keyed by project_id (e.g., `"rak-central"`). Each entry:
```json
{
  "name": "RAK Central",
  "platforms": {
    "morta":   {"hub_id": "...", "tags": [{"tag_reference_id":"...", "name":"..."}]},
    "revizto": {
      "project_uuid": "...",
      "region":       "dubai",
      "roles":        [{"id": 256975, "uuid": "...", "name": "06_Viewer"}, ...],
      "license_roles":[{"id": 1, "name": "Guest"}, ...],
      "member_suggestions": {
        "companies":   ["EGIS", "ALEC", ...],
        "departments": ["Digital", "QA", ...],
        "locations":   ["Abu Dhabi", "Dubai", ...]
      }
    },
    "forma":   {"project_id": "...", "companies": [...], "roles": [...]}
  },
  "admins": ["oawad@alec.ae", ...]
}
```

The `member_suggestions` block was added this session. Sourced from
`/v5/project/{uuid}/team` (see Revizto API quirks below).

---

## 5. Revizto API quirks (LEARN THESE)

These tripped me up multiple times this session. Future me: read this
section before assuming any Revizto endpoint behaves the way docs imply.

### Result envelope
Revizto returns 200 OK with a JSON envelope even for app-level errors:
```json
{"result": -20, "data": null, "message": "..."}
```
- `result: 0` = success
- `result < 0` = app-level error (e.g., `-20` = access denied, `-10` =
  not found, `-21` = invalid token)
- HTTP status alone is NOT enough. **Always check `body.result`** in
  addition to `response.status_code`.

### Per-license permissions are independent
A user can be a member of multiple licenses with different permission
scopes per license. So `/v5/license/{uuid}/role/list` may succeed for
license A but 404/`-20` for license B with the same token.

When iterating licenses, **don't bail on the first failure** — try all
of them, cache the working one, return what you can.

### Endpoints that work today
- `GET /v5/user/licenses` — list user's licenses ✓
- `GET /v5/license/{uuid}/role/list` — project roles defined in a
  license ✓ (envelope-error per license possible)
- `GET /v5/project/list/{lic_uuid}/paged` — projects in a license ✓
- `GET /v5/project/{uuid}/team` — project team members with
  `company / department / location / accessRole` ✓ — **the workhorse
  for "who's in this project" data**
- `POST /v5/license/{uuid}/invite/bulk` — invite users to a license ✓
- `POST /v5/project/{uuid}/role/invite` — invite to project ✓

### Endpoints that are dead (for our account)
- `GET /v5/license/{uuid}/member/list` — **404 for all our licenses**.
  This is the one I chased for 4 rounds before discovering. May have
  been deprecated; may be permission-gated. Either way, don't use it.
  The replacement is `/v5/project/{uuid}/team` (project-scoped instead
  of license-scoped, but returns the same identity data plus location).

### Two regions
`api.dubai.revizto.com` and `api.ireland.revizto.com`. Same API, different
hosts. Each region has its own token file. The cockpit's
`active_context.revizto_region` decides which to talk to for invites
etc. Multi-region inventory scripts (License_Projects, License_Members)
iterate `["dubai", "ireland"]` and tag results with the region that
returned them.

### Token refresh
`Revizto_MasterScript.get_access_token()` refreshes via stored refresh
token, falling back to access code re-auth. **Single-region** — only
refreshes the active-context region's token. To refresh the OTHER
region's token, the user has to switch active context to that region
in the sidebar (which triggers a fresh refresh) and then back.

### Member endpoint response shape (project/team)
```json
{
  "result": 0,
  "data": {
    "count": N,
    "entities": [
      {
        "email": "...",
        "fullname": "...",
        "company": "...", "department": "...", "location": "...",
        "accessRole": {"id": 0, "uuid": "...", "name": "Architect"},
        "activated": true, "frozen": false, "invited": false,
        "lastActive": "2018-05-30 23:24:57",
        "uuid": "...", "firstname": "...", "lastname": "...",
        "member": {"user": {"uuid": "...", "email": "..."}},
        ...
      }
    ]
  }
}
```

Note: `data.entities` is an ARRAY. The `/role/list` endpoint, by contrast,
returns `data` as a DICT keyed by role UUID. Different shapes for
different endpoints — don't assume.

### Token staging — bridging blob to subprocess
`USE_BLOB=true` on Azure means tokens live in blob storage, **not on
the local filesystem**. Subprocess scripts have no Azure SDK and no
storage credentials, so they can't read tokens directly from blob.

The cockpit solves this with two staging helpers in `app.py`:

- `prepare_forma_3l_token(user, user_temp)` — writes the admin's 3L
  Forma access token to `<user_temp>/_forma_3l_token.json`. Patched
  `Forma_MasterScript.get_3legged_token()` reads the path from
  `COCKPIT_FORMA_3L_TOKEN_FILE` env var.

- `prepare_revizto_tokens(user, user_temp)` — writes
  `_revizto_tokens_dubai.json` and `_revizto_tokens_ireland.json`
  to user_temp using the cockpit's `revizto_get_token_for_region()`
  (which handles blob → refresh → access-code-fallback). Multi-region
  scripts (License_Projects, License_Members) read both files via
  `COCKPIT_REVIZTO_TOKENS_DIR` env var.

The order to look for tokens in a multi-region Revizto script:
1. `$COCKPIT_REVIZTO_TOKENS_DIR/_revizto_tokens_<region>.json` (Azure
   production via blob → staged file)
2. `<root>/revizto_tokens_<region>_<safe_email>.json` (cockpit without
   blob — local-disk fallback)
3. `<root>/revizto_tokens_<region>.json` (local dev / standalone run)

This pattern is why `Revizto_GET_License_Projects.py` v2 works on Azure.
v1 tried direct filesystem reads and failed with "no token saved" for
both regions.

---

## 6. Things I built this session

Three big chunks plus several smaller ones:

### A. Inline invite card (replaced popup modal)
Largest piece. The Manage page used to open a modal popup for inviting
users. Replaced entirely with an inline card that sits ABOVE the Pending
Submissions card and matches its visual pattern (header with title +
action buttons on the right, body that expands when `+ Invite` is
clicked).

Three stages inside one card:
1. **Compose** — mode toggle (Bulk Upload / Single User), upload zone for
   Bulk, editable wide table, footer with Cancel + Review
2. **Review** — readonly version of the same table + target-info banner,
   footer with Back + Confirm & Send
3. **Results** — success/error summary

The wide table is the **primary editing surface** (no separate form
step). Cells are dropdowns for registry-backed values (Tag, License Role,
Project Role, Forma Company, Forma Role), text/datalist hybrid inputs
for free-text (Email, Name, Revizto Company/Department/Location), and
checkboxes for booleans (platform On toggles, Revizto L/P, Forma
Docs/Design/Model). Two-way binding via `data-bind` attribute updates
`previewUsers[]` array on every edit.

The hybrid `<input list="..."> + <datalist>` pattern lets admins pick
from suggestions OR type freely. Suggestions populated from
`platforms.revizto.member_suggestions` in the registry, which is sourced
from `/v5/project/{uuid}/team` during metadata sync.

### B. Multi-region Revizto extraction scripts
- `Revizto_GET_License_Projects.py` — was single-region (used `BASE_URL`
  + `REGION` from config). Now iterates `["dubai", "ireland"]` in one
  run. Each project tagged with its actual region.
- `Revizto_GET_License_Members.py` — same overhaul, plus the endpoint
  swap from `/license/{uuid}/member/list` (dead) to `/project/{uuid}/team`
  (working). Also gains `location` field.

Both honor `COCKPIT_USER` env var for per-admin token filename
resolution; fall back to legacy shared filename for local-dev.

### C. Pending Submissions visual harmonization
The Pending Submissions per-user table got rebuilt to visually match the
new Invite Users card. Same minimal headers (uppercase muted, no cream
fill), same compact row padding, same borderless-until-focus form
controls, same sticky-left freeze for ☑ + Email columns. Footer changed
from `[Discard] [⚡ Process Approved]` (split, emoji) to `[Discard]
[Approve]` (right-aligned pair, no emoji), matching Invite's
`[Cancel] [Review]` pattern.

Single-letter column labels (`M` / `R` / `F`) replaced with full words
(`Morta` / `Revizto` / `Forma`) for consistency.

### D. Smaller fixes
- Dropped top-level Company/Department from direct invite flow (each
  platform owns its own identity now). Portal flow keeps them.
- Bulk parser, single + bulk handlers all updated.
- Various CSS overrides scoped to the page so global styles don't bleed.

---

## 7. Failures I fell into this session (don't repeat)

### Flex layout overflow chase (3 rounds)
The wide table was widening the entire `.main` flex column, dragging
every card on the page wider. I tried band-aid fixes at the inner
wrapper level for two rounds before identifying the real cause.

**Root cause:** `body { display: flex }` + `.main { flex: 1 }` (set in
style.css). Flex items default to `min-width: auto`, meaning they
won't shrink below their content's intrinsic min-width. So a wide
descendant forces `.main` wider than its allocated track.

**Fix:** `.main { min-width: 0; }` scoped to the page. This is the
**standard, well-known fix for flex content overflow** — I should have
gone there immediately. I added `.page-content { overflow-x: hidden; }`
as belt-and-braces.

**Lesson for future me:** when an inner `overflow-x: auto` wrapper
isn't triggering, the cause is almost always an unconstrained flex
ancestor, not the wrapper itself. Skip the wrapper-level fixes and go
straight to `min-width: 0` on flex items.

### Wrong Revizto endpoint chase (4 rounds)
Spent four rounds trying to make `/v5/license/{uuid}/member/list` work.
Iterated all licenses, added diagnostic surfacing, etc. Each round I
came up with an explanation for why it might be failing (token scope,
permission tier, multi-license iteration, etc.) and a fix. None worked.

**Root cause:** that endpoint is genuinely dead for our account.
Returns 404 across all licenses. The reference script
(`Revizto_GET_License_Members.py`) was also using it and also fails —
so it wasn't a cockpit-specific issue.

**The right path was `/v5/project/{uuid}/team`** — different scope
(project, not license), but returns the data we need. The user
eventually pointed me to the API docs page for this endpoint. Once we
saw the docs, fix took one round.

**Lesson for future me:** when a single endpoint fails repeatedly with
404 across multiple inputs and the same auth that works for sibling
endpoints, **the endpoint is the problem**. Stop trying variations of
the same call. Ask the user for the Revizto API docs page, or have
them HAR-capture a working call from Revizto's web UI to find the real
endpoint. Don't assume the reference script in the codebase is up to
date — it might be using a deprecated path.

### USE_BLOB filesystem assumption (3 rounds)
Wrote a multi-region Revizto script that read tokens directly from
the filesystem. Worked locally. On Azure, returned "no token saved"
for both regions every time.

**Root cause:** the cockpit runs with `USE_BLOB=true` on Azure. Tokens
live in blob storage, not on disk. Subprocess scripts have no SDK and
no credentials to reach blob. My script's filesystem reads found
nothing because nothing was there.

**The right pattern, which the codebase already had for Forma:** the
cockpit's main process uses an `prepare_*_token()` helper to fetch
credentials from blob and stage them as plain JSON files in the
per-user temp dir before launching the subprocess. The script reads
the staged file via a well-known env-var path. I missed this even
though `prepare_forma_3l_token()` was sitting right there as
precedent.

**Lesson for future me:** when a subprocess script needs platform
credentials, check whether a staging helper already exists for ANY
platform first. If yes, mirror the pattern. If no, build one in
`app.py` using `revizto_get_token_for_region()` / equivalent and
expose the path via env var. Never have a subprocess script attempt
to read from blob directly — that's an architectural layering
violation that won't work on Azure even when it works locally.

### Big-bang doc rebuild without checkpoints (multiple rounds)
User asked to rebuild `info.html` from scratch — bigger scope, denser
format, all in one round. I committed to it and produced a 100KB+
document with 9 SVG diagrams across 27 sections. After delivery the
user asked for changes (background color mismatch with the cockpit's
brand, project-name specifics that shouldn't have been there). The
fixes were straightforward but multiple rounds were needed because
the issues only became visible at full-document scale. Eventually the
user reverted to the previous version.

**Root cause:** I should have pushed harder for an outline-first or
chunked approach when the user picked one-round rebuild. The bigger
the artefact, the more variables can drift from intent (color theme,
naming conventions, layout density, factual accuracy). Catching all
of these on a single review pass is unrealistic.

**Lesson for future me:** when a user asks for a one-shot rebuild of
something already-working and substantial (>50KB, multiple diagrams,
or "everything"), name the risk explicitly. Offer to build a sample
section first OR produce just the visual system (palette + 1-2
diagrams) for sign-off before committing to the full piece. "Working
but imperfect" is often more valuable to the user than "completely
new but possibly off in ways neither of us anticipated."

Corollary: when rebuilding, **mirror the existing brand colors** by
literally reading them from the deployed `style.css` file rather than
picking my own palette. The cockpit uses very specific values — navy
`#0f1b2d`, accent `#4a9eff`, success `#34d399`, warning `#fbbf24`,
error `#f87171` — and I should never guess these.

### Stale snapshot mistake (caught by user)
Once mid-session I shipped a fix based on `/mnt/project/manage.html`
without realizing the user had edited their local copy more recently.
Ended up reverting. **Lesson:** whenever editing a file in this
session, ask "have you changed this recently?" if more than a couple
hours have passed since I last edited it.

### Single-letter labels misread
At one point user asked about column labels `M`, `R`, `F` in Pending
Submissions. I initially misread these as a separate styling issue;
they were actually clear platform abbreviations the user wanted spelled
out. Read user feedback carefully — small wording matters.

### Misreading "use this endpoint"
User showed me `Revizto_GET_Project_Roles.py` (using `/role/list`) and
said "use this endpoint". I almost interpreted that as "put role names
in the Company dropdown" which would have been semantic nonsense. I
asked clarifying questions instead and discovered they were pointing
at the URL **pattern**, not the data. **Lesson:** when a user's
direction would result in an obviously wrong outcome, ASK before doing
it. Don't optimize for "execute fast", optimize for "execute right".

---

## 8. User communication patterns

- Brief messages, often via single-select forms when I offer them
- Strong preference for **splitting deploys into smaller rounds** — one
  conceptual change per round, validate, then next round
- Catches stale snapshots — explicitly cares about not losing recent work
- Values **diagnostic-first approach** — HAR file uploads have been
  decisive; raw stdout from local script runs has settled multiple
  endpoint mysteries
- Pragmatic about tradeoffs (chose the "good enough" multi-source
  suggestion strategy without insisting on perfect)
- When sharing multiple files I MUST be explicit about which goes where
  and which to copy. User missed `manage.html` once when I only
  emphasized `app.py`.
- Will sometimes **say "do option 1 + option 3" but pick a third**
  combination when I present a multi-question form. Read the actual
  answers carefully; don't assume the first picks become the plan.
- Will **revert** without much explanation if a deliverable misses the
  mark. Don't take it personally — recover, ship a fix, or accept that
  the previous version was good enough.
- Often uploads reference files (a screenshot, a script's output, a
  doc page) as the cleanest way to communicate intent. **Read these
  carefully and quote back the specific bits I'm acting on** — caught
  myself misreading "use this endpoint" once when the user meant the
  URL pattern, not the data.

---

## 9. Locked design decisions (don't relitigate)

- **Direct invite flow has NO top-level Company/Department.** Each
  platform owns its own identity (Revizto Company/Dept/Location, Forma
  Company UUID). Portal flow keeps top-level — separate code path.
- **Invite log schema is mixed:** direct invites write empty
  Company/Department; portal-derived invites populate them. Mixed is
  fine, accurately reflects what was collected.
- **The wide invite table IS the editing surface** for both Single and
  Bulk. No separate form step. Single mode = 1 row prepopulated; Bulk
  mode = N rows from upload. Both use the same render function.
- **Both Revizto invite-target checkboxes (License + Project) default
  to checked.** Per-row checkbox handles include/exclude; no add-row
  button (Single = 1 row by definition).
- **Hybrid datalist approach** for Revizto Company/Dept/Location: real
  values from `/project/{uuid}/team` are suggestions, but admin can
  always type new values too.
- **Revizto location IS exposed by `/team` endpoint** even though the
  old `/member/list` didn't. Use it.
- **Single API endpoint for both Single + Bulk invites** — `/api/invite/bulk`
  handles N=1 fine. `/api/invite/single` exists as a deprecated alias
  but should be removed eventually (in pending_items).

---

## 10. Conventions I learned to follow

### CSS scoping
When fixing visual issues, scope the fix to the page (in
`{% block head %}<style>`) rather than editing `style.css`. Global
edits affect all pages.

### Override pattern for global rules
`style.css` has a `table tr:hover { background: var(--cream-mid) }`
rule that I had to override per-page. Pattern:
```css
#previewTable tr:hover, #reviewTable tr:hover { background: transparent; }
```
Set hover at the row level, not the td level — let td-specific
backgrounds (like `.col-morta`) keep painting.

### Sticky-left freeze pattern
For wide tables that scroll horizontally:
```css
table { border-collapse: separate; border-spacing: 0; }
th:nth-child(1), td:nth-child(1) {
    position: sticky; left: 0; z-index: 2;
    background: white;
    min-width: 28px; width: 28px;
}
th:nth-child(2), td:nth-child(2) {
    position: sticky; left: 28px; z-index: 2;
    background: white;
    min-width: 180px;
    box-shadow: 1px 0 0 var(--border-cream);
}
thead th:nth-child(1), thead th:nth-child(2) { z-index: 3; }
```

`border-collapse: separate` is REQUIRED — sticky doesn't work on `td`
under `border-collapse: collapse`.

### Don't use `pointer-events: none` on a control's own toggle
A platform's "On" checkbox lives inside a `td.col-revizto`. The fade
selector `tr.revizto-off td.col-revizto input { pointer-events: none }`
disabled the toggle itself, making it impossible to turn the platform
back on. Fix: `:not([data-bind="revizto"])` exclusion. Future me — if
adding similar disable-when-off logic, always exempt the toggle that
controls it.

---

## 11. Open threads at session end

See `pending_items.json` for the canonical list. Highlights:

**Major queued items:**
- Portal page revamp (subcontractor request flow, user wants own round)
- Git history cleanup (66 unpushed commits, scrub rotated secrets via
  `git filter-repo` before pushing)
- Pre-commit hook to prevent future secret commits

**Smaller items:**
- `revizto-license-uuid-on-member-rows` — `/team` endpoint doesn't
  return `license_uuid`, so members CSV has it blank now
- `audit-other-revizto-cli-scripts` — sweep for other deprecated
  endpoints like the dead `/member/list`
- `Profile_Status_Check_DIAG.py` not wired in production
- Activity feed only shows invite events
- `Revizto_POST_Remove_Users_From_Project.py` has a broken URL pattern

**In flight when session paused:**
- User wants "another file explaining everything" — that's THIS file
- Git cleanup walkthrough was queued (Step 0 + Step 1 commands ready,
  user paused to address something else first)
- Local `Revizto_GET_License_Members.py` rewrite ready to deploy

---

## 12. Tools available in this environment

In Claude.ai with project knowledge:
- `project_knowledge_search` — search `/mnt/project/*` files. Use this
  before assuming anything about the codebase.
- `bash_tool` (`/home/claude/work/`) — full Python/shell environment.
  Network-allowed for: pypi, github.com, npmjs, ubuntu mirrors,
  api.anthropic.com (NOT Revizto/Forma APIs — can't actually hit them
  from this sandbox).
- `view`, `str_replace`, `create_file` — file editing in workspace.
  `/mnt/project/` is read-only — copy to `/home/claude/work/` first.
- `present_files` — share files for the user to download. Path
  pattern matches the repo: `templates/foo.html`, `codeRevizto/vsCode/bar.py`.
- `web_search`, `web_fetch` — for API docs etc. Revizto's developer.revizto.com
  is JS-rendered and hard to scrape; ask user for the docs page or HAR.
- `ask_user_input_v0` — for clarifying questions. Don't use it for
  obvious things; use it for genuine ambiguity that would change the
  scope of the work.

---

## 13. Tone notes

User is friendly, technical, and pragmatic. They appreciate when I:
- Show my reasoning before coding (especially on architecture decisions)
- Ask clarifying questions on genuine ambiguity
- Validate before staging
- Own mistakes briefly without spiraling into apology
- Suggest options with my honest read on each

They get less patient when I:
- Repeat the same failed approach
- Don't read their feedback carefully (single-letter `M` thing)
- Skip the "ask first" step on coupled changes

When I'm uncertain about a major change, **propose a plan in writing
before coding**. Multiple times this session a paragraph of "here's
what I'd change and why" caught a misalignment before code was written.

---

End of reference. Pair this with `pending_items.json` for the full
context. Good luck, future me.