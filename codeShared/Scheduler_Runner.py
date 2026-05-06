"""
Scheduler_Runner.py — Headless execution of scheduler shards and compilers
═══════════════════════════════════════════════════════════════════════════

This module owns the run logic for the cockpit's scheduler. It is called
from app.py's /api/scheduler/shards/{id}/run and /compilers/{id}/run
endpoints, and (eventually, in Block 5) from the cron-firing engine.

THE RUN PIPELINE:
  1. Validate the shard (active, not auto-disabled, deps satisfied)
  2. Build merged config + override with the shard's context
  3. Spawn the script subprocess into a per-run temp dir
  4. Wait for completion
  5. Upload output files to the platform's blob container
  6. Generate SAS download URLs
  7. Record the run (status, duration, output blobs, errors)
  8. Update the shard's failure counter (auto-disable at 3)
  9. Clean up the temp dir

DESIGN DECISIONS (locked in during planning):
  - Use the TRIGGERING admin's tokens, not the shard.created_by admin's.
    The triggering admin is logged in, so their tokens are guaranteed
    fresh. (Block 5 will need server-side refresh helpers for cron.)
  - L4 dependency stamps ARE enforced — same rule as manual extractions.
  - Output files go to /tmp/_scheduler/{run_id}/ — a dedicated temp dir,
    NOT the admin's personal /tmp/{email}/ — so the Extractions tab
    doesn't get cluttered with files the admin didn't manually trigger.
  - Background execution is fire-and-forget via FastAPI's BackgroundTasks.
    The endpoint returns the run_id immediately; the browser polls
    /api/scheduler/runs to watch status flip from running → success/failed.

WHAT THIS MODULE DOES NOT HANDLE:
  - Cron firing (Block 5)
  - Server-side token refresh for headless runs (Block 5)
  - Multi-instance job locking (postponed; B1 is single-instance)
"""

from __future__ import annotations
import json
import os
import shutil
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Callable, Any


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

# Where in-flight scheduler runs write their output before upload to blob.
# Dedicated to the scheduler so it doesn't intermingle with manual runs.
SCHEDULER_TEMP_ROOT = Path("/tmp/_scheduler")

# Subprocess hard timeout. L3 on Wynn-scale models can run ~20 minutes; we
# allow 45 to be safe. Above this we kill the process and mark the run failed.
SUBPROCESS_TIMEOUT_SEC = 45 * 60

# How many days the SAS download URL stays valid. 7 is generous; the actual
# blob lives 15 days under the lifecycle rule, so making the URL outlive the
# blob would be misleading.
SAS_URL_TTL_HOURS = 7 * 24


# ═══════════════════════════════════════════════════════════════════════════
# Public entry point — fire one shard
# ═══════════════════════════════════════════════════════════════════════════

def run_shard(
    *,
    shard: dict,
    triggering_user: str,
    # Callbacks injected by app.py so this module stays decoupled from
    # the web layer and from app.py's blob client. All are required.
    build_config_for_user: Callable[[str], dict],
    write_config_to_path: Callable[[dict, Path], None],
    prepare_forma_3l_token: Callable[[str, Path], Optional[Path]],
    prepare_revizto_tokens: Callable[[str, Path], Optional[Path]],
    check_l4_dependency: Callable[[str, str], dict],
    # Persistence callbacks
    update_shard: Callable[[str, dict], None],
    append_run: Callable[[dict], None],
    # Blob upload (Blob_Client functions)
    blob_upload_file: Callable[..., dict],
    blob_generate_url: Callable[..., str],
    blob_container_for_platform: Callable[[str], str],
    # The script_path resolver — given platform + script_name, returns Path
    resolve_script_path: Callable[[str, str], Path],
    # The L3 stamp helper (so successful L3 runs in the scheduler also feed
    # the dependency-tracking system, just like manual runs)
    stamp_l3_run: Callable[[str, str, list], None],
    # Optional: parent compiler run for compiler-driven invocations
    parent_compiler_run_id: Optional[str] = None,
) -> dict:
    """Execute one shard end-to-end. Returns the final run record.

    This function does the FULL run synchronously. The caller (app.py's
    endpoint) is expected to invoke it via FastAPI BackgroundTasks so the
    HTTP request doesn't block.

    The run record is appended to automation_runs.json TWICE:
      - Once at start with status='running' (so the UI sees activity)
      - Once at end with final status (replaces the earlier entry by id)
    """
    run_id  = _new_run_id("run")
    started = time.time()
    started_iso = _utc_now_iso()

    # Build the initial run record so the UI can poll it
    run_record = {
        "id":                 run_id,
        "kind":               "shard",
        "shard_id":           shard["id"],
        "shard_name":         shard.get("name", ""),
        "compiler_id":        shard.get("in_compiler"),
        "parent_compiler_run_id": parent_compiler_run_id,
        "platform":           shard.get("platform", ""),
        "script":             shard.get("script", ""),
        "triggered_by":       triggering_user,
        "trigger_kind":       "manual",
        "status":             "running",
        "started_at":         started_iso,
        "ended_at":           None,
        "duration_sec":       None,
        "output_blobs":       [],
        "error":              None,
        "exit_code":          None,
        "stdout_tail":        [],
        # Block 6A: which user(s) have downloaded this run via the agent.
        # Empty list means no agent has picked it up yet. The agent flips
        # this to add its user when it successfully delivers locally.
        "delivered_by":       [],
    }

    # Stamp the run as running before any heavy work — the UI polls this
    try:
        append_run(run_record)
    except Exception as e:
        # If we can't even record the run, abort. There's nothing useful
        # the UI could show afterwards.
        print(f"[scheduler] Failed to write initial run record: {e}", flush=True)
        return run_record

    # The temp dir for this run. Created fresh, removed at the end.
    run_temp = SCHEDULER_TEMP_ROOT / run_id
    try:
        run_temp.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        return _finalize_run(
            run_record, started, append_run,
            status="failed",
            error=f"Could not create temp dir: {e}",
        )

    try:
        # ── Step 1: validate the shard is runnable ─────────────────────
        if not shard.get("active", True):
            return _finalize_run(run_record, started, append_run,
                                 status="failed", error="Shard is paused.")
        if shard.get("auto_disabled"):
            return _finalize_run(run_record, started, append_run,
                                 status="failed",
                                 error="Shard is auto-disabled after repeated failures.")

        # ── Step 2: enforce dependency stamps for L4 scripts ───────────
        script_name = shard.get("script", "")
        platform    = shard.get("platform", "")
        if not script_name or not platform:
            return _finalize_run(run_record, started, append_run,
                                 status="failed", error="Shard has no script or platform.")

        dep_check = _check_l4_dependency_for_shard(
            shard, triggering_user, check_l4_dependency,
        )
        if dep_check is not None and not dep_check.get("ready", True):
            return _finalize_run(run_record, started, append_run,
                                 status="failed",
                                 error=f"Dependency not satisfied: {dep_check.get('message', 'unknown')}")

        # ── Step 3: resolve the script file ────────────────────────────
        try:
            script_path = resolve_script_path(platform, script_name)
        except Exception as e:
            return _finalize_run(run_record, started, append_run,
                                 status="failed", error=f"Could not resolve script path: {e}")
        if not script_path.exists():
            return _finalize_run(run_record, started, append_run,
                                 status="failed",
                                 error=f"Script file not found: {script_path}")

        # ── Step 4: build merged config, override active_context with shard's ──
        try:
            cfg = build_config_for_user(triggering_user)
            cfg = _override_context_from_shard(cfg, shard)
            config_path = run_temp / "_cockpit_config.json"
            write_config_to_path(cfg, config_path)
        except Exception as e:
            return _finalize_run(run_record, started, append_run,
                                 status="failed", error=f"Config preparation failed: {e}")

        # ── Step 5: prepare per-platform supplementary token files ─────
        # These are best-effort. Failure here doesn't abort — the script's
        # own auth path will surface the real problem if there is one.
        script_env = os.environ.copy()
        script_env["COCKPIT_USER"]        = triggering_user
        script_env["COCKPIT_EXPORTS_DIR"] = str(run_temp)
        script_env["COCKPIT_CONFIG"]      = str(config_path)
        script_env["PYTHONUNBUFFERED"]    = "1"
        script_env["COCKPIT_RUN_ID"]      = run_id
        script_env["COCKPIT_RUN_KIND"]    = "scheduler"

        try:
            f3l = prepare_forma_3l_token(triggering_user, run_temp)
            if f3l:
                script_env["COCKPIT_FORMA_3L_TOKEN_FILE"] = str(f3l)
        except Exception as e:
            print(f"[scheduler] forma 3l token prep failed (non-fatal): {e}", flush=True)
        try:
            rev_dir = prepare_revizto_tokens(triggering_user, run_temp)
            if rev_dir:
                script_env["COCKPIT_REVIZTO_TOKENS_DIR"] = str(rev_dir)
        except Exception as e:
            print(f"[scheduler] revizto tokens prep failed (non-fatal): {e}", flush=True)

        # ── Step 6: spawn the subprocess ───────────────────────────────
        try:
            result = subprocess.run(
                [sys.executable, str(script_path)],
                capture_output=True, text=True,
                timeout=SUBPROCESS_TIMEOUT_SEC,
                cwd=str(script_path.parent),
                env=script_env,
            )
        except subprocess.TimeoutExpired:
            return _finalize_run(run_record, started, append_run,
                                 status="failed",
                                 error=f"Script timed out after {SUBPROCESS_TIMEOUT_SEC // 60} minutes.",
                                 stdout_tail=[])
        except Exception as e:
            return _finalize_run(run_record, started, append_run,
                                 status="failed",
                                 error=f"Failed to launch subprocess: {e}")

        # Combine stdout + stderr; keep the last 50 lines for the run record
        all_lines = (result.stdout + "\n" + result.stderr).strip().splitlines()
        all_lines = [l for l in all_lines if l.strip()]
        stdout_tail = all_lines[-50:]

        # Heuristic: success = exit 0 OR a "saved/exported" marker in stdout.
        # Same logic as /api/extract — some scripts return non-zero on warnings.
        had_save_marker = any(
            ("saved" in l.lower() or "exported" in l.lower())
            for l in all_lines
        )
        ok = result.returncode == 0 or had_save_marker
        run_record["exit_code"]   = result.returncode
        run_record["stdout_tail"] = stdout_tail

        # ── Step 7: collect output files ───────────────────────────────
        output_files = _collect_output_files(run_temp)
        if ok and not output_files:
            # Script claimed success but produced no files. Treat as partial:
            # don't block the shard from running again, but flag it.
            return _finalize_run(run_record, started, append_run,
                                 status="failed",
                                 error="Script succeeded but produced no output files.")

        # ── Step 8: upload to blob ─────────────────────────────────────
        if ok:
            try:
                container = blob_container_for_platform(platform)
            except Exception as e:
                return _finalize_run(run_record, started, append_run,
                                     status="failed",
                                     error=f"No blob container for platform '{platform}': {e}")

            uploaded_blobs = []
            today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            for f in output_files:
                # Compute the blob path. Format:
                #   YYYY-MM-DD/{run_id}/{filename}
                blob_name = f"{today_prefix}/{run_id}/{f.name}"
                try:
                    blob_upload_file(container, blob_name, str(f))
                    download_url = blob_generate_url(
                        container, blob_name,
                        expiry_hours=SAS_URL_TTL_HOURS,
                    )
                    uploaded_blobs.append({
                        "filename":     f.name,
                        "size_kb":      round(f.stat().st_size / 1024, 1),
                        "container":    container,
                        "blob_path":    blob_name,
                        "download_url": download_url,
                    })
                except Exception as e:
                    # One file failing doesn't kill the whole upload — but
                    # we record the per-file error so it's visible in the UI.
                    uploaded_blobs.append({
                        "filename":     f.name,
                        "size_kb":      round(f.stat().st_size / 1024, 1),
                        "container":    container,
                        "blob_path":    blob_name,
                        "download_url": None,
                        "error":        str(e),
                    })

            run_record["output_blobs"] = uploaded_blobs

            # If any uploads succeeded, the run is "success"; if all failed,
            # it's a failure. Mixed: success with per-file error visible.
            any_uploaded   = any(b.get("download_url") for b in uploaded_blobs)
            all_uploaded   = all(b.get("download_url") for b in uploaded_blobs)
            if not any_uploaded:
                return _finalize_run(run_record, started, append_run,
                                     status="failed",
                                     error="All blob uploads failed.")
            final_status = "success" if all_uploaded else "partial"
        else:
            final_status = "failed"
            run_record["error"] = (
                f"Script exited with code {result.returncode}. "
                f"Last lines: {stdout_tail[-3:] if stdout_tail else 'no output'}"
            )

        # ── Step 9: stamp L3 run if applicable ─────────────────────────
        if ok and final_status in ("success", "partial"):
            try:
                stamp_l3_run(triggering_user, script_name, [b["filename"] for b in run_record["output_blobs"]])
            except Exception as e:
                print(f"[scheduler] Failed to stamp L3 run for {script_name}: {e}", flush=True)

        # ── Step 10: update shard's failure counter ────────────────────
        try:
            _update_shard_after_run(shard, final_status, update_shard)
        except Exception as e:
            print(f"[scheduler] Failed to update shard counter: {e}", flush=True)

        return _finalize_run(run_record, started, append_run, status=final_status)

    except Exception as e:
        # Any unhandled exception during the run
        return _finalize_run(
            run_record, started, append_run,
            status="failed",
            error=f"Unexpected error: {e}\n{traceback.format_exc()[:500]}",
        )

    finally:
        # Always clean up the temp dir, even on error
        try:
            if run_temp.exists():
                shutil.rmtree(run_temp, ignore_errors=True)
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════════
# Public entry point — fire one compiler (sequential shards)
# ═══════════════════════════════════════════════════════════════════════════

def run_compiler(
    *,
    compiler: dict,
    triggering_user: str,
    load_shard_by_id: Callable[[str], Optional[dict]],
    append_run: Callable[[dict], None],
    # Everything else gets forwarded to run_shard
    **shard_kwargs,
) -> dict:
    """Fire all shards in a compiler sequentially. Failures don't cascade —
    later shards still run. Returns the compiler-level run record."""
    run_id  = _new_run_id("crun")
    started = time.time()
    started_iso = _utc_now_iso()

    compiler_record = {
        "id":             run_id,
        "kind":           "compiler",
        "compiler_id":    compiler["id"],
        "compiler_name":  compiler.get("name", ""),
        "triggered_by":   triggering_user,
        "trigger_kind":   "manual",
        "status":         "running",
        "started_at":     started_iso,
        "ended_at":       None,
        "duration_sec":   None,
        "child_run_ids":  [],
        "shard_count":    0,
        "shards_succeeded": 0,
        "shards_failed":    0,
        "shards_partial":   0,
        # Block 6A: agent-delivered tracking. For compilers this is mostly
        # symbolic — the agent downloads child SHARD outputs, not compiler
        # outputs (compilers have no outputs). Kept here for schema parity.
        "delivered_by":   [],
    }

    try:
        append_run(compiler_record)
    except Exception as e:
        print(f"[scheduler] Failed to write compiler run record: {e}", flush=True)
        return compiler_record

    # Enumerate child shards
    shard_ids = compiler.get("shard_ids", []) or []
    compiler_record["shard_count"] = len(shard_ids)

    for sid in shard_ids:
        shard = load_shard_by_id(sid)
        if not shard:
            # Phantom reference — record a synthetic failed run record
            phantom = {
                "id":           _new_run_id("run"),
                "kind":         "shard",
                "shard_id":     sid,
                "shard_name":   "(deleted)",
                "compiler_id":  compiler["id"],
                "parent_compiler_run_id": run_id,
                "platform":     "",
                "script":       "",
                "triggered_by": triggering_user,
                "trigger_kind": "manual",
                "status":       "failed",
                "started_at":   _utc_now_iso(),
                "ended_at":     _utc_now_iso(),
                "duration_sec": 0,
                "output_blobs": [],
                "error":        "Shard referenced by compiler no longer exists.",
                "delivered_by": [],
            }
            try: append_run(phantom)
            except Exception: pass
            compiler_record["child_run_ids"].append(phantom["id"])
            compiler_record["shards_failed"] += 1
            continue

        child = run_shard(
            shard=shard,
            triggering_user=triggering_user,
            append_run=append_run,
            parent_compiler_run_id=run_id,
            **shard_kwargs,
        )
        compiler_record["child_run_ids"].append(child["id"])
        st = child.get("status")
        if   st == "success": compiler_record["shards_succeeded"] += 1
        elif st == "partial": compiler_record["shards_partial"]   += 1
        else:                 compiler_record["shards_failed"]    += 1

    # Determine compiler-level status from children
    if compiler_record["shards_failed"] == 0 and compiler_record["shards_partial"] == 0:
        final_status = "success"
    elif compiler_record["shards_succeeded"] == 0 and compiler_record["shards_partial"] == 0:
        final_status = "failed"
    else:
        final_status = "partial"

    return _finalize_run(compiler_record, started, append_run, status=final_status)


# ═══════════════════════════════════════════════════════════════════════════
# Internal helpers
# ═══════════════════════════════════════════════════════════════════════════

def _new_run_id(prefix: str) -> str:
    """Generate a sortable, opaque ID. Format: prefix_<base36 millis>_<rand>"""
    import secrets
    ms   = int(time.time() * 1000)
    rand = secrets.token_hex(3)
    return f"{prefix}_{_int_to_b36(ms)}{rand}"


def _int_to_b36(n: int) -> str:
    if n == 0: return "0"
    chars = "0123456789abcdefghijklmnopqrstuvwxyz"
    out = []
    while n > 0:
        n, r = divmod(n, 36)
        out.append(chars[r])
    return "".join(reversed(out))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _override_context_from_shard(cfg: dict, shard: dict) -> dict:
    """Take the user's merged config and overwrite the active_context fields
    with whatever the shard specifies. The shard config is authoritative for
    the run — we don't want it to depend on what the user happens to have
    selected in the sidebar at the moment."""
    sc = shard.get("context", {}) or {}
    cfg.setdefault("active_context", {})
    ctx = cfg["active_context"]

    # Map shard.context fields into active_context using the same names the
    # cockpit normally uses internally.
    if sc.get("project_id"):
        ctx["forma_project_id"] = sc["project_id"]
        cfg.setdefault("forma", {})["project_id"] = sc["project_id"]
    if sc.get("model_item_id"):
        ctx["forma_model_item_id"] = sc["model_item_id"]
        cfg.setdefault("forma", {})["model_item_id"] = sc["model_item_id"]
    if sc.get("model_name"):
        ctx["forma_model_name"] = sc["model_name"]
        cfg.setdefault("forma", {})["model_name"] = sc["model_name"]
    if sc.get("project_uuid"):
        ctx["revizto_project_uuid"] = sc["project_uuid"]
        region = sc.get("region", ctx.get("revizto_region", "dubai"))
        ctx["revizto_region"] = region
        cfg.setdefault(f"revizto_{region}", {})["project_uuid"] = sc["project_uuid"]
    if sc.get("region"):
        ctx["revizto_region"] = sc["region"]
    if sc.get("hub_id"):
        ctx["morta_hub_id"] = sc["hub_id"]
        cfg.setdefault("morta", {})["hub_id"] = sc["hub_id"]

    return cfg


def _check_l4_dependency_for_shard(
    shard: dict,
    user: str,
    check_l4_dependency: Callable[[str, str], dict],
) -> Optional[dict]:
    """Return a dep-check result ONLY if this shard's script is registered
    in L4_DEPENDENCIES. For all other scripts (the majority), this returns
    None — meaning "no dependency to check, proceed with the run".

    The cockpit's _check_l4_dependency() returns reason='unknown_script'
    for scripts not in its map. That's a "no dep needed" signal, NOT a
    failure — we filter it out here so the runner doesn't mistakenly
    block scripts that simply have no L4 prerequisite (e.g.
    Revizto_GET_License_Members, Morta_GET_Hub_Members,
    Forma_GET_Models, etc.).

    Note we use a slightly modified check: the cockpit's _check_l4_dependency
    looks at the user's CURRENT config context. For shards we want to check
    against the SHARD's context (because that's what the run will use).
    For now we trust the cockpit check as a reasonable approximation —
    if the user's current context doesn't have a stamped L3, the shard
    almost certainly won't either since the shard.context belongs to the
    same admin who created the shard. Edge case: an admin running another
    admin's shard. We accept this approximation for Block 4."""
    script = shard.get("script", "")
    try:
        result = check_l4_dependency(user, script)
    except Exception:
        return None
    if not isinstance(result, dict):
        return None
    # 'unknown_script' = "I don't have a dependency entry for this script,
    # which means it doesn't have one." Treat as pass-through.
    if result.get("reason") == "unknown_script":
        return None
    return result


def _collect_output_files(run_temp: Path) -> list[Path]:
    """List all non-internal output files in the run's temp dir."""
    out = []
    if not run_temp.exists():
        return out
    for f in sorted(run_temp.iterdir()):
        if not f.is_file():
            continue
        if f.name.startswith("_"):  # _cockpit_config.json, etc
            continue
        if f.suffix.lower() not in (".csv", ".json", ".xlsx", ".txt"):
            continue
        out.append(f)
    return out


def _update_shard_after_run(
    shard: dict,
    final_status: str,
    update_shard: Callable[[str, dict], None],
) -> None:
    """Increment consecutive_failures on fail, reset on success.
    Auto-disable the shard at 3 consecutive failures."""
    AUTO_DISABLE_AFTER = 3
    cur = int(shard.get("consecutive_failures", 0) or 0)
    if final_status == "success" or final_status == "partial":
        new_count = 0
        new_disabled = False
    else:
        new_count = cur + 1
        new_disabled = new_count >= AUTO_DISABLE_AFTER

    update_shard(shard["id"], {
        "consecutive_failures": new_count,
        "auto_disabled":        new_disabled,
        "last_run_id":          None,  # the caller writes this with the run record
        "updated_at":           int(time.time() * 1000),
    })


def _finalize_run(
    run_record: dict,
    started: float,
    append_run: Callable[[dict], None],
    *,
    status: str,
    error: Optional[str] = None,
    stdout_tail: Optional[list] = None,
) -> dict:
    """Set the final fields and persist."""
    run_record["status"]       = status
    run_record["ended_at"]     = _utc_now_iso()
    run_record["duration_sec"] = round(time.time() - started, 2)
    if error is not None:
        run_record["error"] = error
    if stdout_tail is not None:
        run_record["stdout_tail"] = stdout_tail

    try:
        append_run(run_record)
    except Exception as e:
        print(f"[scheduler] Failed to update final run record: {e}", flush=True)

    return run_record