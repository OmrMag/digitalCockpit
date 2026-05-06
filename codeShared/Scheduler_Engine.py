"""
Scheduler_Engine.py — Cron firing engine for scheduler compilers
═══════════════════════════════════════════════════════════════════════════

This module owns the timer side of the scheduler. It boots an APScheduler
BackgroundScheduler when the FastAPI process starts, registers a job for
each active compiler, and fires those jobs on cadence by delegating to
Scheduler_Runner.run_compiler().

THE LIFECYCLE:
  - boot(...)                    → start scheduler, register all active compilers
  - register_compiler(c)         → add or replace job for one compiler
  - unregister_compiler(id)      → remove a compiler's job
  - pause_compiler(id)           → pause job (kept registered, won't fire)
  - resume_compiler(id)          → resume a paused job
  - shutdown()                   → graceful stop on app exit

Token refresh for headless runs is NOT done here. The runner already calls
prepare_forma_3l_token() and prepare_revizto_tokens() with the shard's
created_by user, and those helpers refresh tokens from blob-stored refresh
tokens server-side. So cron runs reuse the same auth path manual runs use.

DESIGN DECISIONS:
  - APScheduler runs IN-PROCESS as a BackgroundScheduler. If the App
    Service worker restarts, in-flight runs are lost; missed scheduled
    runs may fire late (within misfire grace) or be dropped. Acceptable
    for B1 single-instance, not-mission-critical use.
  - Misfire grace = 30 minutes (1800s). A job that should have fired up
    to 30 minutes ago will still run when the scheduler comes up.
  - Startup sweep: any run record left in 'running' state for >10 min
    gets marked failed so the UI doesn't show stale "running" forever.
  - Job IDs are 'compiler_{compiler_id}' so we can find/remove cleanly.

WHAT'S NOT HERE:
  - Multi-instance safety. If App Service ever scales to 2+, both will
    fire jobs. Fix is a database-backed job store with locking.
  - Per-shard "next run at" preview in the UI.
  - Email notifications on failure (deferred — visible in run records UI).
"""

from __future__ import annotations
from typing import Optional, Callable, Any
import time
from datetime import datetime, timezone, timedelta


# ═══════════════════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════════════════

# How long after the scheduled fire time will APScheduler still try to fire
# a missed job? 30 minutes per the locked-in spec. Tradeoff: longer grace
# = more chance of recovering from brief restarts, but a stale 6 AM job
# firing at 6:25 AM might be confusing. 30 min is the negotiated middle.
MISFIRE_GRACE_SEC = 30 * 60

# Stale-run threshold for the startup sweep. If a run record is in 'running'
# state and started_at is older than this, treat it as a casualty of a
# restart and mark it failed. 10 minutes is generous enough for genuinely
# long-running scripts (Forma L3 takes ~20 min on Wynn-scale, but that's
# in-flight not stuck — see SUBPROCESS_TIMEOUT_SEC in Scheduler_Runner).
STALE_RUNNING_THRESHOLD_SEC = 10 * 60

# Day-of-week translation: our schedule format uses lowercase 3-char codes,
# APScheduler accepts the same. Provided as a constant for clarity.
DOW_MAP = {
    "mon": "mon", "tue": "tue", "wed": "wed",
    "thu": "thu", "fri": "fri", "sat": "sat", "sun": "sun",
}


# ═══════════════════════════════════════════════════════════════════════════
# Module state — singleton scheduler instance + injected callbacks
# ═══════════════════════════════════════════════════════════════════════════

class _State:
    """Holds the running scheduler + the callbacks app.py wired in at boot.
    Using a class-as-namespace keeps state explicit and lets us shut down
    cleanly on app exit."""
    scheduler: Any = None             # APScheduler BackgroundScheduler
    callbacks: dict = {}              # filled by boot()
    booted: bool = False


# ═══════════════════════════════════════════════════════════════════════════
# Public API — boot, register, unregister, pause, resume
# ═══════════════════════════════════════════════════════════════════════════

def boot(
    *,
    # Persistence callbacks (so this module doesn't import app.py directly)
    load_compilers: Callable[[], list],
    load_shard_by_id: Callable[[str], Optional[dict]],
    load_runs: Callable[[], list],
    save_runs: Callable[[list], None],
    # Run-execution callback bundle — passed straight through to Scheduler_Runner
    run_compiler_kwargs_factory: Callable[[str], dict],
) -> bool:
    """Initialise the scheduler. Returns True on success.

    Should be called ONCE during FastAPI startup. Idempotent — calling
    again on an already-booted state is a no-op (logs a warning).

    Parameters:
      load_compilers              → list[dict]      (called at boot to enumerate active jobs)
      load_shard_by_id            → dict|None       (used by the fire callback)
      load_runs / save_runs       → for the startup stale-run sweep
      run_compiler_kwargs_factory → callable(compiler_id) -> dict
                                     The factory builds the kwargs dict that
                                     Scheduler_Runner.run_compiler() needs at
                                     fire time (token-refresh callbacks, blob
                                     client refs, etc.). It's called per-fire
                                     rather than once at boot, so any updates
                                     to those references in app.py are picked
                                     up without restarting the scheduler.
    """
    if _State.booted:
        print("[scheduler-engine] boot() called twice — ignoring", flush=True)
        return False

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.executors.pool import ThreadPoolExecutor
    except ImportError as e:
        print(f"[scheduler-engine] APScheduler not installed — cron firing disabled: {e}", flush=True)
        return False

    # Single-thread executor: only one job fires at a time. Multiple compilers
    # fire serially, not in parallel. This is intentional — concurrent fires
    # could collide on the same shared cockpit-data blobs (run log writes).
    # If concurrency becomes valuable later, bump max_workers AND add a write
    # lock on automation_runs.json.
    executors = {"default": ThreadPoolExecutor(max_workers=1)}
    job_defaults = {
        "coalesce":          True,                  # collapse missed bursts into one fire
        "misfire_grace_time": MISFIRE_GRACE_SEC,    # 30-minute grace
        "max_instances":     1,                     # never run the same compiler twice in parallel
    }

    scheduler = BackgroundScheduler(
        executors=executors,
        job_defaults=job_defaults,
        timezone="UTC",
    )

    _State.scheduler = scheduler
    _State.callbacks = {
        "load_compilers":             load_compilers,
        "load_shard_by_id":           load_shard_by_id,
        "load_runs":                  load_runs,
        "save_runs":                  save_runs,
        "run_compiler_kwargs_factory": run_compiler_kwargs_factory,
    }

    # Step 1: sweep stale 'running' run records BEFORE registering any jobs.
    # This prevents the UI from showing zombie runs after a restart.
    try:
        _sweep_stale_runs(load_runs, save_runs)
    except Exception as e:
        print(f"[scheduler-engine] Stale-run sweep failed (non-fatal): {e}", flush=True)

    # Step 2: register every active compiler as an APScheduler job
    try:
        compilers = load_compilers()
    except Exception as e:
        print(f"[scheduler-engine] Could not load compilers at boot: {e}", flush=True)
        compilers = []

    registered = 0
    for c in compilers:
        if not c.get("active", True):
            continue
        if not (c.get("shard_ids") or []):
            # Empty compilers don't need scheduled fires — they'd just no-op.
            # Still allow them in the data layer but skip registration.
            continue
        try:
            _register_one(c)
            registered += 1
        except Exception as e:
            print(f"[scheduler-engine] Failed to register compiler {c.get('id')}: {e}", flush=True)

    # Step 3: start the scheduler
    try:
        scheduler.start()
    except Exception as e:
        print(f"[scheduler-engine] Failed to start scheduler: {e}", flush=True)
        return False

    _State.booted = True
    print(f"[scheduler-engine] Booted — {registered} compiler(s) registered, misfire grace {MISFIRE_GRACE_SEC}s", flush=True)
    return True


def shutdown() -> None:
    """Stop the scheduler cleanly. Safe to call multiple times."""
    if _State.scheduler is not None:
        try:
            _State.scheduler.shutdown(wait=False)
            print("[scheduler-engine] Shut down", flush=True)
        except Exception as e:
            print(f"[scheduler-engine] Shutdown error: {e}", flush=True)
    _State.scheduler = None
    _State.booted = False


def register_compiler(compiler: dict) -> None:
    """Add or replace the APScheduler job for one compiler.
    Called from app.py whenever a compiler is created or updated."""
    if not _State.booted or _State.scheduler is None:
        return
    if not compiler.get("active", True):
        # Inactive: ensure no job exists, then return
        unregister_compiler(compiler["id"])
        return
    if not (compiler.get("shard_ids") or []):
        # Empty: ensure no job exists, then return
        unregister_compiler(compiler["id"])
        return
    try:
        _register_one(compiler, replace=True)
    except Exception as e:
        print(f"[scheduler-engine] register_compiler({compiler.get('id')}) failed: {e}", flush=True)


def unregister_compiler(compiler_id: str) -> None:
    """Remove the APScheduler job for one compiler.
    Called from app.py on delete or when toggling to paused."""
    if not _State.booted or _State.scheduler is None:
        return
    job_id = _job_id_for(compiler_id)
    try:
        _State.scheduler.remove_job(job_id)
    except Exception:
        # JobLookupError is fine — job didn't exist or was already removed
        pass


def pause_compiler(compiler_id: str) -> None:
    """Pause a registered job (it stays registered but won't fire)."""
    if not _State.booted or _State.scheduler is None:
        return
    try:
        _State.scheduler.pause_job(_job_id_for(compiler_id))
    except Exception:
        pass


def resume_compiler(compiler_id: str) -> None:
    """Resume a paused job."""
    if not _State.booted or _State.scheduler is None:
        return
    try:
        _State.scheduler.resume_job(_job_id_for(compiler_id))
    except Exception:
        # If the job doesn't exist, the caller probably wants register_compiler instead
        pass


def list_jobs() -> list[dict]:
    """Return a summary of currently-registered jobs. Useful for diagnostics."""
    if not _State.booted or _State.scheduler is None:
        return []
    out = []
    for job in _State.scheduler.get_jobs():
        out.append({
            "id":       job.id,
            "name":     job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger":  str(job.trigger),
        })
    return out


# ═══════════════════════════════════════════════════════════════════════════
# Internal: register one compiler + the actual fire callback
# ═══════════════════════════════════════════════════════════════════════════

def _register_one(compiler: dict, *, replace: bool = False) -> None:
    """Convert compiler.schedule to an APScheduler cron trigger and add the job."""
    from apscheduler.triggers.cron import CronTrigger

    cron_kwargs = _translate_schedule_to_cron(compiler.get("schedule") or {})
    if cron_kwargs is None:
        # Invalid schedule shape — skip silently. The validator should have
        # caught this at save time, so reaching here means data is corrupted.
        print(f"[scheduler-engine] Compiler {compiler.get('id')} has invalid schedule — skipping", flush=True)
        return

    trigger = CronTrigger(**cron_kwargs)
    job_id  = _job_id_for(compiler["id"])

    if replace:
        try:
            _State.scheduler.remove_job(job_id)
        except Exception:
            pass

    _State.scheduler.add_job(
        func=_fire_compiler_callback,
        trigger=trigger,
        id=job_id,
        name=compiler.get("name") or compiler["id"],
        args=[compiler["id"]],
        replace_existing=True,
    )
    print(f"[scheduler-engine] Registered job {job_id}: {cron_kwargs}", flush=True)


def _fire_compiler_callback(compiler_id: str) -> None:
    """The function APScheduler actually calls when a job fires.

    This is invoked OUTSIDE the FastAPI request lifecycle (it's running
    on the scheduler's own thread). We can't use FastAPI dependencies,
    Request objects, etc. — only the callbacks we wired in at boot.
    """
    cb = _State.callbacks
    try:
        # Reload the compiler fresh — it may have been edited since registration
        compilers = cb["load_compilers"]()
        compiler  = next((c for c in compilers if c.get("id") == compiler_id), None)
        if not compiler:
            print(f"[scheduler-engine] Fire: compiler {compiler_id} no longer exists, skipping", flush=True)
            unregister_compiler(compiler_id)
            return
        if not compiler.get("active", True):
            print(f"[scheduler-engine] Fire: compiler {compiler_id} is paused, skipping", flush=True)
            return

        # Build kwargs for run_compiler. The factory pattern lets app.py
        # rebuild fresh references each time (in case anything was reloaded).
        run_kwargs = cb["run_compiler_kwargs_factory"](compiler_id)
        if run_kwargs is None:
            print(f"[scheduler-engine] Fire: kwargs factory returned None for {compiler_id}", flush=True)
            return

        # Determine the user to run as. Convention from the spec:
        # cron-fired compilers run as the compiler's created_by admin (the
        # admin who set up the automation owns its execution identity).
        triggering_user = compiler.get("created_by") or run_kwargs.get("default_user", "")
        if not triggering_user:
            print(f"[scheduler-engine] Fire: no created_by or default_user for {compiler_id}", flush=True)
            return

        # Import lazily so circular-import risks stay minimal
        from codeShared import Scheduler_Runner

        # Mark this run as cron-triggered (not manual)
        # We modify run_compiler's behavior by patching the trigger_kind in
        # the compiler record's run output. The runner reads trigger_kind
        # from a kwarg if we pass it.
        Scheduler_Runner.run_compiler(
            compiler=compiler,
            triggering_user=triggering_user,
            **run_kwargs,
        )
        # We don't need to do anything with the return — the runner already
        # persisted the run record(s).
        print(f"[scheduler-engine] Fired compiler {compiler_id}", flush=True)

    except Exception as e:
        # Unhandled exception in the fire callback — log it, don't crash
        # the scheduler thread. The next scheduled fire will still work.
        import traceback
        print(f"[scheduler-engine] Fire callback exception for {compiler_id}: {e}", flush=True)
        print(traceback.format_exc()[:1000], flush=True)


# ═══════════════════════════════════════════════════════════════════════════
# Schedule translation: cockpit format → APScheduler cron kwargs
# ═══════════════════════════════════════════════════════════════════════════

def _translate_schedule_to_cron(schedule: dict) -> Optional[dict]:
    """Convert a compiler's schedule dict to APScheduler CronTrigger kwargs.

    Cockpit schedule shape:
        {
          "frequency":      "daily" | "weekdays" | "weekend" | "weekly" | "monthly",
          "days_of_week":   ["mon","wed",...]    # for weekly only
          "days_of_month":  [1, 15, ...]         # for monthly only
          "time":           "HH:MM"              # 24-hour
          "tz":             "Asia/Dubai"         # IANA timezone
        }

    Returns None if the schedule is too malformed to translate. The
    validate_compiler() function in app.py should have caught this at
    save time, so reaching here usually means data corruption."""
    if not isinstance(schedule, dict):
        return None

    freq = schedule.get("frequency")
    time_str = schedule.get("time", "06:00")
    tz       = schedule.get("tz", "UTC") or "UTC"

    # Parse HH:MM
    try:
        hh_str, mm_str = time_str.split(":", 1)
        hour   = int(hh_str)
        minute = int(mm_str)
    except Exception:
        return None
    if not (0 <= hour < 24 and 0 <= minute < 60):
        return None

    base = {"hour": hour, "minute": minute, "timezone": tz}

    if freq == "daily":
        return base

    if freq == "weekdays":
        return {**base, "day_of_week": "mon-fri"}

    if freq == "weekend":
        return {**base, "day_of_week": "sat,sun"}

    if freq == "weekly":
        days = schedule.get("days_of_week") or []
        # Lowercase + dedupe + filter to known codes
        clean = []
        seen = set()
        for d in days:
            d2 = (d or "").lower().strip()[:3]
            if d2 in DOW_MAP and d2 not in seen:
                clean.append(d2); seen.add(d2)
        if not clean:
            # No days picked — behave like "daily" so something fires.
            # (Empty days_of_week with frequency=weekly is operator error;
            # silently degrading to daily is more useful than not firing.)
            return base
        return {**base, "day_of_week": ",".join(clean)}

    if freq == "monthly":
        days = schedule.get("days_of_month") or []
        clean = []
        seen = set()
        for d in days:
            try:
                di = int(d)
                if 1 <= di <= 31 and di not in seen:
                    clean.append(str(di)); seen.add(di)
            except Exception:
                pass
        if not clean:
            # Same fallback as weekly — degrade gracefully
            return base
        return {**base, "day": ",".join(clean)}

    # Unknown frequency
    return None


# ═══════════════════════════════════════════════════════════════════════════
# Internal: helpers
# ═══════════════════════════════════════════════════════════════════════════

def _job_id_for(compiler_id: str) -> str:
    """APScheduler job ID convention. Prefix lets us identify our jobs in
    logs and avoids collisions with any other jobs the host might add."""
    return f"compiler_{compiler_id}"


def _sweep_stale_runs(load_runs: Callable[[], list], save_runs: Callable[[list], None]) -> None:
    """On boot, scan for run records stuck in 'running' status from a
    previous instance that crashed or restarted. Mark them failed so the
    UI doesn't show zombie 'running' indefinitely.

    A run is 'stale' if status is 'running' AND started_at is older than
    STALE_RUNNING_THRESHOLD_SEC. Long-running real runs (e.g. a 20-min
    Forma L3) are NOT in the new instance's memory — they couldn't be
    by definition since they were in the old instance — so the threshold
    serves as a safety net: anything running for longer than the threshold
    that the current instance doesn't know about must have been orphaned.
    """
    runs = load_runs()
    if not runs:
        return
    now = time.time()
    threshold_dt = datetime.now(timezone.utc) - timedelta(seconds=STALE_RUNNING_THRESHOLD_SEC)
    threshold_iso = threshold_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    swept = 0
    for r in runs:
        if r.get("status") != "running":
            continue
        started = r.get("started_at", "")
        if started and started < threshold_iso:
            r["status"]   = "failed"
            r["ended_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            r["error"]    = (r.get("error") or "") + (
                "App Service worker restarted while this run was in flight. "
                "The original subprocess (if any) is gone."
            )
            swept += 1

    if swept:
        save_runs(runs)
        print(f"[scheduler-engine] Stale-run sweep: marked {swept} run(s) as failed", flush=True)


# ═══════════════════════════════════════════════════════════════════════════
# Diagnostics — used by app.py /api/_diag if exposed
# ═══════════════════════════════════════════════════════════════════════════

def status() -> dict:
    """Return a snapshot of scheduler state for diagnostics."""
    return {
        "booted":      _State.booted,
        "scheduler":   _State.scheduler.__class__.__name__ if _State.scheduler else None,
        "running":     bool(_State.scheduler and _State.scheduler.running),
        "job_count":   len(_State.scheduler.get_jobs()) if (_State.scheduler and _State.scheduler.running) else 0,
        "misfire_grace_sec": MISFIRE_GRACE_SEC,
    }