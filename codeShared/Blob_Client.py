"""
Blob_Client.py
Dedicated Azure Blob Storage wrapper for scheduler export files.

This module is separate from app.py's existing blob client (which manages
permanent runtime state in the `cockpit-data` container). Here we only
deal with the three new EXPORT containers:

  - forma-exports
  - revizto-exports
  - morta-exports

These containers have a 15-day lifecycle rule applied at the storage account
level, so any blob older than 15 days is auto-deleted. Don't put anything
here that needs to persist longer than that.

Auth is via the same `AZURE_STORAGE_CONNECTION_STRING` env var that the
cockpit already uses — they point to the same storage account, just
different containers.

Public API (intentionally small):

  upload_blob(container, blob_name, content) -> dict
      Upload bytes/str/file-path to a container. Returns metadata about
      the upload.

  generate_download_url(container, blob_name, expiry_hours=24) -> str
      Return a signed SAS URL that lets a browser download the blob
      without needing the connection string. Expires after the given
      number of hours (default 24).

  list_blobs(container, prefix="") -> list[dict]
      List blobs in a container, optionally filtered by name prefix.
      Returns name, size, last_modified for each.

  delete_blob(container, blob_name) -> bool
      Delete a blob. Returns True if deleted, False if not found.

  is_available() -> bool
      Returns True if the env var is set and the SDK is importable.
      Use this for graceful degradation in callers.

Errors:
    Operations raise the underlying azure.core exceptions on failure
    (e.g. ResourceNotFoundError, ServiceRequestError). Callers should
    catch and translate as needed.
"""

import os
import io
import logging
from datetime import datetime, timedelta, timezone
from typing import Union, Optional

# Containers used by the scheduler. Listed here so callers can reference
# them by name without typo risk; the scheduler picks one based on the
# shard's platform.
CONTAINER_FORMA   = "forma-exports"
CONTAINER_REVIZTO = "revizto-exports"
CONTAINER_MORTA   = "morta-exports"

VALID_CONTAINERS = {CONTAINER_FORMA, CONTAINER_REVIZTO, CONTAINER_MORTA}

# Module-level lazy state. The first call instantiates the client; later
# calls reuse it. Same pattern as app.py's existing client to avoid
# repeating the import + connection cost on every call.
_blob_service_client = None
_account_name = None        # cached for SAS-URL generation
_account_key = None         # cached for SAS-URL generation

logger = logging.getLogger(__name__)


def _get_connection_string() -> Optional[str]:
    """Look up the connection string from the environment. Returns None if
    the env var isn't set; callers should treat that as a soft failure
    (storage unavailable) rather than a hard crash."""
    return os.environ.get("AZURE_STORAGE_CONNECTION_STRING") or None


def _parse_connection_string(conn_str: str) -> tuple[Optional[str], Optional[str]]:
    """Pull AccountName and AccountKey out of a standard Azure connection
    string. Used for generating SAS URLs (which need the key explicitly).
    Returns (account_name, account_key) or (None, None) if either part
    is missing."""
    parts = {}
    for pair in conn_str.split(";"):
        if "=" in pair:
            k, _, v = pair.partition("=")
            parts[k.strip()] = v.strip()
    return parts.get("AccountName"), parts.get("AccountKey")


def is_available() -> bool:
    """Cheap check that doesn't touch the network. Returns True if we
    have a connection string and the SDK is importable. Used by app.py
    to decide whether to expose scheduler endpoints at all."""
    if not _get_connection_string():
        return False
    try:
        import azure.storage.blob   # noqa: F401
        return True
    except ImportError:
        return False


def _get_client():
    """Return the cached BlobServiceClient, creating it on first call.
    Imports happen inside the function so that environments without
    azure-storage-blob installed can still import this module without
    crashing — they just can't use the upload/download functions."""
    global _blob_service_client, _account_name, _account_key
    if _blob_service_client is not None:
        return _blob_service_client

    conn_str = _get_connection_string()
    if not conn_str:
        raise RuntimeError(
            "AZURE_STORAGE_CONNECTION_STRING env var is not set. "
            "Scheduler export uploads cannot proceed without it."
        )

    from azure.storage.blob import BlobServiceClient
    _blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    _account_name, _account_key = _parse_connection_string(conn_str)
    return _blob_service_client


def _validate_container(container: str) -> None:
    """Reject calls to containers we didn't intend to touch. The
    cockpit's runtime-state container `cockpit-data` is OFF LIMITS for
    this module — that's owned by app.py. Adding a check here catches
    accidents before they can corrupt state."""
    if container not in VALID_CONTAINERS:
        raise ValueError(
            f"Container {container!r} is not a scheduler export container. "
            f"Allowed: {sorted(VALID_CONTAINERS)}. "
            f"Note: 'cockpit-data' is reserved for app.py and must not be used here."
        )


def upload_blob(container: str, blob_name: str,
                content: Union[bytes, str, "io.IOBase"]) -> dict:
    """Upload content to a blob.

    Args:
        container: One of CONTAINER_FORMA / CONTAINER_REVIZTO / CONTAINER_MORTA.
        blob_name: Path-like name within the container, e.g.
            "2026-05-05/shard_abc/wynn_report.json". Slashes are folders
            (Azure renders them in the portal as virtual directories).
        content: Either a bytes object, a string (encoded as UTF-8), or
            a file-like object opened in binary mode. Strings longer than
            a few MB should be encoded by the caller and passed as bytes
            for memory efficiency.

    Returns:
        dict with keys: container, name, size_bytes, etag, last_modified.

    Raises:
        ValueError if container is invalid.
        RuntimeError if connection string isn't set.
        azure.core exceptions on transport failures.
    """
    _validate_container(container)

    # Normalise content into something BlobClient.upload_blob accepts.
    # The SDK accepts bytes/str/file-like directly, so we just track the
    # size for our return value. For strings we don't pre-encode (the
    # SDK does it) but we estimate size for the return.
    if isinstance(content, str):
        size_estimate = len(content.encode("utf-8"))
    elif isinstance(content, bytes):
        size_estimate = len(content)
    else:
        # File-like — try to seek to determine size, fall back to None
        size_estimate = None
        try:
            cur = content.tell()
            content.seek(0, io.SEEK_END)
            size_estimate = content.tell() - cur
            content.seek(cur)
        except (AttributeError, OSError):
            pass

    svc = _get_client()
    blob = svc.get_blob_client(container=container, blob=blob_name)

    # overwrite=True: if a shard re-runs, the new file replaces the old.
    # If the caller wants append-mode (new file each time), they should
    # pass a unique blob_name (e.g. include a timestamp).
    result = blob.upload_blob(content, overwrite=True)

    return {
        "container":     container,
        "name":          blob_name,
        "size_bytes":    size_estimate,
        "etag":          (result or {}).get("etag") if isinstance(result, dict) else getattr(result, "etag", None),
        "last_modified": _utc_now_iso(),
    }


def upload_file(container: str, blob_name: str, file_path: str) -> dict:
    """Convenience wrapper: open a file in binary mode and upload it.
    Useful when callers have produced an output on disk and just want
    it shipped to blob without buffering it all into memory.

    Returns the same shape as upload_blob().
    """
    _validate_container(container)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Cannot upload — file not found: {file_path}")

    size = os.path.getsize(file_path)
    svc = _get_client()
    blob = svc.get_blob_client(container=container, blob=blob_name)

    with open(file_path, "rb") as fh:
        result = blob.upload_blob(fh, overwrite=True)

    return {
        "container":     container,
        "name":          blob_name,
        "size_bytes":    size,
        "etag":          (result or {}).get("etag") if isinstance(result, dict) else getattr(result, "etag", None),
        "last_modified": _utc_now_iso(),
    }


def generate_download_url(container: str, blob_name: str,
                          expiry_hours: int = 24) -> str:
    """Generate a SAS URL that lets a browser download a specific blob
    over HTTPS without needing the storage credentials. The URL embeds
    a time-bounded signature.

    Args:
        container: Container name.
        blob_name: Blob to grant read access to.
        expiry_hours: How long the URL stays valid. Defaults to 24h —
            long enough for users to click an emailed link the next day,
            short enough to limit damage if a URL leaks.

    Returns:
        Full https URL with SAS query string appended.

    Notes:
        Uses the storage account key for signing (account-key SAS, not
        user-delegation SAS). This is fine for our use case — the key
        is only available to the App Service via env var and is rotated
        if compromised.
    """
    _validate_container(container)
    _get_client()      # ensures _account_name/_account_key are populated

    if not _account_name or not _account_key:
        raise RuntimeError(
            "Could not parse AccountName/AccountKey from connection string. "
            "SAS URL generation requires both."
        )

    from azure.storage.blob import generate_blob_sas, BlobSasPermissions

    expiry = datetime.now(timezone.utc) + timedelta(hours=expiry_hours)
    sas_token = generate_blob_sas(
        account_name = _account_name,
        container_name = container,
        blob_name = blob_name,
        account_key = _account_key,
        permission = BlobSasPermissions(read=True),
        expiry = expiry,
    )
    return (
        f"https://{_account_name}.blob.core.windows.net/"
        f"{container}/{blob_name}?{sas_token}"
    )


def list_blobs(container: str, prefix: str = "") -> list[dict]:
    """List blobs in a container, optionally filtered by name prefix.

    Returns a list of dicts each with: name, size_bytes, last_modified.
    For a 15-day lifecycle policy this list stays small in practice;
    we don't paginate, but a hard cap of 1000 entries protects against
    runaway listings.
    """
    _validate_container(container)
    svc = _get_client()
    container_client = svc.get_container_client(container)

    out = []
    for i, blob in enumerate(container_client.list_blobs(name_starts_with=prefix)):
        if i >= 1000:
            logger.warning("list_blobs hit 1000-entry cap for container=%s prefix=%s",
                           container, prefix)
            break
        out.append({
            "name":          blob.name,
            "size_bytes":    blob.size,
            "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
        })
    return out


def delete_blob(container: str, blob_name: str) -> bool:
    """Delete a blob. Returns True if it existed and was deleted, False
    if the blob wasn't found. Other errors propagate."""
    _validate_container(container)
    svc = _get_client()
    blob = svc.get_blob_client(container=container, blob=blob_name)
    try:
        blob.delete_blob()
        return True
    except Exception as e:
        # Translate "blob not found" into False; re-raise everything else.
        # Imported lazily to avoid forcing the import on module load.
        from azure.core.exceptions import ResourceNotFoundError
        if isinstance(e, ResourceNotFoundError):
            return False
        raise


def container_for_platform(platform: str) -> str:
    """Map a platform name (used in shards) to its export container.
    Encapsulates the convention so callers don't repeat the if/elif."""
    p = (platform or "").lower()
    if p == "forma":   return CONTAINER_FORMA
    if p == "revizto": return CONTAINER_REVIZTO
    if p == "morta":   return CONTAINER_MORTA
    raise ValueError(f"Unknown platform {platform!r}; expected forma/revizto/morta")


def _utc_now_iso() -> str:
    """Helper for return values — current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat()


# ── Self-test entry point ──────────────────────────────────────────────────
# When run directly (python Blob_Client.py), exercise the API end-to-end
# against the live storage account. Useful for verifying setup post-deploy.

if __name__ == "__main__":
    import sys
    print("Blob_Client self-test")
    print("=" * 50)

    if not is_available():
        print("FAIL: storage not available")
        print(f"  AZURE_STORAGE_CONNECTION_STRING set: "
              f"{bool(_get_connection_string())}")
        try:
            import azure.storage.blob   # noqa: F401
            print("  azure-storage-blob installed: yes")
        except ImportError:
            print("  azure-storage-blob installed: NO — run pip install azure-storage-blob")
        sys.exit(1)

    print("OK: storage is available")
    name, _ = _parse_connection_string(_get_connection_string() or "")
    print(f"  AccountName: {name}")

    # Upload a test blob to forma-exports
    test_name = f"_selftest/{int(datetime.now().timestamp())}.txt"
    test_payload = f"Self-test from Blob_Client at {_utc_now_iso()}\n".encode("utf-8")

    print(f"\nUploading test blob: {CONTAINER_FORMA}/{test_name}")
    result = upload_blob(CONTAINER_FORMA, test_name, test_payload)
    print(f"  size_bytes: {result['size_bytes']}")
    print(f"  etag: {result['etag']}")

    print(f"\nGenerating download URL (expires in 1h)…")
    url = generate_download_url(CONTAINER_FORMA, test_name, expiry_hours=1)
    print(f"  {url[:80]}…")

    print(f"\nListing blobs under prefix '_selftest/'…")
    items = list_blobs(CONTAINER_FORMA, prefix="_selftest/")
    print(f"  found {len(items)} item(s)")
    for it in items[-3:]:
        print(f"    {it['name']}  ({it['size_bytes']}B)")

    print(f"\nDeleting test blob…")
    deleted = delete_blob(CONTAINER_FORMA, test_name)
    print(f"  deleted: {deleted}")

    # Test the validation rejects the runtime-state container
    print(f"\nValidating that 'cockpit-data' is rejected…")
    try:
        upload_blob("cockpit-data", "should-fail.txt", b"x")
        print("  FAIL: should have raised ValueError")
        sys.exit(2)
    except ValueError as e:
        print(f"  OK: ValueError raised as expected")

    print("\n" + "=" * 50)
    print("Self-test passed.")