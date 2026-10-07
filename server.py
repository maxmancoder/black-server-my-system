#!/usr/bin/env python3
"""Public File Download Server for Windows.

Serves files over HTTP, bound to ``127.0.0.1`` only, and publishes it to the
internet through a Cloudflare Quick Tunnel (``cloudflared``).

Storage lives in **Neon** and nowhere else:

    Internet
       |
       v
    Cloudflare Quick Tunnel   (cloudflared, local publishing only)
       |
       v
    127.0.0.1:<port>          (this file server)
       |
       v
    Neon Object Storage       (file bytes, S3-compatible, path-style)
    Neon Postgres             (file metadata)

``neon_store`` owns the Neon plumbing: uploaded bytes go to Object Storage and
the metadata row (key, name, size, type, created date, user) goes to Postgres.
Downloads are handed out as short-lived ``generate_presigned_url`` links, so the
bytes never travel through this process.  Nothing is written to server disk in
Neon mode.

When no Neon environment variables are configured the server keeps its original
local behaviour and serves ``./downloads`` from disk, which is what the test
suite and the desktop workflow use.  On a PaaS host a half-configured Neon
environment is a hard error instead of a silent fallback, so the cloud
deployment can never quietly start writing to an ephemeral disk.

The HTTP layer needs only the standard library; Neon support adds the
``boto3`` / ``psycopg`` requirements.  The program works as a small orchestrator:
it starts the local file server in a background thread, spawns ``cloudflared``
as a child process, captures its output, extracts the public
``https://*.trycloudflare.com`` URL and prints a clear banner.

The design keeps the tunneling layer isolated (:class:`CloudflareTunnel`) so
that switching to a *named / managed* Cloudflare Tunnel later (for a custom
domain such as ``files.example.com``) only requires changing the command that is
launched, not the rest of the project.
"""

from __future__ import annotations

import argparse
import functools
import gzip
import html
import io
import json
import logging
import os
import re
import shutil
import signal
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
import webbrowser
import zipfile
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Tuple

try:  # Neon storage extras (boto3 + psycopg); absent -> local disk mode
    import neon_store as neon
except Exception:  # noqa: BLE001 - never block startup on the optional backend
    neon = None

# ---------------------------------------------------------------------------
# Paths and constants (everything is relative to the project directory, so the
# project can live anywhere and be launched from any working directory).
# ---------------------------------------------------------------------------

PROJECT_ROOT: Path = Path(__file__).resolve().parent
DOWNLOADS_DIR: Path = PROJECT_ROOT / "downloads"
LOGS_DIR: Path = PROJECT_ROOT / "logs"
BIN_DIR: Path = PROJECT_ROOT / "bin"
CLOUDFLARED_EXE: Path = BIN_DIR / "cloudflared.exe"
LOCK_FILE: Path = LOGS_DIR / "server.lock"

HOST: str = "127.0.0.1"          # bind to localhost only, never 0.0.0.0
DEFAULT_PORT: int = 8080
PORT_SCAN_LIMIT: int = 20        # how many ports to try after the default one
CHUNK_SIZE: int = 256 * 1024     # streaming chunk size (keeps RAM usage flat)
UPLOAD_CHUNK: int = 512 * 1024   # larger reads while receiving uploads

# Resumable uploads: the browser splits a file into parts of this size and sends
# them one request at a time, so pausing only loses the part in flight. 8 MiB is
# comfortably above the 5 MiB minimum that S3-style multipart APIs require.
UPLOAD_PART: int = 8 * 1024 * 1024
UPLOAD_PART_SPILL: int = 4 * 1024 * 1024   # spool this many bytes in RAM per part
UPLOAD_SESSION_TTL: float = 24 * 3600.0     # drop forgotten sessions after a day


def _env_flag(name: str) -> bool:
    """True when an environment variable looks like a boolean switch."""
    return (os.environ.get(name) or "").strip().lower() in {"1", "true", "yes", "on"}


# PaaS hosts (Render, Replit, Koyeb, Railway, ...) inject PORT, route traffic to
# whatever the process listens on (so it must be 0.0.0.0) and hand out the
# public URL themselves - no quick tunnel, no browser, no instance lock.
# Render sets RENDER=true; BS_PAAS / BS_HOST are the explicit opt-ins used by
# the bundled render.yaml, .replit and Procfile.  Desktop stays the default.
PAAS: bool = (
    _env_flag("RENDER")
    or _env_flag("RENDER_CLOUD")
    or bool((os.environ.get("BS_PAAS") or "").strip())
    or bool((os.environ.get("BS_HOST") or "").strip())
)
IS_CLOUD: bool = PAAS          # CLI-layer alias (--cloud)
PAAS_HOST: str = os.environ.get("BS_HOST", "0.0.0.0")

# Background job store for long-running move/copy (avoids Cloudflare tunnel timeouts).
_JOBS: dict = {}
_JOBS_LOCK = threading.Lock()
_SIDEBAR_CACHE: dict = {"html": None, "ts": 0.0, "for": None}
_SIDEBAR_TTL = 4.0

# Neon storage backend (bytes in Object Storage, metadata in Postgres).
# Resolved once on first use; ``None`` means "serve ./downloads from disk".
_STORAGE = None
_STORAGE_READY = False
_STORAGE_LOCK = threading.Lock()

# In-flight resumable uploads. A session owns the destination name, the S3
# multipart id (object storage) or the scratch file on disk (local mode), plus
# the parts the client has already acked so "resume" continues where it stopped.
_UPLOADS: dict = {}
_UPLOADS_LOCK = threading.Lock()


class UploadSession:
    """Server-side state for one resumable (chunked) upload."""

    __slots__ = ("sid", "store", "target", "name", "rel", "user",
                 "upload_id", "scratch", "size", "ctype", "parts", "touched")

    def __init__(self, sid: str, store, target: Path, name: str, rel: str,
                 user: str, size: int, ctype: str) -> None:
        self.sid = sid
        self.store = store
        self.target = target          # directory the file lands in (local mode)
        self.name = name              # sanitised destination file name
        self.rel = rel                # storage key prefix (object mode)
        self.user = user
        self.upload_id: str = ""      # provider multipart id (object mode)
        self.scratch: Optional[Path] = None   # partial file (local mode)
        self.size = size              # total expected bytes
        self.ctype = ctype
        self.parts: dict = {}         # part index (0-based) -> ETag/size
        self.touched = time.monotonic()

    @property
    def key(self) -> str:
        """Object key of the finished file (only valid in object mode)."""
        return self.store.child_key(self.rel, self.name) if self.store else ""

    def part_count(self) -> int:
        return max(1, (self.size + UPLOAD_PART - 1) // UPLOAD_PART)


def _upload_session(sid: str) -> Optional[UploadSession]:
    """Look up a live session and age out abandoned ones as a side effect."""
    if not re.fullmatch(r"[0-9a-f]{32}", str(sid or "")):
        return None
    now = time.monotonic()
    with _UPLOADS_LOCK:
        for dead_sid, dead in list(_UPLOADS.items()):
            if now - dead.touched > UPLOAD_SESSION_TTL:
                _UPLOADS.pop(dead_sid, None)
                _upload_discard(dead)
        sess = _UPLOADS.get(sid)
        if sess is not None:
            sess.touched = now
        return sess


def _upload_forget(sid: str) -> Optional[UploadSession]:
    with _UPLOADS_LOCK:
        return _UPLOADS.pop(sid, None)


def _upload_discard(sess: UploadSession) -> None:
    """Best-effort teardown of an unfinished upload (no response is sent)."""
    if sess.store is not None and sess.upload_id:
        try:
            sess.store.abort_multipart(sess.key, sess.upload_id)
        except Exception:  # noqa: BLE001 - teardown must never raise
            logger.warning("abort multipart failed for %s", sess.key, exc_info=True)
    if sess.scratch is not None:
        try:
            sess.scratch.unlink()
        except OSError:
            pass


def storage():
    """Return the Neon backend, or ``None`` when it is not configured.

    A partially configured environment is fatal on a PaaS host (the cloud
    deployment must never silently fall back to an ephemeral disk) and a loud
    warning locally, where the disk backend is still a valid dev mode.
    """
    global _STORAGE, _STORAGE_READY
    if neon is None or _STORAGE_READY:
        return _STORAGE
    with _STORAGE_LOCK:
        if not _STORAGE_READY:
            try:
                _STORAGE = neon.get_store()
            except Exception as exc:  # noqa: BLE001 - surfaced, never swallowed
                if PAAS:
                    raise
                logging.getLogger("public-download-server").warning(
                    "Neon storage unavailable (%s); serving from local disk.", exc
                )
                _STORAGE = None
            _STORAGE_READY = True
    return _STORAGE


def storage_active() -> bool:
    """True when files live in Neon rather than on local disk."""
    try:
        return storage() is not None
    except Exception:  # noqa: BLE001
        return False

# Official, always-latest Cloudflare release asset for 64-bit Windows.
CLOUDFLARED_DOWNLOAD_URL: str = (
    "https://github.com/cloudflare/cloudflared/releases/latest/download/"
    "cloudflared-windows-amd64.exe"
)

# Matches the Quick Tunnel URL printed by cloudflared, e.g.
#   https://random-name-here.trycloudflare.com
#
# Requires at least one hyphen in the sub-domain.  This deliberately excludes
# infrastructure hosts such as "api.trycloudflare.com" or "edge.trycloudflare.com"
# that cloudflared also logs, so only real per-tunnel URLs are captured.
TUNNEL_URL_RE: re.Pattern[str] = re.compile(
    r"https://[a-zA-Z0-9]+(?:-[a-zA-Z0-9]+)+\.trycloudflare\.com"
)

# Lines that mean the quick tunnel could not be created at all.
TUNNEL_FATAL_RE: re.Pattern[str] = re.compile(
    r"failed to (?:request|create).*tunnel|Failed to create quick tunnel|"
    r"fatal|INTERNAL ERROR",
    re.IGNORECASE,
)

# --- SSH fallback tunnel providers -------------------------------------------
# Used automatically when Cloudflare is blocked (e.g. national filters).
# Each provider only needs an outbound SSH connection; the URL regex and
# command differ per provider.  Providers are tried in order until one works.
SSH_STARTUP_TIMEOUT: float = 45.0
SSH_REACH_TRIES: int = 6
SSH_REACH_DELAY: float = 4.0


def _ssh_cmd(dest: str, port: int, remote_port: str = "80") -> list:
    """Build a standard SSH reverse-tunnel command."""
    return [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "ServerAliveInterval=5",
        "-o", "ServerAliveCountMax=3",
        "-o", "ExitOnForwardFailure=yes",
        "-R", f"{remote_port}:127.0.0.1:{port}",
        dest,
        "-p", "22",
    ]


# Ordered list of SSH tunnel providers (tried first → last).
SSH_PROVIDERS: list[dict] = [
    {
        "name": "localhost.run",
        "log_name": "localhostrun.log",
        "url_re": re.compile(r"https://[a-zA-Z0-9-]+\.lhr\.life"),
        "cmd": lambda port: _ssh_cmd("nokey@localhost.run", port),
    },
    {
        "name": "serveo.net",
        "log_name": "serveo.log",
        "url_re": re.compile(r"https://[a-zA-Z0-9.-]+\.serveousercontent\.com"),
        "cmd": lambda port: _ssh_cmd("serveo.net", port),
    },
]

# ANSI escape sequences that cloudflared uses to colour its log output.
ANSI_RE: re.Pattern[str] = re.compile(r"\x1b\[[0-9;]*m")

logger = logging.getLogger("public-download-server")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def setup_logging(verbose: bool = False) -> None:
    """Configure console + rotating file logging (never logs secrets)."""
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    file_handler = RotatingFileHandler(
        LOGS_DIR / "server.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    console_handler.setLevel(logging.DEBUG if verbose else logging.WARNING)
    logger.addHandler(console_handler)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def human_size(num_bytes: int) -> str:
    """Return a human readable file size such as ``4.2 GB``."""
    size = float(num_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _folder_stats(folder: Path, *, max_depth: int = 6, max_files: int = 20000):
    """Return (file_count, total_bytes) for files under *folder* (recursive)."""
    count = 0
    total = 0
    stack: list[tuple[Path, int]] = [(folder, 0)]
    while stack and count < max_files:
        current_dir, depth = stack.pop()
        if depth > max_depth:
            continue
        try:
            with os.scandir(current_dir) as it:
                for entry in it:
                    if entry.name.startswith("."):
                        continue
                    try:
                        if entry.is_dir(follow_symlinks=False):
                            if depth < max_depth:
                                stack.append((Path(entry.path), depth + 1))
                        elif entry.is_file(follow_symlinks=False):
                            count += 1
                            total += entry.stat(follow_symlinks=False).st_size
                            if count >= max_files:
                                break
                    except OSError:
                        continue
        except OSError:
            continue
    return count, total


def folder_stat(rel: str):
    """Neon metadata row for a relative path, or ``None`` in disk mode."""
    store = storage()
    if store is None:
        return None
    return store.entry("/" + str(rel).strip("/"))


def sidebar_folders(store) -> list[tuple[str, str]]:
    """Every folder as ``(name, rel_path)``, used to build the sidebar tree."""
    return [
        (path.rstrip("/").rsplit("/", 1)[-1] or "/", path)
        for path in store.sidebar_paths()
    ]


def is_process_running(pid: int) -> bool:
    """Best-effort check whether *pid* is still alive (Windows friendly)."""
    if pid <= 0:
        return False
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return str(pid) in (result.stdout or "")


def default_bind_host() -> str:
    """Interface to listen on: ``HOST``/``BS_HOST`` env, else 0.0.0.0 in the cloud."""
    env = (os.environ.get("HOST") or "").strip()
    if env:
        return env
    return PAAS_HOST if PAAS else HOST


def bind_port(default: int) -> int:
    """Preferred port: a PaaS-provided ``PORT`` always wins and never shifts."""
    raw = (os.environ.get("PORT") or "").strip()
    if PAAS and raw.isdigit() and 0 < int(raw) < 65536:
        return int(raw)
    return default


def acquire_single_instance_lock() -> Tuple[bool, Optional[int]]:
    """Prevent two servers from running at once.

    Returns ``(True, own_pid)`` when the lock was acquired, otherwise
    ``(False, existing_pid)``.
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_FILE.exists():
        try:
            existing = int(LOCK_FILE.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            existing = 0
        if existing and is_process_running(existing):
            return False, existing
    try:
        LOCK_FILE.write_text(str(os.getpid()), encoding="utf-8")
    except OSError:
        logger.warning("Could not write lock file %s", LOCK_FILE)
    return True, os.getpid()


def release_single_instance_lock() -> None:
    """Remove the lock file on a clean shutdown."""
    try:
        if LOCK_FILE.exists():
            LOCK_FILE.unlink()
    except OSError:
        logger.warning("Could not remove lock file %s", LOCK_FILE)


def find_free_port(preferred: int = DEFAULT_PORT) -> int:
    """Return the first free TCP port on localhost.

    Tries ``preferred`` first, then the following ports, and finally lets the
    OS pick an ephemeral port.  This keeps the server and cloudflared in sync
    because the chosen port is returned to the caller.
    """
    candidates = list(range(preferred, preferred + PORT_SCAN_LIMIT))
    candidates.append(0)  # let the OS choose a free port

    for port in candidates:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind((HOST, port))
            except OSError:
                continue
            return probe.getsockname()[1]
    raise OSError("No free TCP port could be found on localhost.")


def wait_for_port(host: str, port: int, timeout: float = 10.0) -> bool:
    """Poll a TCP endpoint until it accepts connections or *timeout* expires."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(0.5)
            if probe.connect_ex((host, port)) == 0:
                return True
        time.sleep(0.15)
    return False


def check_internet(timeout: float = 4.0) -> bool:
    """Quick reachability check so we can print a helpful warning.

    This is only a heuristic (many networks block ICMP/DNS of well-known
    hosts yet still allow the Cloudflare edge), so a ``False`` result never
    blocks the tunnel - it only produces a warning.
    """
    for target in (
        ("1.1.1.1", 443),
        ("8.8.8.8", 53),
        ("www.cloudflare.com", 443),
    ):
        try:
            with socket.create_connection(target, timeout=timeout):
                return True
        except OSError:
            continue
    return False


def copy_to_clipboard(text: str) -> bool:
    """Put *text* on the Windows clipboard (best effort, never fatal)."""
    try:
        subprocess.run(
            "clip",
            input=text,
            text=True,
            shell=True,
            check=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return True
    except Exception:  # noqa: BLE001 - clipboard failures must not crash us
        pass
    try:
        subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-Command",
                "Set-Clipboard -Value $input",
            ],
            input=text,
            text=True,
            check=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return True
    except Exception:  # noqa: BLE001
        return False


# ---------------------------------------------------------------------------
# cloudflared management
# ---------------------------------------------------------------------------

def print_log_tail(log_path: Path, max_lines: int = 12) -> None:
    """Print the tail of a tunnel log to help the user self-diagnose."""
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return
    for line in lines[-max_lines:]:
        if line.strip():
            print("        " + line.strip()[:200])


def verify_cloudflared(exe: Path) -> Optional[str]:
    """Run ``cloudflared --version`` and return the version string."""
    try:
        result = subprocess.run(
            [str(exe), "--version"],
            capture_output=True,
            text=True,
            timeout=20,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.error("cloudflared could not be executed: %s", exc)
        return None
    output = (result.stdout or result.stderr or "").strip()
    if result.returncode != 0:
        logger.error("cloudflared --version failed: %s", output)
        return None
    return output.splitlines()[0] if output else "cloudflared"


def download_cloudflared() -> Optional[str]:
    """Download cloudflared from the official Cloudflare/GitHub release.

    The file is written to a temporary path first and only moved into place
    after the download looks valid, so a failed download never leaves a broken
    ``bin/cloudflared.exe`` behind.
    """
    BIN_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = CLOUDFLARED_EXE.with_suffix(".exe.download")

    print("[..] Downloading cloudflared from Cloudflare/GitHub ...")
    request = urllib.request.Request(
        CLOUDFLARED_DOWNLOAD_URL,
        headers={"User-Agent": "public-download-server/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response, open(
            tmp_path, "wb"
        ) as out_file:
            shutil.copyfileobj(response, out_file, length=CHUNK_SIZE)
    except Exception as exc:  # noqa: BLE001 - report any network error clearly
        logger.error("cloudflared download failed: %s", exc)
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    # Sanity check: a Windows executable starts with the "MZ" magic bytes.
    try:
        with open(tmp_path, "rb") as probe:
            if probe.read(2) != b"MZ":
                raise ValueError("downloaded file is not a valid Windows executable")
        os.replace(tmp_path, CLOUDFLARED_EXE)
    except (OSError, ValueError) as exc:
        logger.error("cloudflared download is invalid: %s", exc)
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass
        return None

    print(f"[OK] Downloaded to {CLOUDFLARED_EXE}")
    return verify_cloudflared(CLOUDFLARED_EXE)


def ensure_cloudflared(force_download: bool = False) -> Optional[Path]:
    """Make sure ``bin/cloudflared.exe`` exists and runs.

    Returns the path on success or ``None`` on failure.
    """
    if CLOUDFLARED_EXE.exists() and not force_download:
        version = verify_cloudflared(CLOUDFLARED_EXE)
        if version:
            print(f"[OK] cloudflared found: {version}")
            return CLOUDFLARED_EXE
        logger.warning("Existing cloudflared binary is not usable; re-downloading.")

    # No hard internet pre-check here: connectivity probes can give false
    # negatives, and the download below reports a clear error if it fails.
    version = download_cloudflared()
    if not version:
        print("[ERROR] Could not download cloudflared automatically.")
        print("        Download it manually from:")
        print("        https://github.com/cloudflare/cloudflared/releases/latest")
        print(f"        and save it as: {CLOUDFLARED_EXE}")
        return None

    print(f"[OK] cloudflared ready: {version}")
    return CLOUDFLARED_EXE


class CloudflareTunnel:
    """Runs ``cloudflared tunnel --url http://127.0.0.1:<port>``.

    The tunneling layer is intentionally isolated: supporting a named tunnel
    with a custom domain later means passing different arguments here, without
    touching the file server.
    """

    def __init__(self, exe: Path, port: int, log_path: Path, protocol: str = "auto") -> None:
        self.exe = exe
        self.port = port
        self.log_path = log_path
        self.protocol = protocol
        self.process: Optional[subprocess.Popen] = None
        self.public_url: Optional[str] = None
        self.registered = False       # data plane connected to the edge
        self.fatal_error: Optional[str] = None
        self._url_event = threading.Event()
        self._registered_event = threading.Event()
        self._fatal_event = threading.Event()
        self._reader_thread: Optional[threading.Thread] = None

    def _command(self) -> list:
        # Quick Tunnel.  ``--no-autoupdate`` keeps behaviour deterministic.
        cmd = [
            str(self.exe),
            "tunnel",
            "--url",
            f"http://{HOST}:{self.port}",
            "--no-autoupdate",
        ]
        if self.protocol and self.protocol != "auto":
            cmd += ["--protocol", self.protocol]
        return cmd

    def start(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Starting tunnel: %s", " ".join(self._command()))
        self.process = subprocess.Popen(
            self._command(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self._reader_thread = threading.Thread(
            target=self._read_output, name="cloudflared-reader", daemon=True
        )
        self._reader_thread.start()

    def _read_output(self) -> None:
        """Tee cloudflared output to a log file and watch for the public URL."""
        assert self.process is not None and self.process.stdout is not None
        with open(self.log_path, "a", encoding="utf-8") as log_file:
            for raw_line in self.process.stdout:
                line = ANSI_RE.sub("", raw_line).rstrip()
                if not line:
                    continue
                log_file.write(line + "\n")
                log_file.flush()
                logger.debug("cloudflared: %s", line)
                if self.public_url is None:
                    match = TUNNEL_URL_RE.search(line)
                    if match:
                        self.public_url = match.group(0)
                        self._url_event.set()
                if not self.registered and "Registered tunnel connection" in line:
                    # Data plane is up: the public URL is actually reachable now.
                    logger.info("Cloudflare tunnel connection registered.")
                    self.registered = True
                    self._registered_event.set()
                if self.fatal_error is None and TUNNEL_FATAL_RE.search(line):
                    # The quick tunnel itself could not be created (e.g. DNS or
                    # API blocked).  Abort the wait early instead of timeouting.
                    self.fatal_error = line.strip()
                    self._fatal_event.set()

    def wait_for_url(self, timeout: float) -> Optional[str]:
        """Block until the public URL is found (or fail fast on a fatal error)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._url_event.is_set():
                return self.public_url
            if self._fatal_event.is_set() or (
                self.process is not None and self.process.poll() is not None
            ):
                return None
            time.sleep(0.25)
        return None

    def wait_for_registration(self, timeout: float) -> bool:
        """Block until the tunnel data plane connects to the Cloudflare edge."""
        return self._registered_event.wait(timeout)

    def stop(self) -> None:
        """Terminate the tunnel process, escalating to kill if needed."""
        if self.process is None:
            return
        if self.process.poll() is None:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    self.process.kill()
                except OSError:
                    pass
        self.process = None
        logger.info("cloudflared stopped.")


class SshTunnel:
    """A public reverse tunnel borrowed over SSH.

    Used as an automatic fallback when Cloudflare is unreachable.  Works with
    nothing but an outbound SSH connection to a free provider.  The provider
    config (command builder + URL regex) is passed in from ``SSH_PROVIDERS``.
    """

    def __init__(self, port: int, log_path: Path, provider: dict) -> None:
        self.port = port
        self.log_path = log_path
        self.provider = provider
        self.process: Optional[subprocess.Popen] = None
        self.public_url: Optional[str] = None
        self.reachable = False          # URL served our local content (verified)
        self._log_file = None
        self._url_event = threading.Event()

    def _command(self) -> list:
        return self.provider["cmd"](self.port)

    def start(self) -> None:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Starting SSH tunnel: %s", " ".join(self._command()))
        # On Windows, ssh.exe produces no output through a pipe (subprocess.PIPE).
        # Redirect stdout to the log file directly instead — this matches how
        # the manual probe successfully captured the URL.
        self._log_file = open(
            self.log_path, "w", encoding="utf-8", errors="replace"
        )
        self.process = subprocess.Popen(
            self._command(),
            stdout=self._log_file,
            stderr=subprocess.STDOUT,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
        )
        self._reader_thread = threading.Thread(
            target=self._poll_for_url, name="ssh-tunnel-reader", daemon=True
        )
        self._reader_thread.start()

    def _poll_for_url(self) -> None:
        """Watch the log file for the public URL (ssh writes to a file on Windows)."""
        deadline = time.monotonic() + SSH_STARTUP_TIMEOUT
        url_re = self.provider["url_re"]
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                break
            try:
                text = self.log_path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                time.sleep(0.5)
                continue
            match = url_re.search(text)
            if match:
                self.public_url = match.group(0)
                self._url_event.set()
                return
            time.sleep(0.5)
        # Final read — the URL may have appeared right before the timeout.
        try:
            text = self.log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return
        match = url_re.search(text)
        if match:
            self.public_url = match.group(0)
            self._url_event.set()

    def wait_for_url(self, timeout: float) -> Optional[str]:
        """Block until the public URL is found (or the process exits)."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self._url_event.is_set():
                return self.public_url
            if self.process is not None and self.process.poll() is not None:
                return None
            time.sleep(0.25)
        return None

    def stop(self) -> None:
        """Terminate the ssh process, escalating to kill if needed."""
        if self.process is None:
            return
        if self.process.poll() is None:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                try:
                    self.process.kill()
                except OSError:
                    pass
        if self._log_file and not self._log_file.closed:
            self._log_file.close()
        self.process = None
        logger.info("SSH tunnel stopped.")


def wait_reachable(url: str, tries: int = SSH_REACH_TRIES,
                   delay: float = SSH_REACH_DELAY, timeout: float = 12.0) -> bool:
    """Best-effort check that the public URL actually reaches the local server.

    Reverse tunnels over SSH can take a few seconds before the first request
    is forwarded; ``delay`` lets the tunnel settle between attempts.  Quick
    Tunnel DNS can also lag, so Cloudflare URLs may need a few retries too.
    """
    for _ in range(max(1, tries)):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "Mozilla/5.0 (BlackServerHealthCheck)"},
            )
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
                if resp.status == HTTPStatus.OK:
                    return True
        except Exception:  # noqa: BLE001 - any failure just means "try again"
            pass
        time.sleep(delay)
    return False


def try_ssh_fallback(port: int, timeout: float = SSH_STARTUP_TIMEOUT,
                     reason: str = "") -> Tuple[Optional[SshTunnel], Optional[str]]:
    """Try each SSH provider in order; return the first working ``(tunnel, url)``."""
    if reason:
        print(f"[i] {reason}")

    for provider in SSH_PROVIDERS:
        name = provider["name"]
        print(f"[..] Trying SSH tunnel provider: {name} ...")
        ssh = SshTunnel(port, LOGS_DIR / provider["log_name"], provider)
        try:
            ssh.start()
        except OSError as exc:
            print(f"    [!] Could not start ssh ({name}): {exc}")
            continue

        public_url = ssh.wait_for_url(timeout)
        if public_url:
            print(f"    [OK] {name} gave URL: {public_url}")
            print("    [..] Verifying the public URL reaches this server ...")
            if wait_reachable(public_url, tries=SSH_REACH_TRIES, delay=SSH_REACH_DELAY):
                print("    [OK] Public URL is live.")
                return ssh, public_url
            print(f"    [!] {name} URL did not respond; trying next provider ...")
        else:
            print(f"    [!] {name} did not give a URL in {int(timeout)}s.")
        ssh.stop()

    print("[ERROR] All SSH tunnel providers failed.")
    return None, None


# ---------------------------------------------------------------------------
# File icons (based on extension)
# ---------------------------------------------------------------------------

_FILE_ICONS: dict[str, str] = {
    ".zip": "\U0001F4E6", ".rar": "\U0001F4E6", ".7z": "\U0001F4E6",
    ".tar": "\U0001F4E6", ".gz": "\U0001F4E6",
    ".pdf": "\U0001F4C4",
    ".doc": "\U0001F4C3", ".docx": "\U0001F4C3",
    ".xls": "\U0001F4CA", ".xlsx": "\U0001F4CA",
    ".ppt": "\U0001F4AC", ".pptx": "\U0001F4AC",
    ".txt": "\U0001F4DD", ".md": "\U0001F4DD",
    ".jpg": "\U0001F5BC\uFE0F", ".jpeg": "\U0001F5BC\uFE0F",
    ".png": "\U0001F5BC\uFE0F", ".gif": "\U0001F5BC\uFE0F",
    ".webp": "\U0001F5BC\uFE0F", ".bmp": "\U0001F5BC\uFE0F",
    ".svg": "\U0001F5BC\uFE0F", ".ico": "\U0001F5BC\uFE0F",
    ".mp3": "\U0001F3B5", ".wav": "\U0001F3B5", ".flac": "\U0001F3B5",
    ".aac": "\U0001F3B5", ".ogg": "\U0001F3B5", ".m4a": "\U0001F3B5",
    ".mp4": "\U0001F3AC", ".mkv": "\U0001F3AC", ".avi": "\U0001F3AC",
    ".mov": "\U0001F3AC", ".wmv": "\U0001F3AC", ".webm": "\U0001F3AC",
    ".exe": "\u2699\uFE0F", ".msi": "\u2699\uFE0F",
    ".py": "\U0001F40D", ".js": "\U0001F4DC", ".ts": "\U0001F4DC",
    ".html": "\U0001F310", ".css": "\U0001F310",
    ".json": "\U0001F4CB", ".xml": "\U0001F4CB", ".csv": "\U0001F4CB",
    ".iso": "\U0001F4BF", ".img": "\U0001F4BF",
    ".apk": "\U0001F4F1",
    ".ttf": "\U0001F5A8\uFE0F", ".otf": "\U0001F5A8\uFE0F",
    ".log": "\U0001F4DD", ".ini": "\u2699\uFE0F",
    ".bak": "\U0001F4BE",
}


def _file_icon(ext: str) -> str:
    """Return an emoji icon for a file extension."""
    return _FILE_ICONS.get(ext, "\U0001F4C4")


_FILE_BADGES: dict[str, tuple[str, str]] = {
    # ext: (css class, inner label)
    ".html": ("html", "&lt;/&gt;"),
    ".htm": ("html", "&lt;/&gt;"),
    ".css": ("css", "#"),
    ".js": ("js", "JS"),
    ".ts": ("ts", "TS"),
    ".json": ("json", "{}"),
    ".md": ("md", "MD"),
    ".txt": ("txt", "TXT"),
    ".png": ("img", "IMG"),
    ".jpg": ("img", "IMG"),
    ".jpeg": ("img", "IMG"),
    ".gif": ("img", "IMG"),
    ".webp": ("img", "IMG"),
    ".svg": ("img", "SVG"),
    ".ico": ("ico", "&#9733;"),
    ".pdf": ("pdf", "PDF"),
    ".zip": ("zip", "ZIP"),
    ".rar": ("zip", "ZIP"),
    ".7z": ("zip", "ZIP"),
    ".mp4": ("vid", "MP4"),
    ".mkv": ("vid", "MKV"),
    ".mp3": ("aud", "MP3"),
    ".py": ("py", "PY"),
    ".exe": ("exe", "EXE"),
}


def _file_badge(ext: str) -> str:
    """Colored rounded badge matching the file-manager UI."""
    cls, label = _FILE_BADGES.get(ext, ("file", "FILE"))
    return f'<span class="badge {cls}">{label}</span>'


def _fmt_mtime(ts: float) -> str:
    """Human-friendly modified time like ``Today, 05:30 PM`` / ``May 1, 2023``."""
    dt = time.localtime(ts)
    now = time.localtime()
    if dt.tm_year == now.tm_year and dt.tm_yday == now.tm_yday:
        return "Today, " + time.strftime("%I:%M %p", dt).lstrip("0")
    if dt.tm_year == now.tm_year and dt.tm_yday == now.tm_yday - 1:
        return "Yesterday, " + time.strftime("%I:%M %p", dt).lstrip("0")
    return time.strftime("%b %d, %Y", dt)


def _perm_string(path: Path) -> str:
    """Unix-style permission string, e.g. ``drwxr-xr-x`` / ``rw-r--r--``."""
    try:
        st = path.stat()
    except OSError:
        return "rwxr-xr-x" if path.is_dir() else "rw-r--r--"
    prefix = "d" if path.is_dir() else "-"
    perms = ""
    for shift in (6, 3, 0):
        triple = (st.st_mode >> shift) & 0o7
        perms += "r" if triple & 4 else "-"
        perms += "w" if triple & 2 else "-"
        perms += "x" if triple & 1 else "-"
    return prefix + perms


def _sidebar_tree(current: Path) -> str:
    """Build the nested folder tree for the left sidebar (cached, no repeated resolve)."""
    store = storage()
    root = DOWNLOADS_DIR.resolve()
    try:
        cur = str(current.resolve())
    except OSError:
        cur = str(current)
    now = time.time()
    if _SIDEBAR_CACHE["html"] is not None and now - _SIDEBAR_CACHE["ts"] < _SIDEBAR_TTL and _SIDEBAR_CACHE["for"] == cur:
        return _SIDEBAR_CACHE["html"]

    if store is not None:
        # Neon mode: the folder list comes from Postgres, never from disk.
        cur_rel = "/" + current.relative_to(root).as_posix().lstrip("./")
        folders = sidebar_folders(store)
        nodes: dict[str, list] = {}

        def children_of(rel: str) -> list[tuple[str, str]]:
            prefix = rel.rstrip("/") + "/"
            return sorted(
                (
                    (name, path)
                    for name, path in folders
                    if path.startswith(prefix) and "/" not in path[len(prefix):]
                ),
                key=lambda item: item[0].lower(),
            )

        def render(rel: str) -> str:
            parts = []
            for name, path in children_of(rel):
                href = path.rstrip("/") + "/"
                is_self = cur_rel == href
                under = cur_rel.startswith(href)
                cls = " active" if is_self else ""
                open_cls = " open" if under else ""
                current_cls = " current" if is_self else ""
                kids = render(href)
                chev = (
                    '<button type="button" class="tchev" aria-label="Toggle" '
                    'onclick="toggleTNode(event, this)">&#9654;</button>'
                    if kids
                    else '<span class="tchev empty"></span>'
                )
                parts.append(
                    f'<div class="tnode{open_cls}{current_cls}">'
                    f'<div class="trow">{chev}'
                    f'<a class="tlink{cls}" href="{html.escape(href)}">'
                    f'<span class="tfolder"></span>'
                    f'<span class="tname">{html.escape(name)}</span></a></div>'
                    f"{kids}</div>"
                )
            return f'<div class="tkids">{"".join(parts)}</div>' if parts else ""

        children = render("/")
        root_current = " current" if cur_rel == "/" else ""
        root_chev = (
            '<button type="button" class="tchev" aria-label="Toggle" '
            'onclick="toggleTNode(event, this)">&#9654;</button>'
            if children
            else '<span class="tchev empty"></span>'
        )
        html_out = (
            f'<div class="tnode open{root_current}">'
            f'<div class="trow">{root_chev}'
            f'<a class="tlink" href="/">'
            '<span class="tfolder root"></span>'
            '<span class="tname">/</span></a></div>'
            f"{children}</div>"
        )
        _SIDEBAR_CACHE.update({"html": html_out, "ts": now, "for": cur})
        return html_out

    def walk(directory: Path, dir_abs: str) -> str:
        try:
            subdirs = sorted(
                (
                    p for p in directory.iterdir()
                    if p.is_dir() and not p.name.startswith(".")
                ),
                key=lambda p: p.name.lower(),
            )
        except OSError:
            return ""
        parts = []
        for sd in subdirs:
            href = "/" + sd.relative_to(root).as_posix() + "/"
            sd_abs = str(sd)
            is_self = sd_abs == cur
            under = cur.startswith(sd_abs + os.sep) or is_self
            cls = " active" if is_self else ""
            open_cls = " open" if under else ""
            current_cls = " current" if is_self else ""
            children = walk(sd, sd_abs)
            has_kids = bool(children)
            if has_kids:
                chev = (
                    '<button type="button" class="tchev" aria-label="Toggle" '
                    'onclick="toggleTNode(event, this)">&#9654;</button>'
                )
            else:
                chev = '<span class="tchev empty"></span>'
            parts.append(
                f'<div class="tnode{open_cls}{current_cls}">'
                f'<div class="trow">{chev}'
                f'<a class="tlink{cls}" href="{html.escape(href)}">'
                f'<span class="tfolder"></span>'
                f'<span class="tname">{html.escape(sd.name)}</span></a></div>'
                f"{children}</div>"
            )
        return f'<div class="tkids">{"".join(parts)}</div>' if parts else ""

    root_abs = str(root)
    root_active = " active" if cur == root_abs else ""
    root_current = " current" if cur == root_abs else ""
    children = walk(root, root_abs)
    root_chev = (
        '<button type="button" class="tchev" aria-label="Toggle" '
        'onclick="toggleTNode(event, this)">&#9654;</button>'
        if children
        else '<span class="tchev empty"></span>'
    )
    html_out = (
        f'<div class="tnode open{root_current}">'
        f'<div class="trow">{root_chev}'
        f'<a class="tlink{root_active}" href="/">'
        '<span class="tfolder root"></span>'
        '<span class="tname">/</span></a></div>'
        f"{children}</div>"
    )
    _SIDEBAR_CACHE.update({"html": html_out, "ts": now, "for": cur})
    return html_out


# Dedicated video player page. Kept as one template so the markup, the styles and
# the script stay side by side; __NAME__/__SIZE__/__SRC__/__JSON_NAME__ are
# filled in per request.
PLAYER_HTML = """<!DOCTYPE html>
<html lang="fa" dir="ltr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, viewport-fit=cover">
<meta name="color-scheme" content="dark">
<title>__NAME__ - Black Server</title>
<link rel="icon" href="/favicon.ico" sizes="any">
<style>
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html,body{height:100%;width:100%;background:#000;overflow:hidden}
body{font-family:'Segoe UI',system-ui,-apple-system,'Noto Sans Arabic',sans-serif;color:#e5e7eb;
  user-select:none;-webkit-user-select:none}
button{font-family:inherit;color:inherit;background:none;border:none;cursor:pointer}

/* ---------- stage ---------- */
.stage{position:fixed;inset:0;background:#000;display:flex;align-items:center;
  justify-content:center;overflow:hidden}
video{max-width:100%;max-height:100%;width:auto;height:auto;object-fit:contain;
  background:#000;outline:none;display:block}
video.noc{pointer-events:none}

/* soft ambient glow behind the video */
.glow{position:fixed;inset:0;pointer-events:none;opacity:.5;
  background:radial-gradient(60% 60% at 50% 45%,rgba(59,130,246,.18),transparent 70%)}

/* ---------- shared chrome ---------- */
.chrome{position:fixed;left:0;right:0;z-index:20;transition:opacity .28s ease,
  transform .28s ease, visibility .28s}
.chrome.hide{opacity:0;visibility:hidden;pointer-events:none}
.top{top:0;display:flex;align-items:center;gap:10px;padding:12px 14px;
  padding-top:max(12px,env(safe-area-inset-top));
  background:linear-gradient(180deg,rgba(0,0,0,.82),rgba(0,0,0,0))}
.top.hide{transform:translateY(-100%)}
.back{display:inline-flex;align-items:center;gap:7px;padding:8px 13px;border-radius:10px;
  border:1px solid rgba(255,255,255,.16);background:rgba(255,255,255,.07);
  font-size:13px;font-weight:600;text-decoration:none;color:#f3f4f6;
  backdrop-filter:blur(10px);-webkit-backdrop-filter:blur(10px);white-space:nowrap}
.back:hover{background:rgba(255,255,255,.15)}
.ttitle{flex:1;min-width:0;font-size:13.5px;font-weight:700;overflow:hidden;
  text-overflow:ellipsis;white-space:nowrap;text-shadow:0 1px 4px rgba(0,0,0,.6)}
.tsize{font-size:12px;color:#9ca3af;white-space:nowrap;font-variant-numeric:tabular-nums}
.dl{display:inline-flex;align-items:center;gap:7px;padding:8px 14px;border-radius:10px;
  border:none;background:linear-gradient(135deg,#4f8cff,#3b5bfc);color:#fff;font-size:13px;
  font-weight:700;text-decoration:none;white-space:nowrap;
  box-shadow:0 4px 18px rgba(59,91,252,.4)}
.dl:hover{filter:brightness(1.12)}

/* ---------- bottom controls ---------- */
.bar{bottom:0;padding:26px 16px 14px;padding-bottom:max(14px,env(safe-area-inset-bottom));
  background:linear-gradient(0deg,rgba(0,0,0,.88),rgba(0,0,0,.55) 55%,rgba(0,0,0,0))}
.bar.hide{transform:translateY(100%)}

/* seek */
.seek{position:relative;height:20px;display:flex;align-items:center;cursor:pointer;
  touch-action:none}
.track{position:relative;width:100%;height:5px;border-radius:999px;
  background:rgba(255,255,255,.22);transition:height .15s ease}
.seek:hover .track,.seek.drag .track{height:8px}
.buf,.played{position:absolute;left:0;top:0;bottom:0;border-radius:999px;width:0}
.buf{background:rgba(255,255,255,.34)}
.played{background:linear-gradient(90deg,#60a5fa,#3b82f6)}
.knob{position:absolute;top:50%;width:13px;height:13px;border-radius:50%;background:#fff;
  transform:translate(-50%,-50%) scale(0);transition:transform .15s ease;
  box-shadow:0 2px 8px rgba(0,0,0,.6);pointer-events:none}
.seek:hover .knob,.seek.drag .knob{transform:translate(-50%,-50%) scale(1)}
.tip{position:absolute;bottom:26px;transform:translateX(-50%);padding:4px 8px;border-radius:7px;
  background:rgba(12,16,26,.94);border:1px solid rgba(255,255,255,.14);font-size:11.5px;
  font-weight:600;white-space:nowrap;opacity:0;pointer-events:none;
  font-variant-numeric:tabular-nums;transition:opacity .12s}
.seek:hover .tip,.seek.drag .tip{opacity:1}

/* button row */
.row{display:flex;align-items:center;gap:6px;margin-top:6px}
.ib{display:inline-flex;align-items:center;justify-content:center;width:40px;height:40px;
  border-radius:11px;color:#e5e7eb;flex-shrink:0;transition:background .15s,transform .12s}
.ib:hover{background:rgba(255,255,255,.14)}
.ib:active{transform:scale(.93)}
.ib svg{width:21px;height:21px;fill:currentColor;pointer-events:none}
.ib.big{width:46px;height:46px}
.ib.big svg{width:26px;height:26px}
.ib.on{color:#60a5fa}
.spacer{flex:1}
.time{font-size:12.5px;color:#d1d5db;font-variant-numeric:tabular-nums;
  white-space:nowrap;padding:0 4px;direction:ltr}
.time b{color:#fff;font-weight:700}

/* volume */
.vol{display:flex;align-items:center;gap:2px}
.vslider{width:0;overflow:hidden;transition:width .2s ease,opacity .2s ease;opacity:0}
.vol:hover .vslider,.vslider.open{width:88px;opacity:1}
input[type=range]{-webkit-appearance:none;appearance:none;height:4px;border-radius:999px;
  background:rgba(255,255,255,.28);outline:none;cursor:pointer}
input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:13px;height:13px;
  border-radius:50%;background:#fff;box-shadow:0 1px 5px rgba(0,0,0,.5)}
input[type=range]::-moz-range-thumb{width:13px;height:13px;border:none;border-radius:50%;
  background:#fff}
#vol{width:78px;margin:0 6px}

/* speed menu */
.menu{position:relative}
.pop{position:absolute;bottom:calc(100% + 10px);right:0;min-width:104px;padding:6px;
  border-radius:13px;background:rgba(14,18,28,.96);border:1px solid rgba(255,255,255,.14);
  box-shadow:0 18px 46px rgba(0,0,0,.6);backdrop-filter:blur(18px);
  -webkit-backdrop-filter:blur(18px);display:none;z-index:30}
.pop.open{display:block;animation:pop .14s cubic-bezier(.16,1,.3,1)}
@keyframes pop{from{opacity:0;transform:translateY(6px) scale(.97)}to{opacity:1;transform:none}}
.pop button{display:flex;align-items:center;justify-content:space-between;gap:12px;width:100%;
  padding:8px 10px;border-radius:9px;font-size:12.5px;font-weight:600;color:#d1d5db}
.pop button:hover{background:rgba(255,255,255,.12);color:#fff}
.pop button.on{color:#60a5fa}
.pop .tick{font-weight:800}

/* ---------- overlays ---------- */
.spin{position:fixed;inset:0;display:none;align-items:center;justify-content:center;
  pointer-events:none;z-index:15}
.spin.on{display:flex}
.spin i{width:46px;height:46px;border-radius:50%;border:3px solid rgba(255,255,255,.18);
  border-top-color:#60a5fa;animation:spin .8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}
.bigplay{position:fixed;inset:0;display:none;align-items:center;justify-content:center;
  z-index:14;background:rgba(0,0,0,.28);cursor:pointer}
.bigplay.on{display:flex}
.bigplay span{width:88px;height:88px;border-radius:50%;background:rgba(59,130,246,.94);
  display:flex;align-items:center;justify-content:center;
  box-shadow:0 12px 46px rgba(0,0,0,.6);transition:transform .18s ease}
.bigplay:hover span{transform:scale(1.07)}
.bigplay svg{width:38px;height:38px;fill:#fff;margin-inline-start:5px}
.err{position:fixed;inset:0;display:none;flex-direction:column;align-items:center;
  justify-content:center;gap:16px;background:#000;z-index:40;text-align:center;padding:24px;
  font-size:14.5px;line-height:1.7}
.err.on{display:flex}
.err .ico{font-size:46px}
@media (max-width:700px){
  .tsize{display:none}
  .ib{width:36px;height:36px}
  .ib.big{width:42px;height:42px}
  .row{gap:2px}
  #vol{width:60px}
  .vol:hover .vslider,.vslider.open{width:68px}
}
</style>
</head>
<body>

<div class="glow"></div>
<div class="stage" id="stage">
  <video id="vv" src="__SRC__" preload="metadata" playsinline webkit-playsinline></video>
</div>

<div class="chrome top" id="top">
  <a class="back" href="#" onclick="closeViewerTab(event)">
    <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor"
      stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round">
      <path d="M15 18l-6-6 6-6"/></svg> بازگشت</a>
  <div class="ttitle" title="__NAME__">__NAME__</div>
  <div class="tsize">__SIZE__</div>
  <a class="dl" id="dlBtn" download>&#8681; دانلود</a>
</div>

<div class="chrome bar" id="bar">
  <div class="seek" id="seek">
    <div class="track" id="track">
      <div class="buf" id="buf"></div>
      <div class="played" id="played"></div>
      <div class="knob" id="knob"></div>
    </div>
    <div class="tip" id="tip">0:00</div>
  </div>
  <div class="row">
    <button class="ib big" id="playBtn" title="پخش / مکث (Space)" aria-label="Play"></button>
    <button class="ib" id="back10" title="۱۰ ثانیه عقب (J)" aria-label="Back 10 seconds">
      <svg viewBox="0 0 24 24"><path d="M12 5V1L7 6l5 5V7a6 6 0 1 1-6 6H4a8 8 0 1 0 8-8z"/></svg>
    </button>
    <button class="ib" id="fwd10" title="۱۰ ثانیه جلو (L)" aria-label="Forward 10 seconds">
      <svg viewBox="0 0 24 24"><path d="M12 5V1l5 5-5 5V7a6 6 0 1 0 6 6h2a8 8 0 1 1-8-8z"/></svg>
    </button>
    <div class="vol">
      <button class="ib" id="muteBtn" title="بی‌صدا (M)" aria-label="Mute"></button>
      <div class="vslider" id="vslider">
        <input type="range" id="vol" min="0" max="100" value="100" aria-label="Volume">
      </div>
    </div>
    <div class="spacer"></div>
    <div class="time"><b id="cur">0:00</b> / <span id="dur">0:00</span></div>
    <div class="menu">
      <button class="ib" id="rateBtn" title="سرعت پخش" aria-label="Playback speed">
        <svg viewBox="0 0 24 24"><path d="M12 4a8 8 0 1 0 8 8h-2a6 6 0 1 1-6-6V4z"/><path d="M12 2l4 3-4 3V2z"/></svg>
      </button>
      <div class="pop" id="ratePop">
        <button data-r="0.25">0.25&times;</button>
        <button data-r="0.5">0.5&times;</button>
        <button data-r="0.75">0.75&times;</button>
        <button data-r="1" class="on">1&times;<span class="tick">&#10003;</span></button>
        <button data-r="1.25">1.25&times;</button>
        <button data-r="1.5">1.5&times;</button>
        <button data-r="1.75">1.75&times;</button>
        <button data-r="2">2&times;</button>
      </div>
    </div>
    <button class="ib" id="pipBtn" title="تصویر در تصویر (P)" aria-label="Picture in picture">
      <svg viewBox="0 0 24 24"><path d="M21 3H3a1 1 0 0 0-1 1v16a1 1 0 0 0 1 1h18a1 1 0 0 0 1-1V4a1 1 0 0 0-1-1zm0 15H3V6h18v12z"/><path d="M11 14h7v5h-7z"/></svg>
    </button>
    <button class="ib" id="fsBtn" title="تمام‌صفحه (F)" aria-label="Fullscreen">
      <svg viewBox="0 0 24 24" id="fsIcon"><path d="M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z"/></svg>
    </button>
  </div>
</div>

<div class="spin" id="spin"><i></i></div>
<div class="bigplay" id="bigplay"><span><svg viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg></span></div>
<div class="err" id="err">
  <div class="ico">&#9888;</div>
  <div>این فرمت ویدیو در مرورگر پشتیبانی نمی&#39;شود.<br>می&#39;توانید فایل را دانلود کنید.</div>
  <a class="dl" id="errDl" download>&#8681; دانلود فایل</a>
</div>

<script>
(function(){
  "use strict";
  var NAME = __JSON_NAME__;
  var v = document.getElementById("vv");
  var $ = function(id){ return document.getElementById(id); };
  var stage = $("stage"), seek = $("seek"), track = $("track");
  var played = $("played"), buf = $("buf"), knob = $("knob"), tip = $("tip");
  var topbar = $("top"), bar = $("bar"), spin = $("spin"), bigplay = $("bigplay");
  var errBox = $("err"), pop = $("ratePop"), volIn = $("vol");

  var ICON_PLAY = '<svg viewBox="0 0 24 24"><path d="M8 5v14l11-7z"/></svg>';
  var ICON_PAUSE = '<svg viewBox="0 0 24 24"><path d="M6 4h4v16H6zm8 0h4v16h-4z"/></svg>';
  var ICON_VOL = '<svg viewBox="0 0 24 24"><path d="M3 9v6h4l5 5V4L7 9H3z"/>'
    + '<path d="M16.5 12a4.5 4.5 0 0 0-2.5-4v8a4.5 4.5 0 0 0 2.5-4z" opacity=".9"/>'
    + '<path d="M14 3.2v2.1a6.8 6.8 0 0 1 0 13.4v2.1a8.9 8.9 0 0 0 0-17.6z" opacity=".55"/></svg>';
  var ICON_MUTE = '<svg viewBox="0 0 24 24"><path d="M3 9v6h4l5 5V4L7 9H3z"/>'
    + '<path d="M16.5 9.1l1.4 1.4 1.4-1.4 1.1 1.1-1.4 1.4 1.4 1.4-1.1 1.1-1.4-1.4'
    + '1.4 1.4-1.1 1.1-1.4-1.4-1.4 1.4-1.1-1.1 1.4-1.4-1.4-1.4z"/></svg>';

  var hideTimer = null;

  /* ---------- helpers ---------- */
  function fmt(t){
    if(!isFinite(t) || t < 0) t = 0;
    var s = Math.floor(t % 60), m = Math.floor(t / 60) % 60, h = Math.floor(t / 3600);
    var mm = (h ? String(m).padStart(2,"0") : String(m));
    return (h ? h + ":" : "") + mm + ":" + String(s).padStart(2,"0");
  }
  function store(k, val){
    try { if(val === undefined) return localStorage.getItem(k);
         localStorage.setItem(k, val); } catch(e){ return null; }
  }
  function clamp(v2, a, b){ return v2 < a ? a : (v2 > b ? b : v2); }

  /* ---------- chrome auto-hide ---------- */
  function wake(){
    topbar.classList.remove("hide");
    bar.classList.remove("hide");
    clearTimeout(hideTimer);
    hideTimer = setTimeout(function(){
      if(!v.paused && !pop.classList.contains("open") && !document.fullscreenElement){
        topbar.classList.add("hide");
        bar.classList.add("hide");
      }
    }, 2600);
  }

  /* ---------- play / pause ---------- */
  function setIcon(){
    $("playBtn").innerHTML = v.paused ? ICON_PLAY : ICON_PAUSE;
    bigplay.classList.toggle("on", v.paused && !errBox.classList.contains("on"));
  }
  function play(){
    var p = v.play();
    if(p && p.catch) p.catch(function(){});
  }
  function toggle(){
    if(v.paused) play(); else v.pause();
  }
  function seekBy(d){ v.currentTime = clamp(v.currentTime + d, 0, v.duration || 0); }
  function seekTo(r){ if(isFinite(v.duration)) v.currentTime = clamp(r, 0, 1) * v.duration; }

  /* ---------- progress ---------- */
  function paint(){
    var d = v.duration || 0, c = v.currentTime || 0;
    var p = d > 0 ? clamp(c / d, 0, 1) : 0;
    played.style.width = (p * 100) + "%";
    knob.style.left = (p * 100) + "%";
    $("cur").textContent = fmt(c);
    if(isFinite(d)) $("dur").textContent = fmt(d);
    var b = v.buffered, end = 0;
    for(var i = 0; i < b.length; i++){ if(b.start(i) <= c + 0.1) end = b.end(i); }
    buf.style.width = (d > 0 ? clamp(end / d, 0, 1) * 100 : 0) + "%";
  }

  /* ---------- seek bar interaction ---------- */
  function ratioAt(clientX){
    var r = track.getBoundingClientRect();
    return clamp((clientX - r.left) / Math.max(1, r.width), 0, 1);
  }
  function hover(clientX){
    var r = ratioAt(clientX), d = v.duration || 0;
    tip.style.left = (r * 100) + "%";
    tip.textContent = d > 0 ? fmt(r * d) : "0:00";
  }
  var dragging = false;
  function down(ev){
    if(errBox.classList.contains("on")) return;
    dragging = true;
    seek.classList.add("drag");
    if(ev.target.setPointerCapture) { try { ev.target.setPointerCapture(ev.pointerId); } catch(e){} }
    hover(ev.clientX);
    ev.preventDefault();
    wake();
  }
  function move(ev){
    if(!dragging) return;
    hover(ev.clientX);
    seekTo(ratioAt(ev.clientX));
    ev.preventDefault();
  }
  function up(ev){
    if(!dragging) return;
    dragging = false;
    seek.classList.remove("drag");
    if(ev) { try { seekTo(ratioAt(ev.clientX)); } catch(e){} }
    wake();
  }
  seek.addEventListener("pointerdown", down);
  seek.addEventListener("pointermove", function(ev){
    if(dragging) move(ev); else { hover(ev.clientX); wake(); }
  });
  window.addEventListener("pointermove", function(ev){
    if(dragging) move(ev);
    wake();
  });
  window.addEventListener("pointerup", up);
  window.addEventListener("pointercancel", up);

  /* ---------- volume ---------- */
  function paintVol(){
    var m = v.muted || v.volume === 0;
    $("muteBtn").innerHTML = m ? ICON_MUTE : ICON_VOL;
    if(!m) volIn.value = Math.round(v.volume * 100);
    volIn.style.background = "linear-gradient(90deg,#60a5fa " +
      (m ? 0 : Math.round(v.volume * 100)) + "%,rgba(255,255,255,.28) " +
      (m ? 0 : Math.round(v.volume * 100)) + "%)";
  }
  function setVol(x){
    x = clamp(x, 0, 1);
    v.volume = x;
    v.muted = (x === 0);
    paintVol();
    store("bs-vol", String(x));
  }
  volIn.addEventListener("input", function(){ setVol(volIn.value / 100); });
  $("muteBtn").addEventListener("click", function(){
    if(v.muted || v.volume === 0){ var s = store("bs-vol"); setVol(s ? parseFloat(s) : 1); }
    else { store("bs-vol", String(v.volume)); v.muted = true; paintVol(); }
    wake();
  });

  /* ---------- speed ---------- */
  $("rateBtn").addEventListener("click", function(ev){
    ev.stopPropagation();
    pop.classList.toggle("open");
    wake();
  });
  pop.addEventListener("click", function(ev){
    var b = ev.target.closest("button[data-r]");
    if(!b) return;
    setRate(parseFloat(b.getAttribute("data-r")));
  });
  function setRate(r){
    v.playbackRate = r;
    store("bs-rate", String(r));
    [].forEach.call(pop.querySelectorAll("button"), function(b){
      b.classList.toggle("on", parseFloat(b.getAttribute("data-r")) === r);
    });
    pop.classList.remove("open");
  }

  /* ---------- fullscreen / pip ---------- */
  function fsIcon(){
    $("fsIcon").innerHTML = document.fullscreenElement || document.webkitFullscreenElement
      ? '<path d="M5 16h3v3h2v-5H5v2zm3-8H5v2h5V5H8v3zm6 11h2v-3h3v-2h-5v5zm2-11V5h-2v5h5V8h-3z"/>'
      : '<path d="M7 14H5v5h5v-2H7v-3zm-2-4h2V7h3V5H5v5zm12 7h-3v2h5v-5h-2v3zM14 5v2h3v3h2V5h-5z"/>';
  }
  function toggleFs(){
    var el = document.fullscreenElement || document.webkitFullscreenElement;
    if(el){ (document.exitFullscreen || document.webkitExitFullscreen).call(document); }
    else {
      var t = document.documentElement;
      var rq = t.requestFullscreen || t.webkitRequestFullscreen;
      if(rq) { var p = rq.call(t); if(p && p.catch) p.catch(function(){}); }
    }
  }
  $("fsBtn").addEventListener("click", function(){ toggleFs(); wake(); });
  document.addEventListener("fullscreenchange", function(){ fsIcon(); wake(); });
  document.addEventListener("webkitfullscreenchange", function(){ fsIcon(); wake(); });

  function togglePip(){
    if(document.pictureInPictureElement){ document.exitPictureInPicture(); return; }
    if(!document.pictureInPictureEnabled || !v.requestPictureInPicture){
      toast("تصویر در تصویر پشتیبانی نمی&#39;شود");
      return;
    }
    var p = v.requestPictureInPicture();
    if(p && p.catch) p.catch(function(){});
  }
  $("pipBtn").addEventListener("click", function(){ togglePip(); wake(); });
  if(!document.pictureInPictureEnabled) $("pipBtn").style.display = "none";

  function toast(msg){
    var d = document.createElement("div");
    d.textContent = msg;
    d.style.cssText = "position:fixed;bottom:120px;left:50%;transform:translateX(-50%);"
      + "padding:10px 18px;border-radius:12px;background:rgba(14,18,28,.95);"
      + "border:1px solid rgba(255,255,255,.14);font-size:13px;z-index:50;"
      + "box-shadow:0 14px 40px rgba(0,0,0,.5)";
    document.body.appendChild(d);
    setTimeout(function(){ d.remove(); }, 2200);
  }

  /* ---------- buttons ---------- */
  $("playBtn").addEventListener("click", function(){ toggle(); wake(); });
  $("back10").addEventListener("click", function(){ seekBy(-10); wake(); });
  $("fwd10").addEventListener("click", function(){ seekBy(10); wake(); });
  bigplay.addEventListener("click", function(){ toggle(); wake(); });
  stage.addEventListener("click", function(ev){
    if(ev.target === v) { toggle(); }
    wake();
  });
  v.addEventListener("dblclick", function(ev){
    if(ev.target === v) toggleFs();
  });

  document.addEventListener("click", function(ev){
    if(!ev.target.closest(".menu")) pop.classList.remove("open");
  });

  /* ---------- keyboard ---------- */
  document.addEventListener("keydown", function(ev){
    var tag = (ev.target.tagName || "").toLowerCase();
    if(tag === "input" || tag === "textarea") return;
    var k = ev.key;
    if(k === " " || k === "k" || k === "K"){ toggle(); }
    else if(k === "ArrowRight"){ seekBy(ev.shiftKey ? 5 : 10); }
    else if(k === "ArrowLeft"){ seekBy(ev.shiftKey ? -5 : -10); }
    else if(k === "j" || k === "J"){ seekBy(-10); }
    else if(k === "l" || k === "L"){ seekBy(10); }
    else if(k === "ArrowUp"){ setVol(v.volume + 0.05); }
    else if(k === "ArrowDown"){ setVol(v.volume - 0.05); }
    else if(k === "m" || k === "M"){ $("muteBtn").click(); }
    else if(k === "f" || k === "F"){ toggleFs(); }
    else if(k === "p" || k === "P"){ togglePip(); }
    else if(k === "Home"){ seekTo(0); }
    else if(k === "End"){ seekTo(0.999); }
    else if(k === ">"){ setRate(clamp(+(v.playbackRate + 0.25).toFixed(2), 0.25, 4)); }
    else if(k === "<"){ setRate(clamp(+(v.playbackRate - 0.25).toFixed(2), 0.25, 4)); }
    else if(k >= "0" && k <= "9"){ seekBy((+k / 10 - v.currentTime / (v.duration || 1)) * (v.duration || 0)); }
    else return;
    ev.preventDefault();
    wake();
  });

  /* ---------- media events ---------- */
  v.addEventListener("play", function(){ setIcon(); wake(); });
  v.addEventListener("pause", function(){ setIcon(); wake(); });
  v.addEventListener("ended", function(){ setIcon(); topbar.classList.remove("hide"); bar.classList.remove("hide"); });
  v.addEventListener("timeupdate", paint);
  v.addEventListener("durationchange", paint);
  v.addEventListener("progress", paint);
  v.addEventListener("waiting", function(){ spin.classList.add("on"); });
  v.addEventListener("stalled", function(){ spin.classList.add("on"); });
  v.addEventListener("seeking", function(){ spin.classList.add("on"); });
  v.addEventListener("canplay", function(){ spin.classList.remove("on"); paint(); });
  v.addEventListener("playing", function(){ spin.classList.remove("on"); });
  v.addEventListener("volumechange", paintVol);
  v.addEventListener("ratechange", function(){
    [].forEach.call(pop.querySelectorAll("button"), function(b){
      b.classList.toggle("on", parseFloat(b.getAttribute("data-r")) === v.playbackRate);
    });
  });
  v.addEventListener("error", function(){
    spin.classList.remove("on");
    errBox.classList.add("on");
    topbar.classList.remove("hide");
    bar.classList.add("hide");
  });

  /* ---------- close / download ---------- */
  var dl = $("dlBtn"), er = $("errDl");
  dl.href = location.pathname;
  dl.setAttribute("download", NAME);
  er.href = location.pathname;
  er.setAttribute("download", NAME);
  window.closeViewerTab = function(ev){
    if(ev) ev.preventDefault();
    try { window.close(); } catch(e){}
    setTimeout(function(){
      try { if(!window.closed) history.back(); }
      catch(e2){ location.href = location.pathname.replace(/[^/]*$/,"") || "/"; }
    }, 80);
  };

  /* ---------- boot ---------- */
  var sv = parseFloat(store("bs-vol"));
  v.volume = isFinite(sv) ? clamp(sv, 0, 1) : 1;
  var sr = parseFloat(store("bs-rate"));
  if(isFinite(sr) && sr >= 0.25 && sr <= 4) v.playbackRate = sr;
  v.muted = false;
  paintVol();
  [].forEach.call(pop.querySelectorAll("button"), function(b){
    b.classList.toggle("on", parseFloat(b.getAttribute("data-r")) === v.playbackRate);
  });
  fsIcon();
  setIcon();
  paint();
  wake();
  play();
})();
</script>
</body>
</html>
"""
# ---------------------------------------------------------------------------
# File server
# ---------------------------------------------------------------------------

class DownloadRequestHandler(SimpleHTTPRequestHandler):
    """Serves files from ``./downloads`` with listing, streaming and ranges.

    Security properties:
      * the root directory is fixed to ``./downloads``;
      * path traversal / symlink escapes are rejected;
      * dot-files (e.g. ``.env``, ``.gitignore``) are never served;
      * GET/HEAD serve files; POST supports upload / mkdir / create only
        (no admin API, names are validated against traversal).
    """

    server_version = "BlackServer/1.0"
    protocol_version = "HTTP/1.1"
    # Buffered writes are much faster than one syscall per header/body slice.
    wbufsize = 65536

    # -- logging ----------------------------------------------------------
    def _client_ip(self) -> str:
        """Real client IP, taken from the tunnel's forwarding header."""
        forwarded = self.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return self.client_address[0]

    def log_message(self, fmt: str, *args) -> None:  # noqa: A003
        logger.info("%s - %s", self._client_ip(), fmt % args)

    # -- path resolution --------------------------------------------------
    def _request_rel(self) -> str:
        """URL path as a storage-relative key (``""`` for the root).

        Percent-decoded and traversal-free, so it is safe to use as an object
        key: ``..`` segments are dropped rather than escaping the bucket.
        """
        raw = urllib.parse.urlparse(self.path).path
        if neon is not None:
            return neon.key_from_url(raw)
        return "/".join(
            p for p in urllib.parse.unquote(raw).replace("\\", "/").split("/")
            if p not in ("", ".", "..")
        )

    def _resolve_path(self) -> Optional[Path]:
        """Map the request path to a real path inside ``downloads``."""
        root = DOWNLOADS_DIR.resolve()
        try:
            candidate = Path(self.translate_path(self.path)).resolve()
        except OSError:
            return None
        # Defence in depth: translate_path already cannot escape, but resolving
        # symlinks makes the containment check explicit and robust.
        if candidate != root and root not in candidate.parents:
            return None
        return candidate

    @staticmethod
    def _is_hidden(path: Path) -> bool:
        root = DOWNLOADS_DIR.resolve()
        try:
            relative = path.relative_to(root)
        except ValueError:
            return True
        return any(part.startswith(".") for part in relative.parts)

    @staticmethod
    def _safe_name(raw: str, *, allow_basename: bool = False) -> Optional[str]:
        """Validate a user-supplied file/folder name (no traversal).

        With ``allow_basename`` (uploads) a client path is reduced to its
        final component; otherwise any path separator is rejected.
        """
        name = (raw or "").strip()
        if not allow_basename:
            name = name.replace("\\", "/")
            if "/" in name or name in (".", "..") or ".." in name:
                return None
        else:
            name = name.replace("\\", "/").split("/")[-1].strip()
            if not name or name in (".", ".."):
                return None
        if not name or name in (".", ".."):
            return None
        if any(ch in name for ch in '<>:"|?*\x00'):
            return None
        if len(name) > 200:
            return None
        return name

    def _json_response(self, code: int, obj: dict) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _read_body(self) -> bytes:
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0 or length > 200 * 1024 * 1024:  # 200 MB cap
            return b""
        return self.rfile.read(length)

    def _read_body_stream(self, on_chunk, chunk_size: int = CHUNK_SIZE) -> int:
        """Read request body in chunks, calling ``on_chunk(bytes)``. Returns total bytes."""
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            return 0
        remaining = length
        total = 0
        while remaining > 0:
            chunk = self.rfile.read(min(chunk_size, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            total += len(chunk)
            on_chunk(chunk)
        return total

    def _target_dir(self) -> Optional[Path]:
        """Directory that POST operations act on (current listing path)."""
        path = self._resolve_path()
        if path is None:
            return None
        store = storage()
        if store is not None:
            # Neon mode: the folder tree lives in Postgres, not on disk, so ask
            # the metadata store instead of stat()ing a path that is not there.
            rel = self._rel_of(path)
            if rel and not store.exists(rel):
                rel = rel.rsplit("/", 1)[0]
            if rel and not store.exists(rel):
                return None
            if self._is_hidden(path):
                return None
            return DOWNLOADS_DIR / rel if rel else DOWNLOADS_DIR
        if path.is_file():
            path = path.parent
        if not path.is_dir() or self._is_hidden(path):
            return None
        return path

    @staticmethod
    def _rel_of(path: Path) -> str:
        """Path under ``downloads`` as a storage key (``""`` for the root)."""
        try:
            return path.resolve().relative_to(DOWNLOADS_DIR.resolve()).as_posix()
        except (OSError, ValueError):
            return ""

    def _join_rel(self, directory: Path, name: str) -> str:
        """Storage key for *name* inside *directory*."""
        base = self._rel_of(directory)
        return f"{base}/{name}" if base else name

    # -- POST: upload / mkdir / create ------------------------------------
    def do_POST(self):  # noqa: N802
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        api = (query.get("__api") or [""])[0]
        if api == "upload":
            return self._handle_upload()
        if api == "upstart":
            return self._handle_upstart()
        if api == "uppart":
            return self._handle_uppart()
        if api == "upfinish":
            return self._handle_upfinish()
        if api == "updrop":
            return self._handle_updrop()
        if api == "mkdir":
            return self._handle_mkdir()
        if api == "create":
            return self._handle_create()
        if api == "delete":
            return self._handle_delete()
        if api == "rename":
            return self._handle_rename()
        if api == "move":
            return self._handle_move()
        if api == "copy":
            return self._handle_copy()
        if api == "tree":
            return self._handle_tree()
        if api == "jobstatus":
            return self._handle_jobstatus()
        if api == "search":
            return self._handle_search()
        if api == "save":
            return self._handle_save()
        self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "unknown route"})

    def _current_user(self) -> str:
        """Best-effort owner name for the metadata row (never trusted for auth)."""
        raw = (
            self.headers.get("X-User")
            or urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query).get("user", [""])[0]
        )
        name = re.sub(r"[^\w.@+-]", "", str(raw or "").strip())[:64]
        if name:
            return name
        store = storage()
        return (store.default_user if store is not None else "anonymous")

    def _handle_upload(self):
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        store = storage()
        target_rel = self._rel_of(target) if store is not None else ""
        user = self._current_user()
        ctype = self.headers.get("Content-Type", "")
        if "multipart/form-data" not in ctype:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "expected multipart"})
            return
        boundary_m = re.search(r'boundary="?([^";]+)"?', ctype)
        if not boundary_m:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "no boundary"})
            return
        boundary = b"--" + boundary_m.group(1).encode("utf-8", "replace")
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        if length <= 0:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "empty body"})
            return

        # Stream multipart: read chunks, split on boundary, write files as we go
        # (never hold the entire body in RAM — makes large uploads fast + flat memory).
        saved: list = []
        state = {
            "buf": b"",
            "remaining": length,
            "phase": "preamble",  # preamble | headers | body
            "cur_name": None,
            "cur_fp": None,
            "cur_path": None,
        }

        def _close_current():
            if state["cur_fp"] is not None:
                try:
                    state["cur_fp"].close()
                except OSError:
                    pass
                state["cur_fp"] = None
            state["cur_name"] = None
            state["cur_path"] = None

        def _open_part(header_blob: bytes) -> bool:
            if b"Content-Disposition" not in header_blob:
                return False
            fn_m = re.search(br'filename="([^"]*)"', header_blob)
            if not fn_m:
                fn_m = re.search(br"filename\*=UTF-8''([^\r\n;]+)", header_blob)
                if not fn_m:
                    return False
                try:
                    fname = urllib.parse.unquote(fn_m.group(1).decode("utf-8", "replace"))
                except Exception:
                    return False
            else:
                try:
                    fname = fn_m.group(1).decode("utf-8")
                except UnicodeDecodeError:
                    fname = fn_m.group(1).decode("latin-1")
            safe = self._safe_name(Path(fname).name, allow_basename=True)
            if not safe:
                return False
            try:
                if store is not None:
                    # Neon: spool this part, then push it to Object Storage and
                    # record its metadata row when the part is closed.
                    key = store.child_key(target_rel, safe)
                    state["cur_fp"] = neon.ObjectPart(store, key, safe, user)
                else:
                    state["cur_fp"] = open(target / safe, "wb")
            except OSError as exc:
                logger.warning("upload open failed for %s: %s", safe, exc)
                return False
            state["cur_name"] = safe
            state["cur_path"] = target / safe
            return True

        def process_buf(final: bool = False):
            buf = state["buf"]
            # Hold back enough bytes that a split boundary + leading CRLF can still match.
            hold = len(boundary) + 4
            while buf:
                if state["phase"] == "preamble":
                    idx = buf.find(boundary)
                    if idx < 0:
                        if final:
                            state["buf"] = b""
                        elif len(buf) > hold:
                            state["buf"] = buf[-hold:]
                        else:
                            state["buf"] = buf
                        return
                    buf = buf[idx + len(boundary):]
                    if buf.startswith(b"--"):
                        state["phase"] = "done"
                        state["buf"] = b""
                        return
                    if buf.startswith(b"\r\n"):
                        buf = buf[2:]
                    elif not buf.startswith(b"\n"):
                        # need more bytes to see CRLF after boundary
                        state["buf"] = boundary + buf
                        state["phase"] = "preamble"
                        return
                    state["phase"] = "headers"
                    state["buf"] = buf
                    continue

                if state["phase"] == "headers":
                    hend = buf.find(b"\r\n\r\n")
                    if hend < 0:
                        if len(buf) > 8192:
                            state["phase"] = "skip"
                            state["buf"] = buf
                            return
                        state["buf"] = buf
                        return
                    header_blob = buf[:hend]
                    buf = buf[hend + 4:]
                    _close_current()
                    _open_part(header_blob)
                    state["phase"] = "body"
                    state["buf"] = buf
                    continue

                if state["phase"] == "body":
                    idx = buf.find(boundary)
                    if idx < 0:
                        if final:
                            # no closing boundary — flush rest as content
                            if state["cur_fp"] is not None and buf:
                                try:
                                    if buf.endswith(b"\r\n"):
                                        buf = buf[:-2]
                                    state["cur_fp"].write(buf)
                                except OSError as exc:
                                    logger.warning("upload write failed: %s", exc)
                            if state["cur_fp"] is not None and state["cur_name"]:
                                saved.append(state["cur_name"])
                            _close_current()
                            state["buf"] = b""
                            state["phase"] = "done"
                            return
                        if len(buf) > hold:
                            data = buf[: len(buf) - hold]
                            state["buf"] = buf[len(buf) - hold:]
                            if state["cur_fp"] is not None and data:
                                try:
                                    state["cur_fp"].write(data)
                                except OSError as exc:
                                    logger.warning("upload write failed: %s", exc)
                                    _close_current()
                            return
                        state["buf"] = buf
                        return
                    data = buf[:idx]
                    if state["cur_fp"] is not None and data:
                        # strip the CRLF that belongs to multipart framing
                        if data.endswith(b"\r\n"):
                            data = data[:-2]
                        try:
                            state["cur_fp"].write(data)
                        except OSError as exc:
                            logger.warning("upload write failed: %s", exc)
                    if state["cur_fp"] is not None and state["cur_name"]:
                        saved.append(state["cur_name"])
                    _close_current()
                    buf = buf[idx + len(boundary):]
                    if buf.startswith(b"--"):
                        state["phase"] = "done"
                        state["buf"] = b""
                        return
                    if buf.startswith(b"\r\n"):
                        buf = buf[2:]
                    elif buf.startswith(b"\n"):
                        buf = buf[1:]
                    elif len(buf) < 2:
                        state["buf"] = boundary + buf
                        return
                    state["phase"] = "headers"
                    state["buf"] = buf
                    continue

                if state["phase"] == "skip":
                    idx = buf.find(boundary)
                    if idx < 0:
                        if final:
                            state["buf"] = b""
                            state["phase"] = "done"
                            return
                        state["buf"] = buf[-hold:] if len(buf) > hold else buf
                        return
                    buf = buf[idx + len(boundary):]
                    if buf.startswith(b"--"):
                        state["phase"] = "done"
                        state["buf"] = b""
                        return
                    if buf.startswith(b"\r\n"):
                        buf = buf[2:]
                    state["phase"] = "headers"
                    state["buf"] = buf
                    continue

                # done
                state["buf"] = b""
                return

            state["buf"] = b""
            if final and state["phase"] == "body":
                # flush any tail without trailing boundary (malformed but finish file)
                if state["cur_fp"] is not None and state["cur_name"]:
                    saved.append(state["cur_name"])
                _close_current()

        def on_chunk(chunk: bytes):
            state["buf"] += chunk
            process_buf(final=False)

        try:
            self._read_body_stream(on_chunk, chunk_size=UPLOAD_CHUNK)
            process_buf(final=True)
            _close_current()
        except Exception as exc:  # noqa: BLE001 - storage/network failure
            _close_current()
            logger.exception("upload failed")
            self._json_response(
                HTTPStatus.BAD_GATEWAY, {"ok": False, "error": f"storage error: {exc}"}
            )
            return

        if not saved:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "no file received"})
            return
        logger.info("Uploaded %s -> %s", ", ".join(saved), target)
        self._json_response(HTTPStatus.OK, {"ok": True, "files": saved})

    # -- resumable uploads -------------------------------------------------
    # The browser splits every file into UPLOAD_PART-sized chunks and posts them
    # one at a time. Nothing is written to the real destination until the last
    # chunk arrives, so a paused upload never leaves a half-written file behind
    # and "resume" only re-sends what never made it.

    def _handle_upstart(self):
        """Open a session: returns the session id and the negotiated part size."""
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        raw = (query.get("name") or [""])[0]
        name = self._safe_name(raw, allow_basename=True)
        if not name:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid file name"})
            return
        try:
            size = int((query.get("size") or ["0"])[0])
        except ValueError:
            size = 0
        if size < 0:
            size = 0
        store = storage()
        rel = self._rel_of(target) if store is not None else ""
        user = self._current_user()
        sid = uuid.uuid4().hex
        sess = UploadSession(sid, store, target, name, rel, user, size,
                             neon.guess_content_type(name) if (store and neon) else "")
        try:
            if store is not None:
                sess.upload_id = store.create_multipart(sess.key, content_type=sess.ctype)
            else:
                sess.scratch = target / f".bsup-{sid}.part"
                with open(sess.scratch, "wb"):
                    pass
        except OSError as exc:
            self._json_response(HTTPStatus.BAD_GATEWAY,
                                {"ok": False, "error": f"storage error: {exc}"})
            return
        except Exception as exc:  # noqa: BLE001 - object storage failure
            self._json_response(HTTPStatus.BAD_GATEWAY,
                                {"ok": False, "error": f"storage error: {exc}"})
            return
        with _UPLOADS_LOCK:
            _UPLOADS[sid] = sess
        self._json_response(HTTPStatus.OK, {
            "ok": True, "sid": sid, "name": name,
            "part": UPLOAD_PART, "parts": sess.part_count(),
        })

    def _handle_uppart(self):
        """Store one chunk. Re-sending the same index simply replaces it."""
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        sess = _upload_session((query.get("sid") or [""])[0])
        if sess is None:
            self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "unknown upload"})
            return
        try:
            index = int((query.get("part") or ["-1"])[0])
        except ValueError:
            index = -1
        total = sess.part_count()
        if index < 0 or index >= total:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad part index"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        expect = sess.size - index * UPLOAD_PART
        if expect > UPLOAD_PART:
            expect = UPLOAD_PART
        if length != expect:
            self._json_response(HTTPStatus.BAD_REQUEST,
                                {"ok": False, "error": "part length mismatch"})
            return

        # Buffer the chunk, then hand it over in one go: the storage SDKs want a
        # known length, and the spool keeps RAM flat for big parts.
        spool = tempfile.SpooledTemporaryFile(max_size=UPLOAD_PART_SPILL)
        try:
            received = self._read_body_stream(spool.write, chunk_size=UPLOAD_CHUNK)
            if received != length:
                # The connection died mid-chunk. Refuse it so the client keeps
                # re-sending this part instead of resuming on top of a hole.
                self._json_response(HTTPStatus.BAD_REQUEST,
                                    {"ok": False, "error": "part truncated"})
                return
            spool.seek(0)
            if sess.store is not None:
                etag = sess.store.upload_part(sess.key, sess.upload_id,
                                             index + 1, spool, length)
            else:
                with open(sess.scratch, "r+b") as fh:
                    fh.seek(index * UPLOAD_PART)
                    remaining = length
                    while remaining > 0:
                        piece = spool.read(min(UPLOAD_CHUNK, remaining))
                        if not piece:
                            break
                        fh.write(piece)
                        remaining -= len(piece)
                    fh.truncate(index * UPLOAD_PART + (length - remaining))
                etag = str(length)
        except Exception as exc:  # noqa: BLE001 - storage/network failure
            logger.exception("upload part failed")
            self._json_response(HTTPStatus.BAD_GATEWAY,
                                {"ok": False, "error": f"storage error: {exc}"})
            return
        finally:
            try:
                spool.close()
            except Exception:  # noqa: BLE001
                pass

        sess.parts[index] = etag
        self._json_response(HTTPStatus.OK, {"ok": True, "part": index, "bytes": length})

    def _handle_upfinish(self):
        """All chunks are in: assemble them and publish the real file."""
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        sid = (query.get("sid") or [""])[0]
        sess = _upload_session(sid)
        if sess is None:
            self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "unknown upload"})
            return
        total = sess.part_count()
        missing = [i for i in range(total) if i not in sess.parts]
        if missing:
            _upload_forget(sid)
            _upload_discard(sess)
            self._json_response(HTTPStatus.BAD_REQUEST, {
                "ok": False, "error": f"missing {len(missing)} part(s)",
                "missing": missing[:32],
            })
            return
        name = sess.name
        try:
            if sess.store is not None:
                sess.store.complete_multipart(
                    sess.key, sess.upload_id,
                    [(i + 1, sess.parts[i]) for i in range(total)],
                )
                sess.store.record_file(
                    sess.key, name=name, size=sess.size,
                    content_type=sess.ctype, user=sess.user,
                )
            else:
                dest = sess.target / name
                if sess.scratch is None or not sess.scratch.exists():
                    raise OSError("scratch file missing")
                os.replace(str(sess.scratch), str(dest))
                sess.scratch = None
        except Exception as exc:  # noqa: BLE001 - storage/network failure
            logger.exception("upload finish failed")
            _upload_forget(sid)
            _upload_discard(sess)
            self._json_response(HTTPStatus.BAD_GATEWAY,
                                {"ok": False, "error": f"storage error: {exc}"})
            return
        _upload_forget(sid)
        logger.info("Uploaded %s -> %s (%d part(s))", name, sess.target, total)
        self._json_response(HTTPStatus.OK, {"ok": True, "files": [name]})

    def _handle_updrop(self):
        """The client cancelled: throw away every chunk without publishing."""
        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        sid = (query.get("sid") or [""])[0]
        sess = _upload_forget(sid)
        if sess is not None:
            _upload_discard(sess)
        self._json_response(HTTPStatus.OK, {"ok": True})

    def _handle_mkdir(self):
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        try:
            payload = json.loads(self._read_body().decode("utf-8") or "{}")
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        name = self._safe_name(str(payload.get("name", "")))
        if not name:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid folder name"})
            return
        dest = target / name
        store = storage()
        if store is not None:
            rel = self._join_rel(target, name)
            if not store.exists(rel):
                try:
                    store.record_dir(rel, user=self._current_user())
                except Exception as exc:  # noqa: BLE001
                    logger.exception("mkdir failed")
                    self._json_response(
                        HTTPStatus.BAD_GATEWAY, {"ok": False, "error": f"storage error: {exc}"}
                    )
                    return
                logger.info("Created folder %s in %s", name, target)
            self._json_response(HTTPStatus.OK, {"ok": True, "name": name})
            return
        if dest.exists():
            self._json_response(HTTPStatus.CONFLICT, {"ok": False, "error": "already exists"})
            return
        try:
            dest.mkdir(parents=False, exist_ok=False)
        except OSError as exc:
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return
        logger.info("Created folder %s in %s", name, target)
        self._json_response(HTTPStatus.OK, {"ok": True, "name": name})

    def _handle_create(self):
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        try:
            payload = json.loads(self._read_body().decode("utf-8") or "{}")
        except json.JSONDecodeError:
            payload = {}
        if not isinstance(payload, dict):
            payload = {}
        raw = str(payload.get("name", "")).strip()
        # default extension .txt when none provided
        if raw and "." not in Path(raw).name:
            raw = raw + ".txt"
        name = self._safe_name(raw)
        if not name:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid file name"})
            return
        dest = target / name
        store = storage()
        if store is not None:
            rel = self._join_rel(target, name)
            try:
                key = store.child_key(self._rel_of(target), name)
                # An empty placeholder object, so the download link resolves.
                store.put_object(key, b"", content_type=neon.guess_content_type(name))
                store.record_file(
                    key, name=name, size=0,
                    content_type=neon.guess_content_type(name),
                    user=self._current_user(),
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("create failed")
                self._json_response(
                    HTTPStatus.BAD_GATEWAY, {"ok": False, "error": f"storage error: {exc}"}
                )
                return
            logger.info("Created file %s in %s", name, target)
            self._json_response(HTTPStatus.OK, {"ok": True, "name": name, "key": rel})
            return
        if dest.exists():
            self._json_response(HTTPStatus.CONFLICT, {"ok": False, "error": "already exists"})
            return
        try:
            dest.write_bytes(b"")
        except OSError as exc:
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return
        logger.info("Created file %s in %s", name, target)
        self._json_response(HTTPStatus.OK, {"ok": True, "name": name})

    def _payload(self) -> dict:
        try:
            data = json.loads(self._read_body().decode("utf-8") or "{}")
        except json.JSONDecodeError:
            return {}
        return data if isinstance(data, dict) else {}

    def _item_in(self, target: Path, name: str) -> Optional[Path]:
        """Resolve *name* inside *target*, ensuring containment."""
        safe = self._safe_name(name)
        if not safe:
            return None
        item = (target / safe).resolve()
        root = DOWNLOADS_DIR.resolve()
        if item != root and root not in item.parents:
            return None
        if self._is_hidden(item):
            return None
        return item

    def _dest_dir(self, dest: str) -> Optional[Path]:
        """Resolve a destination folder path relative to downloads root."""
        raw = (dest or "/").strip().replace("\\", "/")
        if not raw.startswith("/"):
            raw = "/" + raw
        parts = [p for p in raw.split("/") if p and p != "."]
        if any(p == ".." for p in parts):
            return None
        candidate = DOWNLOADS_DIR.resolve()
        for p in parts:
            safe = self._safe_name(p)
            if not safe:
                return None
            candidate = candidate / safe
        try:
            candidate = candidate.resolve()
        except OSError:
            return None
        root = DOWNLOADS_DIR.resolve()
        if candidate != root and root not in candidate.parents:
            return None
        if not candidate.is_dir() or self._is_hidden(candidate):
            return None
        return candidate

    def _handle_delete(self):
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        payload = self._payload()
        store = storage()
        if store is not None:
            name = self._safe_name(str(payload.get("name", "")))
            if not name:
                self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid name"})
                return
            try:
                found, _was_dir = store.delete_entry(self._join_rel(target, name))
            except Exception as exc:  # noqa: BLE001
                logger.exception("delete failed")
                self._json_response(
                    HTTPStatus.BAD_GATEWAY, {"ok": False, "error": f"storage error: {exc}"}
                )
                return
            if not found:
                self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})
                return
            _SIDEBAR_CACHE.update({"html": None, "ts": 0.0, "for": None})
            logger.info("Deleted %s from Neon", name)
            self._json_response(HTTPStatus.OK, {"ok": True, "name": name})
            return
        item = self._item_in(target, str(payload.get("name", "")))
        if item is None or not item.exists():
            self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})
            return
        if item.resolve() == DOWNLOADS_DIR.resolve():
            self._json_response(HTTPStatus.FORBIDDEN, {"ok": False, "error": "cannot delete root"})
            return
        try:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        except OSError as exc:
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return
        logger.info("Deleted %s", item)
        self._json_response(HTTPStatus.OK, {"ok": True, "name": item.name})

    def _neon_unsupported(self, operation: str):
        """Refuse a disk-only operation instead of quietly writing to disk."""
        self._json_response(
            HTTPStatus.NOT_IMPLEMENTED,
            {
                "ok": False,
                "error": f"{operation} is not available on Neon storage yet",
                "storage": "neon",
            },
        )

    def _handle_rename(self):
        if storage() is not None:
            return self._neon_unsupported("rename")
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        payload = self._payload()
        item = self._item_in(target, str(payload.get("name", "")))
        if item is None or not item.exists():
            self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})
            return
        new_name = self._safe_name(str(payload.get("newName", "")))
        if not new_name:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid new name"})
            return
        dest = target / new_name
        if dest.exists() and dest.resolve() != item.resolve():
            self._json_response(HTTPStatus.CONFLICT, {"ok": False, "error": "already exists"})
            return
        if dest.resolve() == item.resolve():
            self._json_response(HTTPStatus.OK, {"ok": True, "name": new_name})
            return
        try:
            item.rename(dest)
        except OSError as exc:
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return
        logger.info("Renamed %s -> %s", item.name, new_name)
        self._json_response(HTTPStatus.OK, {"ok": True, "name": new_name})

    def _handle_move(self):
        if storage() is not None:
            return self._neon_unsupported("move")
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        payload = self._payload()
        item = self._item_in(target, str(payload.get("name", "")))
        if item is None or not item.exists():
            self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})
            return
        dest_dir = self._dest_dir(str(payload.get("dest", "/")))
        if dest_dir is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid destination"})
            return
        dest = dest_dir / item.name
        try:
            if dest.resolve() == item.resolve():
                self._json_response(HTTPStatus.OK, {"ok": True, "name": item.name, "dest": str(payload.get("dest"))})
                return
            # reject moving a folder into itself or its child
            if item.is_dir():
                try:
                    dest_dir.resolve().relative_to(item.resolve())
                    self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "cannot move into itself"})
                    return
                except ValueError:
                    pass
            if dest.exists():
                self._json_response(HTTPStatus.CONFLICT, {"ok": False, "error": "already exists at destination"})
                return
        except OSError as exc:
            self._json_response(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            return
        # Run in background so long moves do not hit tunnel timeouts.
        job_id = uuid.uuid4().hex
        with _JOBS_LOCK:
            _JOBS[job_id] = {"status": "running", "op": "move", "name": item.name}

        def _run():
            try:
                shutil.move(str(item), str(dest))
                with _JOBS_LOCK:
                    _JOBS[job_id] = {
                        "status": "done", "ok": True, "name": item.name,
                        "dest": dest_dir.name or "/",
                    }
                logger.info("Moved %s -> %s", item, dest)
            except PermissionError as exc:
                with _JOBS_LOCK:
                    _JOBS[job_id] = {
                        "status": "done", "ok": False,
                        "error": "file is in use or locked: " + (str(exc) or item.name),
                    }
            except OSError as exc:
                with _JOBS_LOCK:
                    _JOBS[job_id] = {"status": "done", "ok": False, "error": str(exc)}

        threading.Thread(target=_run, daemon=True).start()
        self._json_response(HTTPStatus.OK, {"ok": True, "pending": True, "job": job_id, "name": item.name})

    def _handle_copy(self):
        if storage() is not None:
            return self._neon_unsupported("copy")
        target = self._target_dir()
        if target is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad directory"})
            return
        payload = self._payload()
        item = self._item_in(target, str(payload.get("name", "")))
        if item is None or not item.exists():
            self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})
            return
        dest_dir = self._dest_dir(str(payload.get("dest", "/")))
        if dest_dir is None:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid destination"})
            return
        dest = dest_dir / item.name
        if item.is_dir():
            try:
                dest_dir.resolve().relative_to(item.resolve())
                self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "cannot copy into itself"})
                return
            except ValueError:
                pass
        if dest.resolve() == item.resolve():
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "cannot copy onto itself"})
            return
        if dest.exists():
            self._json_response(HTTPStatus.CONFLICT, {"ok": False, "error": "already exists at destination"})
            return
        job_id = uuid.uuid4().hex
        with _JOBS_LOCK:
            _JOBS[job_id] = {"status": "running", "op": "copy", "name": item.name}

        def _run():
            try:
                if item.is_dir():
                    shutil.copytree(item, dest)
                else:
                    shutil.copy2(item, dest)
                with _JOBS_LOCK:
                    _JOBS[job_id] = {
                        "status": "done", "ok": True, "name": item.name,
                        "dest": dest_dir.name or "/",
                    }
                logger.info("Copied %s -> %s", item, dest)
            except PermissionError as exc:
                with _JOBS_LOCK:
                    _JOBS[job_id] = {
                        "status": "done", "ok": False,
                        "error": "file is in use or locked: " + (str(exc) or item.name),
                    }
            except OSError as exc:
                with _JOBS_LOCK:
                    _JOBS[job_id] = {"status": "done", "ok": False, "error": str(exc)}

        threading.Thread(target=_run, daemon=True).start()
        self._json_response(HTTPStatus.OK, {"ok": True, "pending": True, "job": job_id, "name": item.name})

    def _handle_jobstatus(self):
        try:
            payload = json.loads(self._read_body().decode("utf-8") or "{}")
        except json.JSONDecodeError:
            payload = {}
        job_id = str(payload.get("job", ""))
        with _JOBS_LOCK:
            job = _JOBS.get(job_id)
            # prune finished jobs older than 10 minutes
            now = time.time()
            for k, v in list(_JOBS.items()):
                if v.get("status") == "done" and v.get("_ts") and now - v["_ts"] > 600:
                    _JOBS.pop(k, None)
        if job is None:
            self._json_response(HTTPStatus.NOT_FOUND, {"ok": False, "error": "unknown job"})
            return
        if job.get("status") == "running":
            self._json_response(HTTPStatus.OK, {"ok": True, "running": True})
            return
        if job.get("ok"):
            with _JOBS_LOCK:
                job["_ts"] = time.time()
            self._json_response(HTTPStatus.OK, {
                "ok": True, "name": job.get("name", ""), "dest": job.get("dest", "/"),
            })
        else:
            with _JOBS_LOCK:
                job["_ts"] = time.time()
            self._json_response(HTTPStatus.OK, {"ok": False, "error": job.get("error", "failed")})

    def _handle_search(self):
        """Recursive name search across the whole server (downloads root)."""
        try:
            payload = json.loads(self._read_body().decode("utf-8") or "{}")
        except json.JSONDecodeError:
            payload = {}
        q = str(payload.get("q", "")).strip().lower()
        if not q:
            self._json_response(HTTPStatus.OK, {"ok": True, "results": []})
            return
        store = storage()
        if store is not None:
            try:
                results = store.search(q)
            except Exception as exc:  # noqa: BLE001
                logger.exception("search failed")
                self._json_response(
                    HTTPStatus.BAD_GATEWAY, {"ok": False, "error": f"storage error: {exc}"}
                )
                return
            logger.info("Search %r -> %d results (Neon)", q, len(results))
            self._json_response(HTTPStatus.OK, {"ok": True, "results": results})
            return
        root = DOWNLOADS_DIR.resolve()
        results = []
        max_results = 300

        def walk(directory: Path, rel_prefix: str):
            if len(results) >= max_results:
                return
            try:
                with os.scandir(directory) as it:
                    entries = sorted(it, key=lambda e: e.name.lower())
            except OSError:
                return
            for e in entries:
                if len(results) >= max_results:
                    return
                if e.name.startswith("."):
                    continue
                rel = rel_prefix + e.name
                try:
                    is_dir = e.is_dir(follow_symlinks=False)
                except OSError:
                    is_dir = False
                if q in e.name.lower():
                    results.append({
                        "name": e.name,
                        "path": rel,
                        "kind": 1 if is_dir else 2,
                        "href": urllib.parse.quote(rel, safe="") + ("/" if is_dir else ""),
                    })
                if is_dir:
                    walk(Path(e.path), rel + "/")

        walk(root, "/")
        logger.info("Search %r -> %d results", q, len(results))
        self._json_response(HTTPStatus.OK, {"ok": True, "results": results})

    def _handle_tree(self):
        """JSON tree of all folders under downloads (for move/copy dest picker)."""
        # The body is irrelevant here, but it still has to be drained: bytes
        # left in the socket are prepended to the next keep-alive request, and
        # the request line then reads "{}POST ..." which the server rejects
        # with 501 Unsupported method.
        self._read_body()
        store = storage()
        if store is not None:
            try:
                tree = store.tree()
            except Exception as exc:  # noqa: BLE001
                logger.exception("tree failed")
                self._json_response(
                    HTTPStatus.BAD_GATEWAY, {"ok": False, "error": f"storage error: {exc}"}
                )
                return
            self._json_response(HTTPStatus.OK, {"ok": True, "tree": tree})
            return
        root = DOWNLOADS_DIR.resolve()

        def walk(directory: Path) -> dict:
            rel = "/" + directory.relative_to(root).as_posix().lstrip("./")
            if rel == "/.":
                rel = "/"
            node = {"name": directory.name or "/", "path": rel, "dirs": []}
            try:
                subdirs = sorted(
                    (
                        p for p in directory.iterdir()
                        if p.is_dir() and not p.name.startswith(".")
                    ),
                    key=lambda p: p.name.lower(),
                )
            except OSError:
                subdirs = []
            for sd in subdirs:
                node["dirs"].append(walk(sd))
            return node

        self._json_response(HTTPStatus.OK, {"ok": True, "tree": walk(root)})

    def _send_head_neon(self, raw_path: str, query: dict, store):
        """Serve a request out of Neon instead of local disk.

        Files are handed to the browser as short-lived presigned links, so the
        bytes stream straight from Object Storage to the client and never pass
        through this process.
        """
        rel = neon.key_from_url(raw_path)

        if not rel:
            # Root folder: keep the trailing slash so relative links resolve.
            if not raw_path.endswith("/"):
                return self._redirect(raw_path + "/")
            return self._neon_listing("", store)

        entry = folder_stat(rel)
        if entry is None:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None

        if entry.is_dir:
            if not raw_path.endswith("/"):
                return self._redirect(raw_path + "/")
            if query.get("zip"):
                return self._send_neon_zip(
                    store, self._neon_zip_items(store, entry.key),
                    (entry.name or "downloads") + ".zip",
                )
            items_list = query.get("items") or []
            names = []
            for v in items_list:
                names.extend([n for n in str(v).split(",") if n])
            if names:
                return self._send_neon_zip(
                    store, self._neon_zip_items(store, entry.key, names),
                    (entry.name or "selected") + ".zip",
                )
            return self._neon_listing(rel, store)

        if query.get("zip"):
            return self._send_neon_zip(
                store, [(entry.name, entry.key)], Path(entry.name).stem + ".zip"
            )

        # The viewer pages read the file size; give them the metadata we have.
        self._entry_hint = entry
        if query.get("edit"):
            return self._render_editor_from_neon(entry, store)
        if query.get("inline") or query.get("raw"):
            ext = Path(entry.name).suffix.lower()
            img_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".bmp", ".avif"}
            vid_exts = {".mp4", ".webm", ".ogg", ".ogv", ".mov", ".mkv", ".avi", ".m4v", ".flv", ".wmv"}
            virtual = DOWNLOADS_DIR / rel
            if ext in img_exts and not query.get("raw"):
                return self._render_image_viewer(virtual)
            if ext in vid_exts and not query.get("raw"):
                return self._render_video_viewer(virtual)

        try:
            url = store.download_url(
                entry.key, filename=entry.name,
                inline=bool(query.get("inline") or query.get("raw")),
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("presign failed for %s", entry.key)
            self.send_error(HTTPStatus.BAD_GATEWAY, f"storage error: {exc}")
            return None
        logger.info("Presigned download for %s", entry.key)
        return self._redirect(url)

    def _redirect(self, location: str):
        self.send_response(HTTPStatus.FOUND)
        self.send_header("Location", location)
        self.send_header("Content-Length", "0")
        self.end_headers()
        return None

    def _entry_stat(self, path: Path):
        """Size/mtime for renderers, from Neon metadata when there is no file.

        Returns an object exposing ``st_size``/``st_mtime``, or ``None`` when the
        path is neither on disk nor known to Neon.
        """
        hint = getattr(self, "_entry_hint", None)
        if hint is not None:
            return neon.StatLike(st_size=hint.size, st_mtime=hint.mtime)
        try:
            return path.stat()
        except OSError:
            return None

    def _render_editor_from_neon(self, entry, store):
        """Text editor over a Neon object: load bytes, save back on POST."""
        try:
            body = store.get_object(entry.key)["Body"].read()
        except Exception as exc:  # noqa: BLE001
            logger.exception("editor load failed for %s", entry.key)
            self.send_error(HTTPStatus.BAD_GATEWAY, f"storage error: {exc}")
            return None
        try:
            text = body.decode("utf-8")
        except UnicodeDecodeError:
            self.send_error(HTTPStatus.BAD_REQUEST, "Not a UTF-8 text file")
            return None
        self._entry_hint = entry
        return self._render_editor_text(text, entry.name, entry.size)

    @staticmethod
    def _neon_zip_items(store, directory_key: str, names=None) -> list:
        """``(archive_name, object_key)`` pairs for a folder download.

        *names* limits the archive to a multi-select subset. Arc names are
        relative to *directory_key*, matching what the disk backend produces.
        """
        prefix = str(directory_key or "").strip("/")
        out: list = []
        if names is None:
            for full, key in store.walk_files(prefix):
                rel = full[len(prefix) + 1:] if prefix and full.startswith(prefix + "/") else full
                if any(part.startswith(".") for part in rel.split("/")):
                    continue  # hidden entries stay out, same as the disk backend
                out.append((rel, key))
            return out
        children = {c.name: c for c in store.list_dir("/" + prefix if prefix else "/")}
        for name in names:
            child = children.get(name)
            if child is None or name.startswith("."):
                continue
            if child.is_dir:
                sub = child.key.rstrip("/")
                for full, key in store.walk_files(sub):
                    rel = full[len(sub) + 1:] if sub and full.startswith(sub + "/") else full
                    if any(part.startswith(".") for part in rel.split("/")):
                        continue
                    out.append((f"{name}/{rel}", key))
            else:
                out.append((name, child.key))
        return out

    def _send_neon_zip(self, store, items, zip_name: str):
        """Stream a ZIP built from *items* straight from Object Storage."""
        spool = tempfile.SpooledTemporaryFile(max_size=32 * 1024 * 1024)
        try:
            with zipfile.ZipFile(spool, "w", zipfile.ZIP_DEFLATED) as zf:
                for arcname, key in items:
                    zf.writestr(arcname, store.get_object(key)["Body"].read())
            spool.seek(0)
            payload = spool.read()
        except Exception as exc:  # noqa: BLE001
            logger.exception("zip failed for %s", zip_name)
            self.send_error(HTTPStatus.BAD_GATEWAY, f"storage error: {exc}")
            return None
        finally:
            spool.close()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Disposition", self._content_disposition(zip_name))
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)
        return None

    def _neon_listing(self, rel: str, store):
        """Render a folder listing from Postgres metadata."""
        return self.list_directory(str(DOWNLOADS_DIR / rel if rel else DOWNLOADS_DIR))

    # -- request handling -------------------------------------------------
    def send_head(self):
        raw_path = urllib.parse.urlparse(self.path).path
        if raw_path in ("/favicon.ico", "/favicon.png"):
            return self._send_favicon()

        query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        store = storage()
        if store is not None:
            return self._send_head_neon(raw_path, query, store)

        path = self._resolve_path()
        if path is None or not path.exists() or self._is_hidden(path):
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None

        if path.is_dir():
            # Redirect directory requests without a trailing slash so relative
            # links inside the listing work correctly.
            path_only = urllib.parse.urlparse(self.path).path
            if not path_only.endswith("/"):
                self.send_response(HTTPStatus.MOVED_PERMANENTLY)
                self.send_header("Location", path_only + "/" + (
                    ("?" + urllib.parse.urlparse(self.path).query)
                    if urllib.parse.urlparse(self.path).query else ""
                ))
                self.send_header("Content-Length", "0")
                self.end_headers()
                return None
            if query.get("zip"):
                return self._send_zip(path)
            items_list = query.get("items") or []
            names = []
            for v in items_list:
                names.extend([n for n in str(v).split(",") if n])
            if names:
                return self._send_zip_items(path, names)
            return self.list_directory(str(path))

        if query.get("raw"):
            # raw media bytes (used by <video>/<audio> src) — inline + Range
            return self._send_file(path, inline=True)
        if query.get("zip"):
            return self._send_zip(path)
        if query.get("edit"):
            return self._render_editor(path)
        if query.get("inline"):
            ext = path.suffix.lower()
            img_exts = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico", ".bmp", ".avif"}
            vid_exts = {".mp4", ".webm", ".ogg", ".ogv", ".mov", ".mkv", ".avi", ".m4v", ".flv", ".wmv"}
            if ext in img_exts:
                return self._render_image_viewer(path)
            if ext in vid_exts:
                return self._render_video_viewer(path)
            return self._send_file(path, inline=True)
        return self._send_file(path)

    def _render_video_viewer(self, path: Path):
            """Serve the dedicated fullscreen video player page."""
            st = self._entry_stat(path)
            if st is None:
                self.send_error(HTTPStatus.NOT_FOUND, "File not found")
                return None
            page = (
                PLAYER_HTML
                .replace("__NAME__", html.escape(path.name))
                .replace("__SIZE__", html.escape(human_size(st.st_size)))
                .replace("__SRC__", html.escape(urllib.parse.quote(path.name)) + "?raw=1")
                .replace("__JSON_NAME__", json.dumps(path.name))
            )
            data = page.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            accept_enc = (self.headers.get("Accept-Encoding") or "").lower()
            if "gzip" in accept_enc:
                data = gzip.compress(data, compresslevel=5)
                self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            return io.BytesIO(data)

    def _render_image_viewer(self, path: Path):
        """Serve an image viewer page with a top download button."""
        st = self._entry_stat(path)
        if st is None:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None
        name = html.escape(path.name)
        size = human_size(st.st_size)
        # relative download URL (same path, no query -> attachment)
        page = (
            "<!DOCTYPE html>\n"
            '<html lang="en" data-theme="dark">\n'
            "<head>\n"
            '<meta charset="utf-8">\n'
            "<script>try{var t=localStorage.getItem('bs-theme');"
            "if(t==='light'||t==='dark')document.documentElement.setAttribute('data-theme',t);}catch(e){}</script>\n"
            '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            '<link rel="icon" href="/favicon.ico" sizes="any">\n'
            f"<title>Black File Manager - Black Server</title>\n"
            "<style>\n"
            ":root{--bg:#0b0f17;--panel:#111827;--border:#273244;--text:#e5e7eb;--text2:#9ca3af;--blue:#3b82f6;}\n"
            "html[data-theme=light]{--bg:#f3f4f6;--panel:#ffffff;--border:#d1d5db;--text:#111827;--text2:#6b7280;}\n"
            "*{box-sizing:border-box;margin:0;padding:0;}\n"
            "body{background:var(--bg);color:var(--text);font-family:Segoe UI,system-ui,sans-serif;"
            "height:100vh;display:flex;flex-direction:column;overflow:hidden;}\n"
            ".ihead{display:flex;align-items:center;gap:12px;padding:10px 16px;"
            "background:var(--panel);border-bottom:1px solid var(--border);flex-shrink:0;}\n"
            ".iname{font-weight:700;font-size:14px;overflow:hidden;text-overflow:ellipsis;"
            "white-space:nowrap;flex:1;min-width:0;}\n"
            ".isize{color:var(--text2);font-size:12px;white-space:nowrap;}\n"
            ".ibtn{border:1px solid var(--border);background:var(--panel);color:var(--text);"
            "padding:8px 16px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;"
            "text-decoration:none;display:inline-flex;align-items:center;gap:8px;transition:filter .12s;}\n"
            ".ibtn.primary{background:linear-gradient(135deg,#4f8cff,#3b5bfc);border-color:transparent;color:#fff;}\n"
            ".ibtn:hover{filter:brightness(1.1);}\n"
            ".istage{flex:1;min-height:0;display:flex;align-items:center;justify-content:center;"
            "padding:16px;overflow:auto;"
            "background:repeating-conic-gradient(#151a22 0% 25%, #0f131a 0% 50%) 50%/24px 24px;}\n"
            "html[data-theme=light] .istage{background:repeating-conic-gradient(#e5e7eb 0% 25%, #f9fafb 0% 50%) 50%/24px 24px;}\n"
            ".istage img{max-width:100%;max-height:100%;object-fit:contain;border-radius:8px;"
            "box-shadow:0 8px 32px rgba(0,0,0,.45);}\n"
            "</style>\n"
            "</head>\n"
            "<body>\n"
            '<div class="ihead">\n'
            f'<span class="iname">{name}</span>\n'
            f'<span class="isize">{size}</span>\n'
            '<button class="ibtn" type="button" onclick="closeViewerTab()">Back</button>\n'
            '<a class="ibtn primary" id="dlBtn" download>'
            "&#8681; Download</a>\n"
            "</div>\n"
            '<div class="istage">\n'
            f'<img alt="{name}" src="{html.escape(urllib.parse.quote(path.name))}">\n'
            "</div>\n"
            "<script>\n"
            "function closeViewerTab(){\n"
            "  try{window.close();}catch(e){}\n"
            "  setTimeout(function(){\n"
            "    try{ if(!window.closed) history.back(); }catch(e2){\n"
            "      location.href=location.pathname.replace(/[^/]*$/,'')||'/';\n"
            "    }\n"
            "  },80);\n"
            "}\n"
            "(function(){\n"
            "  var a=document.getElementById('dlBtn');\n"
            "  a.href=location.pathname;\n"
            "  a.setAttribute('download'," + json.dumps(path.name) + ");\n"
            "})();\n"
            "</script>\n"
            "</body>\n"
            "</html>\n"
        )
        data = page.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        accept_enc = (self.headers.get("Accept-Encoding") or "").lower()
        if "gzip" in accept_enc:
            data = gzip.compress(data, compresslevel=5)
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        return io.BytesIO(data)

    def _send_zip_items(self, directory: Path, names: list):
        """Stream a ZIP of selected items inside *directory*."""
        try:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                for name in names:
                    item = self._item_in(directory, name)
                    if item is None or not item.exists() or self._is_hidden(item):
                        continue
                    if item.is_dir():
                        for root, _dirs, files in os.walk(item):
                            for fn in files:
                                full = Path(root) / fn
                                if self._is_hidden(full):
                                    continue
                                zf.write(full, full.relative_to(directory))
                    else:
                        zf.write(item, item.name)
                zip_name = (directory.name or "selected") + ".zip"
            data = buf.getvalue()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "Cannot create ZIP")
            return None
        safe_ascii = "".join(
            ch if 32 <= ord(ch) < 127 and ch not in '"\\' else "_"
            for ch in zip_name
        )
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(len(data)))
        self.send_header(
            "Content-Disposition",
            f'attachment; filename="{safe_ascii}"; '
            f"filename*=UTF-8''{urllib.parse.quote(zip_name)}",
        )
        self.end_headers()
        return io.BytesIO(data)

    def _render_editor(self, path: Path):
        """Serve a standalone text/code editor page for *path*."""
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "Cannot read file")
            return None
        size = path.stat().st_size if path.exists() else 0
        return self._render_editor_text(text, path.name, size)

    def _render_editor_text(self, text: str, raw_name: str, size: int = 0):
        """Render the editor for already-loaded *text* (disk or Neon)."""
        if size > 5 * 1024 * 1024:
            self.send_error(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, "File too large to edit")
            return None
        name = html.escape(raw_name)
        esc_text = html.escape(text)
        theme = "dark"
        page = (
            "<!DOCTYPE html>\n"
            '<html lang="en" data-theme="dark">\n'
            "<head>\n"
            '<meta charset="utf-8">\n'
            "<script>try{var t=localStorage.getItem('bs-theme');"
            "if(t==='light'||t==='dark')document.documentElement.setAttribute('data-theme',t);}catch(e){}</script>\n"
            '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
            '<link rel="icon" href="/favicon.ico" sizes="any">\n'
            f"<title>Black File Manager - Black Server</title>\n"
            "<style>\n"
            ":root{--bg:#0b0f17;--panel:#111827;--panel2:#1f2937;--border:#273244;"
            "--text:#e5e7eb;--text2:#9ca3af;--blue:#3b82f6;--green:#22c55e;--red:#ef4444;}\n"
            "html[data-theme=light]{--bg:#f3f4f6;--panel:#ffffff;--panel2:#e5e7eb;"
            "--border:#d1d5db;--text:#111827;--text2:#6b7280;}\n"
            "*{box-sizing:border-box;margin:0;padding:0;}\n"
            "body{background:var(--bg);color:var(--text);font-family:Segoe UI,system-ui,sans-serif;"
            "height:100vh;display:flex;flex-direction:column;overflow:hidden;}\n"
            ".ehead{display:flex;align-items:center;gap:12px;padding:10px 16px;"
            "background:var(--panel);border-bottom:1px solid var(--border);flex-shrink:0;}\n"
            ".ehead .ename{font-weight:700;font-size:14px;overflow:hidden;"
            "text-overflow:ellipsis;white-space:nowrap;flex:1;min-width:0;}\n"
            ".ehead .esize{color:var(--text2);font-size:12px;white-space:nowrap;}\n"
            ".ebtn{border:1px solid var(--border);background:var(--panel2);color:var(--text);"
            "padding:7px 14px;border-radius:8px;font-size:13px;font-weight:600;cursor:pointer;"
            "transition:background .12s;}\n"
            ".ebtn:hover{background:var(--border);}\n"
            ".ebtn.primary{background:var(--blue);border-color:var(--blue);color:#fff;}\n"
            ".ebtn.primary:hover{filter:brightness(1.1);}\n"
            ".ebtn.primary.saved{background:var(--green);border-color:var(--green);}\n"
            ".estat{font-size:12px;color:var(--text2);min-width:60px;text-align:left;}\n"
            ".estat.ok{color:var(--green);}\n"
            ".estat.err{color:var(--red);}\n"
            ".ename[contenteditable=true]{outline:1px solid var(--blue);border-radius:6px;"
            "padding:2px 6px;background:var(--panel2);cursor:text;}\n"
            "#editor{flex:1;min-height:0;width:100%;border:none;outline:none;resize:none;"
            "background:var(--bg);color:var(--text);font-family:Consolas,Menlo,monospace;"
            "font-size:13.5px;line-height:1.55;padding:16px 18px;tab-size:4;white-space:pre;"
            "overflow:auto;}\n"
            "#editor::selection{background:rgba(59,130,246,.35);}\n"
            "</style>\n"
            "</head>\n"
            "<body>\n"
            '<div class="ehead">\n'
            f'<span class="ename" id="ename" title="Double-click to rename" '
            f'ondblclick="startRename()">{name}</span>\n'
            f'<span class="esize">{human_size(size)}</span>\n'
            '<span class="estat" id="estat"></span>\n'
            '<button class="ebtn" type="button" onclick="startRename()">Rename</button>\n'
            '<button class="ebtn" type="button" onclick="closeEditorTab()">Back</button>\n'
            '<button class="ebtn primary" type="button" id="saveBtn" onclick="saveFile()">Save</button>\n'
            "</div>\n"
            '<textarea id="editor" spellcheck="false" autocomplete="off" '
            'autocorrect="off" autocapitalize="off">'
            f"{esc_text}</textarea>\n"
            "<script>\n"
            "var FILENAME=" + json.dumps(raw_name) + ";\n"
            "var RENAMING=false;\n"
            "function closeEditorTab(){\n"
            "  try{window.close();}catch(e){}\n"
            "  setTimeout(function(){\n"
            "    try{ if(!window.closed) history.back(); }catch(e2){\n"
            "      location.href=location.pathname.replace(/[^/]*$/,'')||'/';\n"
            "    }\n"
            "  },80);\n"
            "}\n"
            "function startRename(){\n"
            "  if(RENAMING)return;\n"
            "  RENAMING=true;\n"
            "  var el=document.getElementById('ename');\n"
            "  el.contentEditable='true';\n"
            "  el.focus();\n"
            "  var r=document.createRange(); r.selectNodeContents(el);\n"
            "  var s=getSelection(); s.removeAllRanges(); s.addRange(r);\n"
            "  var st=document.getElementById('estat');\n"
            "  st.textContent='Enter to confirm'; st.className='estat';\n"
            "  if(!el._renBound){\n"
            "    el._renBound=true;\n"
            "    el.addEventListener('keydown',function(ev){\n"
            "      if(!RENAMING)return;\n"
            "      if(ev.key==='Enter'){ev.preventDefault();commitRename();}\n"
            "      if(ev.key==='Escape'){ev.preventDefault();cancelRename();}\n"
            "    });\n"
            "    el.addEventListener('blur',function(){ setTimeout(function(){ if(RENAMING) commitRename(); },120); });\n"
            "  }\n"
            "}\n"
            "function cancelRename(){\n"
            "  var el=document.getElementById('ename');\n"
            "  el.contentEditable='false';\n"
            "  el.textContent=FILENAME;\n"
            "  RENAMING=false;\n"
            "  var st=document.getElementById('estat'); st.textContent=''; st.className='estat';\n"
            "}\n"
            "function commitRename(){\n"
            "  if(!RENAMING)return;\n"
            "  var el=document.getElementById('ename');\n"
            "  var nn=(el.textContent||'').trim();\n"
            "  el.contentEditable='false';\n"
            "  RENAMING=false;\n"
            "  var st=document.getElementById('estat');\n"
            "  if(!nn||nn===FILENAME){ el.textContent=FILENAME; st.textContent=''; return; }\n"
            "  if(nn.indexOf('/')>=0||nn.indexOf('\\\\')>=0||nn==='.'||nn==='..'){\n"
            "    el.textContent=FILENAME; st.textContent='Invalid name'; st.className='estat err'; return;\n"
            "  }\n"
            "  st.textContent='Renaming...'; st.className='estat';\n"
            "  fetch(location.pathname+'?__api=rename',{\n"
            "    method:'POST',headers:{'Content-Type':'application/json'},\n"
            "    body:JSON.stringify({name:FILENAME,newName:nn})\n"
            "  }).then(function(r){return r.json().then(function(j){return {s:r.status,j:j};});})\n"
            "  .then(function(res){\n"
            "    if(res.j&&res.j.ok){\n"
            "      FILENAME=res.j.name||nn;\n"
            "      el.textContent=FILENAME;\n"
            "      document.title='Black File Manager - Black Server';\n"
            "      var dir=location.pathname.replace(/[^/]*$/,'');\n"
            "      history.replaceState(null,'',dir+encodeURIComponent(FILENAME)+'?edit=1');\n"
            "      st.textContent='Renamed'; st.className='estat ok';\n"
            "    }else{\n"
            "      el.textContent=FILENAME;\n"
            "      st.textContent=(res.j&&res.j.error)||'Rename failed'; st.className='estat err';\n"
            "    }\n"
            "  }).catch(function(){\n"
            "    el.textContent=FILENAME;\n"
            "    st.textContent='Network error'; st.className='estat err';\n"
            "  });\n"
            "}\n"
            "function saveFile(){\n"
            "  var btn=document.getElementById('saveBtn');\n"
            "  var st=document.getElementById('estat');\n"
            "  var ta=document.getElementById('editor');\n"
            "  btn.disabled=true; st.textContent='Saving...'; st.className='estat';\n"
            "  btn.textContent='Saving...';\n"
            "  fetch(location.pathname+'?__api=save',{\n"
            "    method:'POST',headers:{'Content-Type':'application/json'},\n"
            "    body:JSON.stringify({content:ta.value})\n"
            "  }).then(function(r){return r.json().then(function(j){return {s:r.status,j:j};});})\n"
            "  .then(function(res){\n"
            "    btn.disabled=false; btn.textContent='Save';\n"
            "    if(res.j&&res.j.ok){\n"
            "      st.textContent='Saved — closing in 3s'; st.className='estat ok';\n"
            "      btn.classList.add('saved');\n"
            "      setTimeout(function(){ try{window.close();}catch(e){} },3000);\n"
            "    }else{\n"
            "      st.textContent=(res.j&&res.j.error)||'Save failed'; st.className='estat err';\n"
            "    }\n"
            "  }).catch(function(){\n"
            "    btn.disabled=false; btn.textContent='Save';\n"
            "    st.textContent='Network error'; st.className='estat err';\n"
            "  });\n"
            "}\n"
            "document.getElementById('editor').addEventListener('keydown',function(e){\n"
            "  if((e.ctrlKey||e.metaKey)&&e.key==='s'){e.preventDefault();saveFile();}\n"
            "});\n"
            "window.addEventListener('popstate',function(){ try{window.close();}catch(e){} });\n"
            "document.addEventListener('keydown',function(e){\n"
            "  if(e.key==='F2'){e.preventDefault();startRename();}\n"
            "});\n"
            "</script>\n"
            "</body>\n"
            "</html>\n"
        )
        data = page.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        accept_enc = (self.headers.get("Accept-Encoding") or "").lower()
        if "gzip" in accept_enc:
            data = gzip.compress(data, compresslevel=5)
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        return io.BytesIO(data)

    def _handle_save(self):
        path = self._resolve_path()
        if path is None or path.is_dir() or self._is_hidden(path):
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad file"})
            return
        payload = self._payload()
        content = payload.get("content", "")
        if not isinstance(content, str):
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "bad content"})
            return
        data = content.encode("utf-8")
        if len(data) > 10 * 1024 * 1024:
            self._json_response(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "file too large"})
            return
        try:
            store = storage()
            if store is not None:
                key = store.child_key(self._rel_of(path.parent), path.name)
                store.put_object(key, data, content_type=neon.guess_content_type(path.name))
                store.record_file(
                    key, name=path.name, size=len(data),
                    content_type=neon.guess_content_type(path.name),
                    user=self._current_user(),
                )
            else:
                path.write_bytes(data)
        except Exception as exc:  # noqa: BLE001
            logger.exception("save failed")
            self._json_response(
                HTTPStatus.BAD_GATEWAY, {"ok": False, "error": f"storage error: {exc}"}
            )
            return
        logger.info("Saved %s (%d bytes)", path, len(data))
        self._json_response(HTTPStatus.OK, {"ok": True, "name": path.name, "size": len(data)})

    def _send_favicon(self):
        """Serve the project favicon (``favicon.ico`` next to ``server.py``)."""
        ico = PROJECT_ROOT / "favicon.ico"
        try:
            data = ico.read_bytes()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "No favicon")
            return None
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "image/x-icon")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "public, max-age=86400")
        self.end_headers()
        return io.BytesIO(data)

    def _send_zip(self, path: Path):
        """Stream *path* (file or folder) as a ZIP archive."""
        try:
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                if path.is_dir():
                    for root, _dirs, files in os.walk(path):
                        for name in files:
                            full = Path(root) / name
                            if self._is_hidden(full):
                                continue
                            zf.write(full, full.relative_to(path))
                    zip_name = (path.name or "downloads") + ".zip"
                else:
                    zf.write(path, path.name)
                    zip_name = path.stem + ".zip"
            data = buf.getvalue()
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "Cannot create ZIP")
            return None

        safe_ascii = "".join(
            ch if 32 <= ord(ch) < 127 and ch not in '"\\' else "_"
            for ch in zip_name
        )
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/zip")
        self.send_header("Content-Length", str(len(data)))
        self.send_header(
            "Content-Disposition",
            f'attachment; filename="{safe_ascii}"; '
            f"filename*=UTF-8''{urllib.parse.quote(zip_name)}",
        )
        self.end_headers()
        return io.BytesIO(data)

    def _send_file(self, path: Path, inline: bool = False):
        try:
            file_obj = open(path, "rb")
        except OSError:
            self.send_error(HTTPStatus.NOT_FOUND, "File not found")
            return None

        try:
            stat = os.fstat(file_obj.fileno())
            file_size = stat.st_size
            self._bytes_to_send = file_size
            start, end = 0, max(file_size - 1, 0)
            range_header = self.headers.get("Range")

            if range_header:
                parsed = self._parse_range(range_header, file_size)
                if parsed is None:
                    # Unsatisfiable range -> 416, tells clients the real size.
                    file_obj.close()
                    self.send_response(HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
                    self.send_header("Content-Range", f"bytes */{file_size}")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return None
                start, end = parsed
                self._bytes_to_send = end - start + 1
                self.send_response(HTTPStatus.PARTIAL_CONTENT)
                self.send_header(
                    "Content-Range", f"bytes {start}-{end}/{file_size}"
                )
            else:
                self.send_response(HTTPStatus.OK)

            self.send_header("Content-Type", self.guess_type(str(path)))
            self.send_header("Content-Length", str(self._bytes_to_send))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Last-Modified", self.date_time_string(stat.st_mtime))
            if inline:
                self.send_header("Content-Disposition", "inline")
            else:
                self.send_header("Content-Disposition", self._content_disposition(path.name))
            self.end_headers()

            if start:
                file_obj.seek(start)
            return file_obj
        except Exception:
            file_obj.close()
            raise

    @staticmethod
    def _parse_range(header: str, size: int) -> Optional[Tuple[int, int]]:
        """Parse a single ``bytes=`` range. Returns ``(start, end)`` or None."""
        if size <= 0 or not header.startswith("bytes="):
            return None
        spec = header[len("bytes="):].split(",")[0].strip()
        if "-" not in spec:
            return None
        start_str, end_str = spec.split("-", 1)
        try:
            if start_str == "":
                # Suffix range: last N bytes.
                length = int(end_str)
                if length <= 0:
                    return None
                start = max(0, size - length)
                end = size - 1
            else:
                start = int(start_str)
                end = int(end_str) if end_str else size - 1
        except ValueError:
            return None
        if start > end or start >= size:
            return None
        return start, min(end, size - 1)

    @staticmethod
    def _content_disposition(filename: str) -> str:
        """Build a safe ``Content-Disposition`` header for any filename."""
        safe_ascii = "".join(
            ch if 32 <= ord(ch) < 127 and ch not in '"\\' else "_"
            for ch in filename
        )
        encoded = urllib.parse.quote(filename)
        return f"attachment; filename=\"{safe_ascii}\"; filename*=UTF-8''{encoded}"

    def copyfile(self, source, outputfile):
        """Stream the body from disk in chunks (never load whole files in RAM).

        Respects ``_bytes_to_send`` so HTTP Range responses send exactly the
        requested slice, which enables resumable downloads of large files.
        """
        length = getattr(self, "_bytes_to_send", None)
        try:
            if length is None:
                shutil.copyfileobj(source, outputfile, length=CHUNK_SIZE)
                return
            remaining = length
            while remaining > 0:
                data = source.read(min(CHUNK_SIZE, remaining))
                if not data:
                    break
                outputfile.write(data)
                remaining -= len(data)
        except (BrokenPipeError, ConnectionResetError):
            logger.info("%s - client disconnected during transfer", self._client_ip())

    # -- directory listing -------------------------------------------------
    def list_directory(self, path):
        root = DOWNLOADS_DIR.resolve()
        current = Path(path)
        store = storage()

        if store is not None:
            # Neon mode: entries come from Postgres metadata, not from disk.
            try:
                display_path = "/" + current.resolve().relative_to(root).as_posix().lstrip("./")
            except (OSError, ValueError):
                display_path = "/"
            if display_path == "/.":
                display_path = "/"
            try:
                children = store.list_dir(display_path)
            except Exception as exc:  # noqa: BLE001
                logger.exception("listing failed for %s", display_path)
                self.send_error(HTTPStatus.BAD_GATEWAY, f"storage error: {exc}")
                return None
            # Keep plain names for the row builder; metadata rides along with it.
            info_by_name = {e.name: e for e in children}
            entries = sorted(
                (name for name in info_by_name if not name.startswith(".")),
                key=lambda n: (0 if info_by_name[n].is_dir else 1, n.lower()),
            )
        else:
            try:
                raw_entries = os.listdir(path)
            except OSError:
                self.send_error(HTTPStatus.NOT_FOUND, "No permission to list directory")
                return None
            # Hide dot-files from the public listing.
            raw_entries = [name for name in raw_entries if not name.startswith(".")]
            info_by_name = {}

            def _entry_sort_key(name: str):
                try:
                    is_dir = (current / name).is_dir()
                except OSError:
                    is_dir = False
                return (0 if is_dir else 1, name.lower())

            # Folders always first, then files (each group A→Z).
            entries = sorted(raw_entries, key=_entry_sort_key)
            display_path = "/" + current.relative_to(root).as_posix().lstrip("./")
            if display_path == "/.":
                display_path = "/"

        tree_html = _sidebar_tree(current)
        has_parent = display_path != "/"

        rows = []
        first_file_json = "null"
        file_count = 0

        if has_parent:
            rows.append(
                '<div class="frow dir parent-row" data-href="../" data-name=".." '
                'data-kind="0" data-size="-1" data-mtime="0" '
                'onclick="onRowClick(event, this)" '
                'ondblclick="onRowDblClick(event, this)" '
                'onmouseenter="hoverRow(this)" onmouseleave="unhoverRow(this)">'
                '<div class="fcell fchk"></div>'
                '<div class="fcell fname">'
                '<span class="badge folder">&#8617;</span>'
                '<span class="ftext"><span class="flabel">..</span>'
                '<span class="fsub">Parent folder</span></span></div>'
                '<div class="fcell fsize">&mdash;</div>'
                '<div class="fcell fmtime">&mdash;</div>'
                '<div class="fcell fdot"></div>'
                "</div>"
            )

        for name in entries:
            full = current / name
            link = urllib.parse.quote(name, safe="")
            label = html.escape(name)
            info = info_by_name.get(name)
            if info is not None:
                is_dir = info.is_dir
                mtime = info.mtime
                size_b = info.size
            else:
                is_dir = full.is_dir()
                try:
                    st = full.stat()
                    mtime = st.st_mtime
                    size_b = st.st_size
                except OSError:
                    mtime = 0.0
                    size_b = 0

            if is_dir:
                href = link + "/"
                if store is not None:
                    dir_files, dir_bytes = store.folder_stats(self._rel_of(full))
                else:
                    dir_files, dir_bytes = _folder_stats(full)
                dir_stats = f"{dir_files} files &middot; {human_size(dir_bytes)}"
                perms = "drwxr-xr-x" if store is not None else _perm_string(full)
                rel_path = (display_path.rstrip("/") + "/" + name) if display_path != "/" else "/" + name
                meta = json.dumps({
                    "name": name,
                    "href": href,
                    "size": human_size(dir_bytes),
                    "sizeB": dir_bytes,
                    "type": "folder",
                    "mtime": _fmt_mtime(mtime),
                    "mtimeTs": int(mtime),
                    "perm": perms,
                    "path": rel_path,
                    "ext": "folder",
                    "dir": 1,
                    "files": dir_files,
                }, ensure_ascii=False)
                rows.append(
                    f'<div class="frow dir" data-href="{href}" '
                    f'data-meta="{html.escape(meta, quote=True)}" '
                    f'data-name="{label}" '
                    f'data-kind="1" data-size="{dir_bytes}" data-mtime="{int(mtime)}" '
                    f'draggable="true" '
                    f'onclick="onRowClick(event, this)" '
                    f'ondblclick="onRowDblClick(event, this)" '
                    f'oncontextmenu="onRowContextMenu(event, this)" '
                    f'ondragstart="onDragStart(event, this)" '
                    f'ondragend="onDragEnd(event)" '
                    f'ondragover="onDragOver(event, this)" '
                    f'ondragleave="onDragLeave(event, this)" '
                    f'ondrop="onDropRow(event, this)" '
                    f'onmouseenter="hoverRow(this)" onmouseleave="unhoverRow(this)">'
                    f'<div class="fcell fchk" onclick="event.stopPropagation()">'
                    f'<input type="checkbox" class="chk" onchange="onCheckChange(this)">'
                    f'</div>'
                    f'<div class="fcell fname">'
                    f'<span class="badge folder">&#128193;</span>'
                    f'<span class="ftext"><span class="flabel">{label}</span>'
                    f'<span class="fsub">{dir_stats}</span></span></div>'
                    f'<div class="fcell fsize">{human_size(dir_bytes)}</div>'
                    f'<div class="fcell fmtime">{_fmt_mtime(mtime)}</div>'
                    f'<div class="fcell fdot">'
                    f'<button class="dots" type="button" title="More" '
                    f'onclick="openRowMenu(event, this)">&#8943;</button></div>'
                    f"</div>"
                )
            else:
                file_count += 1
                ext = full.suffix.lower()
                badge = _file_badge(ext)
                size_str = human_size(size_b)
                type_label = (ext.lstrip(".") + " file") if ext else "file"
                perms = "rw-r--r--" if store is not None else _perm_string(full)
                rel_path = (display_path.rstrip("/") + "/" + name) if display_path != "/" else "/" + name
                meta = json.dumps({
                    "name": name,
                    "href": link,
                    "size": size_str,
                    "sizeB": size_b,
                    "type": type_label,
                    "mtime": _fmt_mtime(mtime),
                    "mtimeTs": int(mtime),
                    "perm": perms,
                    "path": rel_path,
                    "ext": ext.lstrip(".") or "file",
                }, ensure_ascii=False)
                if first_file_json == "null":
                    first_file_json = meta
                rows.append(
                    f'<div class="frow file" data-meta="{html.escape(meta, quote=True)}" '
                    f'data-name="{label}" data-kind="2" data-size="{size_b}" '
                    f'data-mtime="{int(mtime)}" '
                    f'draggable="true" '
                    f'onclick="onRowClick(event, this)" '
                    f'ondblclick="onRowDblClick(event, this)" '
                    f'oncontextmenu="onRowContextMenu(event, this)" '
                    f'ondragstart="onDragStart(event, this)" '
                    f'ondragend="onDragEnd(event)" '
                    f'ondragover="onDragOver(event, this)" '
                    f'ondragleave="onDragLeave(event, this)" '
                    f'ondrop="onDropRow(event, this)" '
                    f'onmouseenter="hoverRow(this)" onmouseleave="unhoverRow(this)">'
                    f'<div class="fcell fchk" onclick="event.stopPropagation()">'
                    f'<input type="checkbox" class="chk" onchange="onCheckChange(this)">'
                    f'</div>'
                    f'<div class="fcell fname">{badge}'
                    f'<span class="ftext"><span class="flabel">{label}</span>'
                    f'<span class="fsub">{type_label} &middot; {size_str}</span></span></div>'
                    f'<div class="fcell fsize">{size_str}</div>'
                    f'<div class="fcell fmtime">{_fmt_mtime(mtime)}</div>'
                    f'<div class="fcell fdot">'
                    f'<button class="dots" type="button" title="More" '
                    f'onclick="openRowMenu(event, this)">&#8943;</button></div>'
                    f"</div>"
                )

        if not rows:
            body_rows = (
                '<div class="empty-state">'
                "<div class=\"empty-icon\">&#128196;</div>"
                "<div>No files in this folder</div>"
                "</div>"
            )
        else:
            body_rows = "\n".join(rows)

        page = f"""<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<script>try{{var t=localStorage.getItem("bs-theme");var TH=["dark","light","blue","purple","orange","green","sunset"];if(TH.indexOf(t)>=0)document.documentElement.setAttribute("data-theme",t);}}catch(e){{}}</script>
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Black File Manager - Black Server</title>
<link rel="icon" href="/favicon.ico" sizes="any">
<style>
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  html {{
    user-select: none;
    -webkit-user-select: none;
  }}
  input, textarea, select, [contenteditable], .flabel, .search input {{
    user-select: text;
    -webkit-user-select: text;
  }}

  :root, [data-theme="dark"] {{
    --bg: #070b14;
    --bg2: #0b1120;
    --panel: #0e1628;
    --panel2: #111c33;
    --panel3: #152240;
    --border: #1c2d4f;
    --border2: #243b66;
    --text: #e8eefc;
    --text2: #8ba0c5;
    --text3: #5a719e;
    --blue: #3b82f6;
    --blue2: #2563eb;
    --green: #22c55e;
    --red: #ef4444;
    --purple: #8b5cf6;
    --pink: #ec4899;
    --teal: #14b8a6;
    --orange: #f59e0b;
    --sel: rgba(59,130,246,.14);
    --sel-border: rgba(59,130,246,.55);
    --hover: rgba(59,130,246,.07);
    --radius: 14px;
    --shadow: 0 8px 32px rgba(0,0,0,.45);
    --sidebar-bg: rgba(0,0,0,.18);
    --details-bg: rgba(0,0,0,.22);
    --mbar-bg: #1a2744;
    --switch-bg: #1a2744;
    --switch-knob: #f8fafc;
    --glass-bg: rgba(14,22,40,.92);
    --input-bg: #0e1628;
    --body-transition: background .45s ease, color .45s ease;
    --scroll-thumb: rgba(79,140,255,.55);
    --scroll-thumb-hover: rgba(109,150,255,.85);
    --scroll-track: rgba(255,255,255,.04);
    --accent-rgb: 59,130,246;
    --folder-a: #60a5fa;
    --folder-b: #3b82f6;
  }}

  [data-theme="light"] {{
    --bg: #eef1f6;
    --bg2: #f7f8fb;
    --panel: #ffffff;
    --panel2: #f3f5f9;
    --panel3: #e8ecf4;
    --border: #dde3ee;
    --border2: #c9d2e3;
    --text: #152038;
    --text2: #5a6a88;
    --text3: #8b98b3;
    --blue: #3b82f6;
    --blue2: #2563eb;
    --green: #16a34a;
    --red: #dc2626;
    --purple: #8b5cf6;
    --pink: #ec4899;
    --teal: #14b8a6;
    --orange: #f59e0b;
    --sel: rgba(59,130,246,.12);
    --sel-border: rgba(59,130,246,.5);
    --hover: rgba(59,130,246,.06);
    --radius: 14px;
    --shadow: 0 8px 28px rgba(20,35,70,.12);
    --sidebar-bg: #f7f8fb;
    --details-bg: #f7f8fb;
    --mbar-bg: #e4e9f2;
    --switch-bg: #d8dfecc;
    --switch-knob: #ffffff;
    --glass-bg: rgba(255,255,255,.88);
    --input-bg: #ffffff;
    --body-transition: background .45s ease, color .45s ease;
    --scroll-thumb: rgba(59,130,246,.45);
    --scroll-thumb-hover: rgba(37,99,235,.75);
    --scroll-track: rgba(0,0,0,.05);
    --accent-rgb: 59,130,246;
    --folder-a: #93c5fd;
    --folder-b: #3b82f6;
  }}
  /* fix typo-safe: real value */
  [data-theme="light"] {{ --switch-bg: #d8dfec; }}

  /* ===== EXTRA THEMES ===== */
  [data-theme="blue"] {{
    --bg: #061224; --bg2: #08182f; --panel: #0a1e3c; --panel2: #0d2649; --panel3: #113059;
    --border: #1a3f70; --border2: #24518c;
    --text: #e6f2ff; --text2: #8fbaea; --text3: #5c8ac4;
    --blue: #38a6ff; --blue2: #1f7fd6;
    --sel: rgba(56,166,255,.16); --sel-border: rgba(56,166,255,.6); --hover: rgba(56,166,255,.08);
    --shadow: 0 8px 32px rgba(0,0,0,.5);
    --sidebar-bg: rgba(4,14,30,.4); --details-bg: rgba(4,14,30,.45);
    --mbar-bg: #0f2c52; --switch-bg: #0f2c52; --switch-knob: #eaf6ff;
    --glass-bg: rgba(10,30,60,.92); --input-bg: #0a1e3c;
    --scroll-thumb: rgba(56,166,255,.55); --scroll-thumb-hover: rgba(56,166,255,.85);
    --scroll-track: rgba(255,255,255,.05);
    --accent-rgb: 56,166,255;
    --folder-a: #7dd3fc; --folder-b: #38a6ff;
  }}
  [data-theme="purple"] {{
    --bg: #12071f; --bg2: #180a2b; --panel: #1d0d33; --panel2: #25123f; --panel3: #2e174e;
    --border: #46206e; --border2: #5b2c8c;
    --text: #f6eaff; --text2: #b895d8; --text3: #8a63b0;
    --blue: #c026d3; --blue2: #9d17b0;
    --sel: rgba(192,38,211,.16); --sel-border: rgba(192,38,211,.6); --hover: rgba(192,38,211,.09);
    --shadow: 0 8px 32px rgba(0,0,0,.5);
    --sidebar-bg: rgba(20,7,36,.45); --details-bg: rgba(20,7,36,.5);
    --mbar-bg: #341856; --switch-bg: #341856; --switch-knob: #fae8ff;
    --glass-bg: rgba(29,13,51,.92); --input-bg: #1d0d33;
    --scroll-thumb: rgba(216,80,240,.5); --scroll-thumb-hover: rgba(216,80,240,.8);
    --scroll-track: rgba(255,255,255,.05);
    --accent-rgb: 192,38,211;
    --folder-a: #e879f9; --folder-b: #c026d3;
  }}
  [data-theme="orange"] {{
    --bg: #130c03; --bg2: #1a1205; --panel: #201607; --panel2: #291c09; --panel3: #33230c;
    --border: #553a12; --border2: #6e4c19;
    --text: #fdf3e3; --text2: #c9a670; --text3: #9a7743;
    --blue: #f59e0b; --blue2: #d97706;
    --sel: rgba(245,158,11,.15); --sel-border: rgba(245,158,11,.6); --hover: rgba(245,158,11,.08);
    --shadow: 0 8px 32px rgba(0,0,0,.5);
    --sidebar-bg: rgba(24,15,4,.45); --details-bg: rgba(24,15,4,.5);
    --mbar-bg: #3a270c; --switch-bg: #3a270c; --switch-knob: #fff7e6;
    --glass-bg: rgba(32,22,7,.92); --input-bg: #201607;
    --scroll-thumb: rgba(245,158,11,.5); --scroll-thumb-hover: rgba(245,158,11,.8);
    --scroll-track: rgba(255,255,255,.05);
    --accent-rgb: 245,158,11;
    --folder-a: #fbbf24; --folder-b: #f59e0b;
  }}
  [data-theme="green"] {{
    --bg: #04120b; --bg2: #06180f; --panel: #082015; --panel2: #0b291b; --panel3: #0f3323;
    --border: #17513a; --border2: #1f6b4c;
    --text: #e8fdf2; --text2: #86c9a8; --text3: #579c7b;
    --blue: #22c55e; --blue2: #16a34a;
    --sel: rgba(34,197,94,.15); --sel-border: rgba(34,197,94,.6); --hover: rgba(34,197,94,.08);
    --shadow: 0 8px 32px rgba(0,0,0,.5);
    --sidebar-bg: rgba(3,18,11,.45); --details-bg: rgba(3,18,11,.5);
    --mbar-bg: #0d3a25; --switch-bg: #0d3a25; --switch-knob: #ecfdf3;
    --glass-bg: rgba(8,32,21,.92); --input-bg: #082015;
    --scroll-thumb: rgba(34,197,94,.5); --scroll-thumb-hover: rgba(34,197,94,.8);
    --scroll-track: rgba(255,255,255,.05);
    --accent-rgb: 34,197,94;
    --folder-a: #4ade80; --folder-b: #22c55e;
  }}
  [data-theme="sunset"] {{
    --bg: #150a26; --bg2: #1c0f33; --panel: #22123d; --panel2: #2a1749; --panel3: #331d58;
    --border: #4b2a7d; --border2: #5f3799;
    --text: #f7ecff; --text2: #bfa3dd; --text3: #9074b8;
    --blue: #f59e0b; --blue2: #d97706;
    --sel: rgba(245,158,11,.15); --sel-border: rgba(245,158,11,.6); --hover: rgba(245,158,11,.08);
    --shadow: 0 8px 32px rgba(0,0,0,.5);
    --sidebar-bg: rgba(21,10,38,.45); --details-bg: rgba(21,10,38,.5);
    --mbar-bg: #3a2160; --switch-bg: #3a2160; --switch-knob: #fff3df;
    --glass-bg: rgba(34,18,61,.92); --input-bg: #22123d;
    --scroll-thumb: rgba(245,158,11,.5); --scroll-thumb-hover: rgba(245,158,11,.8);
    --scroll-track: rgba(255,255,255,.05);
    --accent-rgb: 245,158,11;
    --folder-a: #fbbf24; --folder-b: #f59e0b;
  }}

  html {{ font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
         -webkit-font-smoothing: antialiased; }}

  body {{
    background: var(--bg);
    color: var(--text);
    height: 100vh;
    padding: 18px;
    display: flex;
    flex-direction: column;
    overflow: hidden;
    transition: var(--body-transition);
  }}
  body, .app, .topbar, .machine, .metric, .gear, .search input,
  .sidebar, .center, .details, .meta-table, .sort-menu, .view-toggle,
  .sort-btn, .btn, .list-tools, .thead, .toast {{
    transition: background .45s ease, color .45s ease,
                border-color .45s ease, box-shadow .45s ease;
  }}

  /* ===== SCROLLBARS ===== */
  * {{
    scrollbar-width: thin;
    scrollbar-color: var(--scroll-thumb) var(--scroll-track);
  }}
  ::-webkit-scrollbar {{ width: 10px; height: 10px; }}
  ::-webkit-scrollbar-track {{ background: var(--scroll-track); }}
  ::-webkit-scrollbar-thumb {{
    background: linear-gradient(180deg, var(--scroll-thumb), rgba(109,77,246,.55));
    border-radius: 999px;
    border: 2px solid transparent;
    background-clip: padding-box;
  }}
  ::-webkit-scrollbar-thumb:hover {{
    background: linear-gradient(180deg, var(--scroll-thumb-hover), rgba(139,92,246,.8));
    background-clip: padding-box;
  }}
  ::-webkit-scrollbar-corner {{ background: transparent; }}

  /* ===== TOP BAR ===== */
  .topbar {{
    display: flex; align-items: center; gap: 14px;
    margin-bottom: 16px;
    position: relative;
    z-index: 120;
    flex-shrink: 0;
  }}
  .search {{
    flex: 1; max-width: 520px; position: relative;
    z-index: 121;
  }}
  .search svg {{
    position: absolute; left: 14px; top: 50%; transform: translateY(-50%);
    width: 16px; height: 16px; stroke: var(--text3); fill: none;
    pointer-events: none;
  }}
  .search input {{
    width: 100%; padding: 12px 52px 12px 42px;
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 12px; color: var(--text); font-size: 14px;
    outline: none; transition: border-color .2s, box-shadow .2s, background .45s;
  }}
  .search input::placeholder {{ color: var(--text3); }}
  .search input:focus {{
    border-color: var(--blue);
    box-shadow: 0 0 0 3px rgba(var(--accent-rgb), .2);
  }}
  .search-scope {{
    position: absolute; right: 8px; top: 50%; transform: translateY(-50%);
    z-index: 130;
  }}
  .scope-dots {{
    width: 34px; height: 34px; border-radius: 9px;
    background: var(--panel2); border: 1px solid var(--border);
    color: var(--text2); font-size: 18px; line-height: 1;
    cursor: pointer; transition: all .15s;
    display: flex; align-items: center; justify-content: center;
    padding: 0;
  }}
  .scope-dots:hover, .scope-dots.on {{
    border-color: var(--blue); color: var(--text);
    background: var(--panel3);
  }}
  .scope-menu {{
    position: absolute; left: 0; right: 0; top: calc(100% + 6px);
    width: auto; max-width: 100%; z-index: 131;
    background: var(--glass-bg); border: 1px solid var(--border2);
    border-radius: 14px; overflow: hidden;
    box-shadow: 0 16px 48px rgba(0,0,0,.4), inset 0 1px 0 rgba(255,255,255,.06);
    backdrop-filter: blur(24px) saturate(1.3);
    -webkit-backdrop-filter: blur(24px) saturate(1.3);
    display: none;
    transform-origin: top left;
    animation: scopePop .16s cubic-bezier(.16,1,.3,1);
  }}
  .scope-menu.open {{ display: block; }}
  @keyframes scopePop {{
    from {{ opacity: 0; transform: translateY(-6px) scale(.97); }}
    to {{ opacity: 1; transform: translateY(0) scale(1); }}
  }}
  .scope-menu button {{
    display: flex; align-items: center; gap: 9px;
    width: 100%; text-align: right;
    padding: 10px 12px; border: none; background: transparent;
    color: var(--text2); cursor: pointer;
    transition: background .12s, color .12s;
  }}
  .scope-menu button + button {{ border-top: 1px solid var(--border); }}
  .scope-menu button:hover {{ background: var(--hover); color: var(--text); }}
  .scope-menu button.on {{ background: rgba(var(--accent-rgb), .12); color: var(--text); }}
  .sm-icon {{
    width: 32px; height: 32px; border-radius: 9px; flex-shrink: 0;
    background: var(--panel3); border: 1px solid var(--border);
    display: flex; align-items: center; justify-content: center;
    font-size: 15px;
  }}
  .scope-menu button.on .sm-icon {{
    background: rgba(var(--accent-rgb), .2); border-color: rgba(var(--accent-rgb), .45);
  }}
  .sm-text {{ display: flex; flex-direction: column; gap: 1px; min-width: 0; flex: 1; }}
  .sm-title {{ font-size: 13px; font-weight: 700; color: var(--text); }}
  .sm-desc {{ font-size: 11px; color: var(--text3); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
  .sm-check {{
    font-size: 14px; color: var(--blue); font-weight: 800;
    opacity: 0; transform: scale(.5); transition: all .15s;
  }}
  .scope-menu button.on .sm-check {{ opacity: 1; transform: scale(1); }}
  .top-right {{
    margin-left: auto; display: flex; align-items: center; gap: 12px;
  }}

  /* ===== THEME PICKER BUTTON + MENU ===== */
  .theme-wrap {{ position: relative; flex-shrink: 0; }}
  .theme-btn {{
    display: flex; align-items: center; gap: 7px;
    height: 34px; padding: 0 12px;
    background: var(--switch-bg);
    border: 1px solid var(--border);
    border-radius: 999px; cursor: pointer;
    color: var(--text); font-family: inherit;
    box-shadow: inset 0 2px 6px rgba(0,0,0,.25);
    transition: background .3s ease, border-color .3s ease, box-shadow .3s ease;
    outline: none;
  }}
  .theme-btn:hover {{
    border-color: rgba(var(--accent-rgb), .55);
    box-shadow: 0 0 0 3px rgba(var(--accent-rgb), .16);
  }}
  .theme-btn .ti {{ font-size: 15px; line-height: 1; }}
  .theme-btn .tcaret {{
    font-size: 10px; color: var(--text3);
    transition: transform .2s ease;
  }}
  .theme-wrap.open .theme-btn .tcaret {{ transform: rotate(180deg); }}
  .theme-menu {{
    position: absolute; top: calc(100% + 10px); right: 0;
    min-width: 190px; padding: 6px;
    background: var(--glass-bg); backdrop-filter: blur(14px); -webkit-backdrop-filter: blur(14px);
    border: 1px solid var(--border2); border-radius: 14px;
    box-shadow: var(--shadow);
    display: none; z-index: 40;
  }}
  .theme-menu.open {{ display: block; animation: menuIn .16s cubic-bezier(.16,1,.3,1); }}
  @keyframes menuIn {{
    from {{ opacity: 0; transform: translateY(-6px); }}
    to {{ opacity: 1; transform: translateY(0); }}
  }}
  .theme-menu button {{
    display: flex; align-items: center; gap: 10px;
    width: 100%; padding: 9px 10px;
    border: none; background: transparent; border-radius: 10px;
    color: var(--text2); font-size: 13px; font-weight: 600;
    font-family: inherit; cursor: pointer; text-align: left;
    transition: background .12s, color .12s;
  }}
  .theme-menu button:hover {{ background: var(--hover); color: var(--text); }}
  .theme-menu button.on {{ background: rgba(var(--accent-rgb), .16); color: var(--text); }}
  .tsw {{
    width: 20px; height: 20px; border-radius: 50%; flex-shrink: 0;
    border: 1px solid rgba(255,255,255,.25);
    box-shadow: inset 0 0 0 1px rgba(0,0,0,.2);
  }}
  .tnm {{ flex: 1; min-width: 0; }}
  .tck {{
    font-size: 13px; font-weight: 800; color: var(--blue);
    opacity: 0; transform: scale(.5); transition: all .15s;
  }}
  .theme-menu button.on .tck, .set-themes button.on .tck {{ opacity: 1; transform: scale(1); }}

  .machine {{
    display: flex; align-items: center; gap: 10px;
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 12px; padding: 8px 14px;
  }}
  .machine .mdot {{
    width: 9px; height: 9px; border-radius: 50%;
    background: var(--green); box-shadow: 0 0 8px var(--green);
    animation: pulse 2s infinite;
  }}
  @keyframes pulse {{
    0%,100% {{ opacity: 1; }} 50% {{ opacity: .55; }}
  }}
  .machine .mtext {{ line-height: 1.15; }}
  .machine .mtitle {{ font-size: 13px; font-weight: 600; }}
  .machine .mstatus {{ font-size: 11px; color: var(--green); font-weight: 600; }}
  .metrics {{
    display: flex; gap: 8px;
  }}
  .metric {{
    background: var(--panel); border: 1px solid var(--border);
    border-radius: 10px; padding: 7px 11px; text-align: center;
    min-width: 58px;
  }}
  .metric .mlabel {{
    font-size: 9px; font-weight: 700; letter-spacing: .06em;
    color: var(--text2); margin-bottom: 4px;
  }}
  .metric .mbar {{
    height: 5px; border-radius: 3px; background: var(--mbar-bg); overflow: hidden;
  }}
  .metric .mbar i {{
    display: block; height: 100%; border-radius: 3px;
  }}
  .metric.cpu .mbar i {{ width: 34%; background: var(--blue); }}
  .metric.ram .mbar i {{ width: 56%; background: var(--green); }}
  .metric.disk .mbar i {{ width: 42%; background: var(--purple); }}
  .gear {{
    width: 42px; height: 42px; border-radius: 12px;
    background: var(--panel); border: 1px solid var(--border);
    color: var(--text2); font-size: 18px; cursor: pointer;
    display: flex; align-items: center; justify-content: center;
    transition: all .2s;
  }}
  .gear:hover {{ border-color: var(--blue); color: var(--text); }}

  /* ===== MAIN CARD ===== */
  .app {{
    background: linear-gradient(180deg, var(--panel) 0%, var(--bg2) 100%);
    border: 1px solid var(--border);
    border-radius: 20px;
    box-shadow: var(--shadow);
    overflow: hidden;
    animation: rise .45s cubic-bezier(.16,1,.3,1) both;
    transition: background .45s ease, border-color .45s ease, box-shadow .45s ease;
    position: relative;
    z-index: 1;
    /* Flex column so .layout can shrink and #tbody becomes a real scroll box. */
    display: flex;
    flex-direction: column;
    flex: 1 1 auto;
    min-height: 0;
  }}
  @keyframes rise {{
    from {{ opacity: 0; transform: translateY(16px); }}
    to {{ opacity: 1; transform: translateY(0); }}
  }}

  .app-head {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 22px 26px 18px;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }}
  .app-head h1 {{
    font-size: 26px; font-weight: 700; letter-spacing: -.02em;
  }}
  .app-head .sub {{
    font-size: 13px; color: var(--text2); margin-top: 4px;
  }}
  .head-actions {{ display: flex; gap: 10px; }}
  .btn {{
    display: inline-flex; align-items: center; gap: 8px;
    padding: 10px 18px; border-radius: 11px;
    font-size: 13.5px; font-weight: 600; cursor: pointer;
    border: 1px solid var(--border2); background: var(--panel2);
    color: var(--text); transition: all .2s;
  }}
  .btn:hover {{ background: var(--panel3); border-color: var(--blue); }}
  .btn.primary {{
    background: linear-gradient(135deg, var(--folder-a) 0%, var(--blue) 50%, var(--blue2) 100%);
    border: none; color: #fff;
    box-shadow: 0 4px 18px rgba(var(--accent-rgb),.35);
  }}
  .btn.primary:hover {{
    transform: translateY(-1px);
    box-shadow: 0 6px 22px rgba(var(--accent-rgb),.5);
  }}

  /* ===== 3-COLUMN LAYOUT ===== */
  .layout {{
    display: grid;
    grid-template-columns: var(--sidebar-w, 210px) 1fr 300px;
    /* One row that fills the flexed .app height and may shrink below content. */
    grid-template-rows: minmax(0, 1fr);
    flex: 1 1 auto;
    min-height: 0;
    overflow: hidden;
    position: relative;
  }}

  /* ---- sidebar tree ---- */
  .sidebar {{
    background: var(--sidebar-bg);
    border-right: 1px solid var(--border);
    padding: 14px 10px;
    overflow-y: auto;
    min-height: 0;
    overscroll-behavior: contain;
    position: relative;
  }}
  /* Handle lives on .layout (not inside the scrolling .sidebar) so it stays put. */
  .sidebar-resize {{
    position: absolute; top: 0; bottom: 0;
    left: calc(var(--sidebar-w, 210px) - 3px);
    width: 7px;
    cursor: col-resize; z-index: 6; background: transparent;
  }}
  .sidebar-resize:hover, .sidebar-resize.active {{
    background: linear-gradient(90deg, transparent, var(--blue) 60%);
    opacity: .7;
  }}
  .sidebar-resize.active {{ opacity: 1; }}
  .tkids {{
    padding-left: 14px;
    margin-left: 7px;
    border-left: 1.5px solid var(--border2);
    position: relative;
  }}
  .tnode > .tkids {{ display: none; }}
  .tnode.open > .tkids {{ display: block; }}
  .tnode.open:not(.current) > .trow {{ opacity: .96; }}
  .tnode.open > .tkids > .tnode {{
    border-left: none;
  }}
  .tnode:has(> .tkids) > .trow {{
    border: 1px solid transparent;
    border-radius: 8px;
  }}
  .tnode.open:has(> .tkids) > .trow {{
    border-color: color-mix(in srgb, var(--blue) 28%, transparent);
    background: color-mix(in srgb, var(--blue) 6%, transparent);
  }}
  .trow {{
    display: flex; align-items: center; gap: 2px;
    min-width: 0;
  }}
  .tchev {{
    width: 18px; height: 18px; flex-shrink: 0;
    display: inline-flex; align-items: center; justify-content: center;
    background: none; border: none; padding: 0; margin: 0;
    color: var(--text3); font-size: 9px; line-height: 1;
    cursor: pointer; border-radius: 4px;
    transition: transform .15s, color .12s, background .12s;
  }}
  .tchev:hover {{ color: var(--text); background: var(--hover); }}
  .tchev.empty {{ visibility: hidden; cursor: default; }}
  .tnode.open > .trow .tchev {{ transform: rotate(90deg); }}
  .tnode.current > .trow .tchev {{ transform: rotate(100deg); color: var(--blue); }}
  .tnode.open.current > .trow .tchev {{ transform: rotate(100deg); color: var(--blue); }}
  .tlink {{
    display: flex; align-items: center; gap: 8px;
    padding: 7px 8px; border-radius: 8px;
    color: var(--text2); text-decoration: none;
    font-size: 13.5px; font-weight: 500;
    transition: all .15s;
    white-space: nowrap; overflow: hidden;
    flex: 1; min-width: 0;
  }}
  .tlink:hover {{ background: var(--hover); color: var(--text); }}
  .tlink.active {{
    background: var(--sel); color: #7db4ff; font-weight: 600;
  }}
  .tfolder {{
    width: 16px; height: 12px; flex-shrink: 0;
    background: linear-gradient(180deg, #fbbf24, #f59e0b);
    border-radius: 2px 3px 3px 3px;
    position: relative;
  }}
  .tfolder::before {{
    content: ""; position: absolute; top: -3px; left: 0;
    width: 7px; height: 4px; background: #fbbf24;
    border-radius: 2px 2px 0 0;
  }}
  .tfolder.root {{ background: linear-gradient(180deg, #94a3b8, #64748b); }}
  .tfolder.root::before {{ background: #94a3b8; }}
  .tname {{ overflow: hidden; text-overflow: ellipsis; }}

  /* ---- center file list ---- */
  .center {{
    display: flex; flex-direction: column;
    border-right: 1px solid var(--border);
    min-width: 0;
    min-height: 0;
    overflow: hidden;
  }}
  .list-tools {{
    display: flex; align-items: center; justify-content: flex-end;
    gap: 10px; padding: 12px 16px;
    border-bottom: 1px solid var(--border);
    flex-shrink: 0;
  }}
  .zoom-tools {{
    display: flex; align-items: center; background: var(--panel2);
    border: 1px solid var(--border); border-radius: 9px;
    overflow: hidden;
  }}
  .zoom-tools button {{
    width: 32px; height: 32px; border: none; background: transparent;
    color: var(--text3); cursor: pointer; font-size: 15px; font-weight: 700;
    display: flex; align-items: center; justify-content: center;
    transition: all .15s;
  }}
  .zoom-tools button:hover {{ background: var(--panel3); color: var(--blue); }}
  .zoom-tools .zval {{
    min-width: 44px; text-align: center; font-size: 11px; font-weight: 700;
    color: var(--text2); user-select: none;
  }}
  .view-toggle {{
    display: flex; background: var(--panel2);
    border: 1px solid var(--border); border-radius: 9px;
    overflow: hidden;
  }}
  .view-toggle button {{
    width: 36px; height: 32px; border: none; background: transparent;
    color: var(--text3); cursor: pointer; font-size: 14px;
    display: flex; align-items: center; justify-content: center;
    transition: all .15s;
  }}
  .view-toggle button.on {{
    background: var(--panel3); color: var(--blue);
  }}
  .sort-wrap {{ position: relative; }}
  .sort-btn {{
    display: flex; align-items: center; gap: 6px;
    padding: 7px 14px; border-radius: 9px;
    background: var(--panel2); border: 1px solid var(--border);
    color: var(--text2); font-size: 13px; font-weight: 600;
    cursor: pointer; transition: all .15s;
  }}
  .sort-btn:hover {{ border-color: var(--blue); color: var(--text); }}
  .sort-menu {{
    position: absolute; right: 0; top: calc(100% + 6px);
    background: var(--panel2); border: 1px solid var(--border2);
    border-radius: 10px; min-width: 150px; z-index: 50;
    box-shadow: var(--shadow); display: none; overflow: hidden;
  }}
  .sort-menu.open {{ display: block; }}
  .sort-menu button {{
    display: block; width: 100%; text-align: left;
    padding: 10px 14px; border: none; background: transparent;
    color: var(--text2); font-size: 13px; cursor: pointer;
    transition: background .12s;
  }}
  .sort-menu button:hover {{ background: var(--hover); color: var(--text); }}
  .sort-menu button.on {{ color: var(--blue); font-weight: 600; }}

  .thead {{
    display: grid;
    grid-template-columns: 28px 1fr 90px 150px 40px;
    gap: 8px; padding: calc(10px * var(--list-zoom, 1)) 16px;
    border-bottom: 1px solid var(--border);
    font-size: calc(11.5px * var(--list-zoom, 1)); font-weight: 700; letter-spacing: .04em;
    color: var(--text3); text-transform: uppercase;
    flex-shrink: 0;
    align-items: center;
  }}
  /* grid view: hide the Name/Size/Last Modified header — info lives on the cards */
  .center.grid-mode .thead {{ display: none; }}
  /* Independent scroll container for the file list.
     Wheel over this region scrolls only the list (native nested overflow);
     wheel outside scrolls the page as usual. */
  .tbody {{
    flex: 1;
    min-height: 0;
    overflow-y: auto;
    padding: 6px 8px;
    overscroll-behavior: contain;
  }}

  .frow {{
    display: grid;
    grid-template-columns: 28px 1fr 90px 150px 40px;
    gap: 8px; align-items: center;
    padding: calc(10px * var(--list-zoom, 1)) 10px; border-radius: 11px;
    cursor: pointer; border: 1.5px solid transparent;
    transition: background .15s, border-color .15s, transform .15s;
    animation: rowIn .35s cubic-bezier(.16,1,.3,1) both;
  }}
  @keyframes rowIn {{
    from {{ opacity: 0; transform: translateX(-8px); }}
    to {{ opacity: 1; transform: translateX(0); }}
  }}
  .frow:hover {{ background: var(--hover); }}
  .frow.selected {{
    background: var(--sel);
    border-color: var(--sel-border);
    box-shadow: 0 0 0 1px var(--sel-border), 0 4px 16px rgba(var(--accent-rgb), .15);
  }}
  .fchk {{
    display: flex; align-items: center; justify-content: center;
    position: relative; z-index: 1;
  }}
  .fchk .chk {{
    appearance: none; -webkit-appearance: none;
    width: 17px; height: 17px; margin: 0;
    border: 1.5px solid var(--border2);
    border-radius: 5px;
    background: var(--panel2);
    cursor: pointer;
    flex-shrink: 0;
    position: relative;
    transition: background .12s, border-color .12s, box-shadow .12s, transform .1s;
    box-shadow: 0 1px 2px rgba(0,0,0,.15);
  }}
  .fchk .chk:hover {{
    border-color: var(--blue);
    box-shadow: 0 0 0 3px rgba(var(--accent-rgb),.18);
  }}
  .fchk .chk:checked {{
    background: linear-gradient(135deg, var(--blue), var(--blue2));
    border-color: transparent;
    box-shadow: 0 2px 8px rgba(var(--accent-rgb),.4);
  }}
  .fchk .chk:checked::after {{
    content: "";
    position: absolute;
    left: 4.5px; top: 1px;
    width: 5px; height: 9px;
    border: solid #fff;
    border-width: 0 2px 2px 0;
    transform: rotate(45deg);
  }}
  .fchk .chk:active {{ transform: scale(.92); }}
  .frow.selected .fchk .chk {{
    border-color: var(--blue);
  }}
  .parent-row .fchk {{ visibility: hidden; }}
  .thead .fchk input {{
    appearance: none; -webkit-appearance: none;
    width: 17px; height: 17px; margin: 0;
    border: 1.5px solid var(--border2);
    border-radius: 5px;
    background: var(--panel2);
    cursor: pointer;
    position: relative;
    transition: background .12s, border-color .12s, box-shadow .12s;
    box-shadow: 0 1px 2px rgba(0,0,0,.15);
  }}
  .thead .fchk input:hover {{
    border-color: var(--blue);
    box-shadow: 0 0 0 3px rgba(var(--accent-rgb),.18);
  }}
  .thead .fchk input:checked {{
    background: linear-gradient(135deg, var(--blue), var(--blue2));
    border-color: transparent;
    box-shadow: 0 2px 8px rgba(var(--accent-rgb),.4);
  }}
  .thead .fchk input:checked::after {{
    content: "";
    position: absolute;
    left: 4.5px; top: 1px;
    width: 5px; height: 9px;
    border: solid #fff;
    border-width: 0 2px 2px 0;
    transform: rotate(45deg);
  }}
  .frow .fname {{
    display: flex; align-items: center; gap: 12px; min-width: 0;
  }}
  .ftext {{ min-width: 0; display: flex; flex-direction: column; gap: 2px; }}
  .flabel {{
    font-size: calc(14px * var(--list-zoom, 1)); font-weight: 600;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }}
  .fsub {{
    font-size: calc(11.5px * var(--list-zoom, 1)); color: var(--text3);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }}
  .fsize, .fmtime {{
    font-size: calc(12.5px * var(--list-zoom, 1)); color: var(--text2);
    font-variant-numeric: tabular-nums;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }}
  .fdot {{ text-align: center; position: relative; }}
  .dots {{
    color: var(--text3); font-size: 18px; letter-spacing: 1px;
    opacity: 0; transition: opacity .15s, color .15s, background .15s;
    background: transparent; border: none; cursor: pointer;
    width: 32px; height: 28px; border-radius: 8px;
    display: inline-flex; align-items: center; justify-content: center;
    padding: 0; line-height: 1;
  }}
  /* Desktop: hide three dots — use right-click instead */
  @media (min-width: 981px) {{
    .dots {{ display: none !important; }}
    .frow, .thead .fchk, .tbody {{ user-select: none; -webkit-user-select: none; }}
  }}
  .frow:hover .dots, .dots.open {{ opacity: 1; }}
  .dots:hover {{ color: var(--text); background: var(--panel3); }}
  .parent-row .dots {{ display: none; }}
  .frow.drag-over {{
    outline: 2px dashed var(--blue);
    outline-offset: -2px;
    background: var(--sel);
  }}
  .frow.dragging {{ opacity: .45; }}

  /* file type badges */
  .badge {{
    width: calc(38px * var(--list-zoom, 1)); height: calc(38px * var(--list-zoom, 1));
    flex-shrink: 0;
    border-radius: 10px;
    display: flex; align-items: center; justify-content: center;
    font-size: calc(11px * var(--list-zoom, 1)); font-weight: 800; letter-spacing: -.02em;
    color: #fff;
  }}
  .badge.html {{ background: linear-gradient(135deg,#f97316,#ea580c); font-size: calc(12px * var(--list-zoom, 1)); }}
  .badge.css  {{ background: linear-gradient(135deg,#38bdf8,#0284c7); }}
  .badge.js   {{ background: linear-gradient(135deg,#facc15,#eab308); color: #1a1a1a; }}
  .badge.ts   {{ background: linear-gradient(135deg,#60a5fa,#2563eb); }}
  .badge.json {{ background: linear-gradient(135deg,#4ade80,#16a34a); font-size: 13px; }}
  .badge.md   {{ background: linear-gradient(135deg,#60a5fa,#3b82f6); }}
  .badge.txt  {{ background: linear-gradient(135deg,#f472b6,#db2777); font-size: 9px; }}
  .badge.img  {{ background: linear-gradient(135deg,#a78bfa,#7c3aed); font-size: 9px; }}
  .badge.svg  {{ background: linear-gradient(135deg,#fb923c,#f97316); font-size: 9px; }}
  .badge.ico  {{ background: linear-gradient(135deg,#fde047,#eab308); color: #854d0e; font-size: 16px; }}
  .badge.pdf  {{ background: linear-gradient(135deg,#f87171,#dc2626); }}
  .badge.zip  {{ background: linear-gradient(135deg,#c084fc,#a855f7); }}
  .badge.vid  {{ background: linear-gradient(135deg,#f472b6,#ec4899); font-size: 9px; }}
  .badge.aud  {{ background: linear-gradient(135deg,#2dd4bf,#0d9488); font-size: 9px; }}
  .badge.py   {{ background: linear-gradient(135deg,#fbbf24,#f59e0b); }}
  .badge.exe  {{ background: linear-gradient(135deg,#94a3b8,#64748b); font-size: 9px; }}
  .badge.file {{ background: linear-gradient(135deg,#64748b,#475569); font-size: 9px; }}
  .badge.folder {{
    background: linear-gradient(135deg,var(--folder-a),var(--folder-b));
    font-size: 16px;
  }}

  .empty-state {{
    text-align: center; padding: 60px 20px; color: var(--text3);
    font-size: 14px;
  }}
  .empty-icon {{ font-size: 42px; margin-bottom: 12px; opacity: .5; }}

  /* ---- right details panel (scrolls when content is tall) ---- */
  .details {{
    background: var(--details-bg);
    padding: 22px 18px;
    overflow-y: auto;
    min-height: 0;
    overscroll-behavior: contain;
    display: flex; flex-direction: column; gap: 22px;
    position: relative;
  }}
  .details > * {{ flex-shrink: 0; }}
  /* empty state: blur the panel and show a centered hint */
  .details-empty {{
    display: none;
    position: absolute; inset: 0; z-index: 3;
    flex-direction: column; align-items: center; justify-content: center;
    gap: 10px; padding: 24px 18px; text-align: center;
    font-size: 14.5px; font-weight: 600; color: var(--text2);
    background: color-mix(in srgb, var(--details-bg) 80%, transparent);
  }}
  .details-empty .de-ico {{ font-size: 34px; opacity: .55; }}
  .details.empty {{ overflow: hidden; }}
  .details.empty .details-empty {{ display: flex; }}
  .details.empty > *:not(.details-empty) {{
    filter: blur(7px); opacity: .55;
    pointer-events: none; user-select: none; -webkit-user-select: none;
  }}
  .detail-top {{
    display: flex; flex-direction: column; align-items: center;
    text-align: center; gap: 6px;
  }}
  .detail-icon {{
    width: 72px; height: 72px; border-radius: 18px;
    display: flex; align-items: center; justify-content: center;
    font-size: 26px; font-weight: 800; color: #fff;
    margin-bottom: 6px;
    box-shadow: 0 8px 24px rgba(0,0,0,.35);
  }}
  .detail-icon.html {{ background: linear-gradient(135deg,#f97316,#ea580c); }}
  .detail-icon.css  {{ background: linear-gradient(135deg,#38bdf8,#0284c7); }}
  .detail-icon.js   {{ background: linear-gradient(135deg,#facc15,#eab308); color:#1a1a1a; }}
  .detail-icon.file {{ background: linear-gradient(135deg,#64748b,#475569); }}
  .detail-icon.folder {{ background: linear-gradient(135deg,var(--folder-a),var(--folder-b)); }}
  .detail-icon.vid {{ background: linear-gradient(135deg,#f472b6,#ec4899); }}
  .detail-name {{
    font-size: 17px; font-weight: 700; word-break: break-all;
  }}
  .detail-path {{
    font-size: 12px; color: var(--text3); word-break: break-all;
  }}
  .meta-table {{
    display: flex; flex-direction: column; gap: 0;
    background: var(--panel2); border: 1px solid var(--border);
    border-radius: 12px; overflow: hidden;
  }}
  .meta-row {{
    display: flex; justify-content: space-between; align-items: center;
    padding: 11px 14px; font-size: 13px;
    border-bottom: 1px solid var(--border);
  }}
  .meta-row:last-child {{ border-bottom: none; }}
  .meta-row .mk {{ color: var(--text3); font-weight: 500; }}
  .meta-row .mv {{ color: var(--text); font-weight: 600; text-align: right; }}

  .dl-title {{
    font-size: 16px; font-weight: 700; margin-bottom: 4px;
  }}
  .dl-center {{ display: flex; flex-direction: column; gap: 12px; }}
  .dl-btn {{
    display: flex; align-items: center; gap: 12px;
    padding: 14px 14px; border-radius: 14px;
    text-decoration: none; color: #fff; cursor: pointer;
    border: none; width: 100%; text-align: left;
    font-family: inherit;
    transition: transform .2s, box-shadow .2s, filter .2s;
    position: relative; overflow: hidden;
  }}
  .dl-btn::after {{
    content: ""; position: absolute; inset: 0;
    background: linear-gradient(135deg, rgba(255,255,255,.18), transparent 50%);
    opacity: 0; transition: opacity .2s;
  }}
  .dl-btn:hover {{ transform: translateY(-2px); filter: brightness(1.08); }}
  .dl-btn:hover::after {{ opacity: 1; }}
  .dl-btn:active {{ transform: translateY(0); }}
  .dl-btn.disabled, .dl-btn:disabled {{
    opacity: .4;
    cursor: not-allowed;
    filter: grayscale(.6);
    pointer-events: none;
    transform: none !important;
    box-shadow: none !important;
  }}
  .dl-btn.blue {{
    background: linear-gradient(135deg, var(--folder-a) 0%, var(--blue) 55%, var(--blue2) 100%);
    box-shadow: 0 6px 20px rgba(var(--accent-rgb),.4);
  }}
  .dl-btn.teal {{
    background: linear-gradient(135deg, #2dd4bf 0%, #14b8a6 55%, #0d9488 100%);
    box-shadow: 0 6px 20px rgba(20,184,166,.4);
  }}
  .dl-btn.purple {{
    background: linear-gradient(135deg, #a78bfa 0%, #8b5cf6 55%, #7c3aed 100%);
    box-shadow: 0 6px 20px rgba(139,92,246,.4);
  }}
  .dl-btn.pink {{
    background: linear-gradient(135deg, #f472b6 0%, #ec4899 55%, #db2777 100%);
    box-shadow: 0 6px 20px rgba(236,72,153,.4);
  }}
  .dl-ico {{
    width: 42px; height: 42px; border-radius: 11px; flex-shrink: 0;
    background: rgba(255,255,255,.2);
    display: flex; align-items: center; justify-content: center;
    font-size: 18px;
  }}
  .dl-txt {{ flex: 1; min-width: 0; }}
  .dl-txt .t {{ font-size: 14px; font-weight: 700; }}
  .dl-txt .s {{
    font-size: 11.5px; opacity: .88; margin-top: 2px;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }}
  .dl-arrow {{
    width: 32px; height: 32px; border-radius: 50%; flex-shrink: 0;
    background: rgba(255,255,255,.22);
    display: flex; align-items: center; justify-content: center;
    font-size: 14px; font-weight: 700;
  }}

  /* ---- grid view ---- */
  .tbody.grid-view {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(calc(140px * var(--list-zoom, 1)), 1fr));
    gap: calc(10px * var(--list-zoom, 1)); padding: 12px;
    align-content: start;
  }}
  .tbody.grid-view .frow {{
    display: flex; flex-direction: column; text-align: center;
    gap: calc(8px * var(--list-zoom, 1)); padding: calc(18px * var(--list-zoom, 1)) 8px calc(14px * var(--list-zoom, 1));
    grid-template-columns: none;
    min-height: calc(120px * var(--list-zoom, 1));
    justify-content: center;
    position: relative;
  }}
  .tbody.grid-view .fcell.fsize {{ display: none; }}
  .tbody.grid-view .fcell.fmtime {{ display: none; }}
  @media (min-width: 981px) {{
    .tbody.grid-view .fcell.fmtime {{
      display: block; width: 100%; text-align: center;
      font-size: calc(11px * var(--list-zoom, 1)); color: var(--text3);
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    }}
  }}
  .tbody.grid-view .fcell.fchk {{
    position: absolute; top: 6px; left: 6px;
    width: auto; z-index: 2;
  }}
  .tbody.grid-view .fcell.fdot {{
    display: block;
    position: absolute; top: 6px; right: 6px;
    width: auto; text-align: center;
  }}
  .tbody.grid-view .dots {{ opacity: 1; width: 28px; height: 28px; font-size: 16px; }}
  .tbody.grid-view .fname {{
    flex-direction: column; gap: calc(8px * var(--list-zoom, 1)); width: 100%;
  }}
  .tbody.grid-view .badge {{
    width: calc(52px * var(--list-zoom, 1)); height: calc(52px * var(--list-zoom, 1));
    border-radius: calc(14px * var(--list-zoom, 1));
    font-size: calc(14px * var(--list-zoom, 1)); margin: 0 auto;
  }}
  .tbody.grid-view .badge.folder {{ font-size: calc(28px * var(--list-zoom, 1)); line-height: 1; }}
  .tbody.grid-view .ftext {{ align-items: center; width: 100%; }}
  .tbody.grid-view .flabel {{
    white-space: normal; word-break: break-word; line-height: 1.3;
    max-height: 2.6em; overflow: hidden;
    display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;
    font-size: calc(14px * var(--list-zoom, 1));
  }}
  .tbody.grid-view .fsub {{ display: block; font-size: calc(10.5px * var(--list-zoom, 1)); }}
  .tbody.grid-view .empty-state {{ grid-column: 1 / -1; }}

  /* ---- toast ---- */
  .toast {{
    position: fixed; bottom: 28px; left: 50%;
    transform: translateX(-50%) translateY(80px);
    background: var(--panel3); border: 1px solid var(--border2);
    color: var(--text); padding: 12px 22px; border-radius: 12px;
    font-size: 13.5px; font-weight: 600; z-index: 999;
    box-shadow: var(--shadow); opacity: 0;
    transition: all .35s cubic-bezier(.16,1,.3,1);
    pointer-events: none;
  }}
  .toast.show {{ opacity: 1; transform: translateX(-50%) translateY(0); }}

  /* ===== ROW CONTEXT MENU ===== */
  .ctx-menu {{
    position: fixed; z-index: 300;
    min-width: 180px;
    background: var(--glass-bg);
    border: 1px solid var(--border2);
    border-radius: 14px;
    box-shadow: 0 16px 48px rgba(0,0,0,.4), inset 0 1px 0 rgba(255,255,255,.06);
    backdrop-filter: blur(24px) saturate(1.3);
    -webkit-backdrop-filter: blur(24px) saturate(1.3);
    padding: 6px;
    display: none;
    transform-origin: top right;
    animation: ctxIn .18s cubic-bezier(.16,1,.3,1) both;
  }}
  .ctx-menu.open {{ display: block; }}
  @keyframes ctxIn {{
    from {{ opacity: 0; transform: scale(.92) translateY(-6px); }}
    to {{ opacity: 1; transform: scale(1) translateY(0); }}
  }}
  .ctx-item {{
    display: flex; align-items: center; gap: 10px;
    width: 100%; padding: 10px 12px;
    background: transparent; border: none; border-radius: 9px;
    color: var(--text); font-size: 13.5px; font-weight: 500;
    cursor: pointer; text-align: left; font-family: inherit;
    transition: background .12s;
  }}
  .ctx-item:hover {{ background: var(--hover); }}
  .ctx-item.danger {{ color: #ff6b6b; }}
  .ctx-item.danger:hover {{ background: rgba(255,80,80,.12); }}
  .ctx-item .ci {{
    width: 22px; text-align: center; font-size: 14px; opacity: .9;
  }}
  .ctx-sep {{
    height: 1px; background: var(--border);
    margin: 4px 8px;
  }}
  .ctx-title {{
    font-size: 11px; font-weight: 700; color: var(--text3);
    padding: 6px 12px 4px; letter-spacing: .04em;
    text-transform: uppercase;
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    max-width: 200px;
  }}

  /* ===== DEST PICKER (move/copy) ===== */
  .dest-tree {{
    max-height: 240px; overflow-y: auto;
    background: var(--input-bg); border: 1.5px solid var(--border);
    border-radius: 12px; padding: 8px; text-align: left;
    margin-top: 4px;
  }}
  .dest-opt {{
    display: flex; align-items: center; gap: 8px;
    width: 100%; padding: 8px 10px;
    background: transparent; border: none; border-radius: 8px;
    color: var(--text2); font-size: 13.5px; font-family: inherit;
    cursor: pointer; transition: all .12s; text-align: left;
  }}
  .dest-opt:hover {{ background: var(--hover); color: var(--text); }}
  .dest-opt.on {{
    background: var(--sel); color: #7db4ff; font-weight: 600;
    outline: 1px solid var(--sel-border);
  }}
  .dest-opt .di {{ font-size: 14px; }}
  .dest-indent {{ width: 14px; flex-shrink: 0; }}

  /* ===== SETTINGS ===== */
  .set-row {{
    display: flex; align-items: center; justify-content: space-between;
    gap: 12px; padding: 12px 14px;
    background: var(--panel2); border: 1px solid var(--border);
    border-radius: 12px; margin-bottom: 10px;
    text-align: right;
  }}
  .set-info {{ min-width: 0; }}
  .set-info .st {{ font-size: 13.5px; font-weight: 600; color: var(--text); }}
  .set-info .ss {{ font-size: 11.5px; color: var(--text3); margin-top: 2px; }}
  .set-seg {{
    display: flex; background: var(--input-bg);
    border: 1px solid var(--border); border-radius: 9px;
    overflow: hidden; flex-shrink: 0;
  }}
  .set-seg button {{
    padding: 7px 12px; border: none; background: transparent;
    color: var(--text3); font-size: 12px; font-weight: 600;
    cursor: pointer; font-family: inherit; transition: all .15s;
  }}
  .set-seg button.on {{
    background: var(--blue); color: #fff;
  }}
  /* settings modal is a bit wider so the theme tiles breathe */
  .modal.set-modal {{ max-width: 430px; }}
  /* stacked row: label on top, control full width underneath */
  .set-row.stack {{ flex-direction: column; align-items: stretch; gap: 11px; }}
  /* theme picker: uniform tiles, wrapping rows stay centred */
  .set-themes {{
    display: flex; flex-wrap: wrap; gap: 8px;
    justify-content: center; width: 100%;
  }}
  .set-themes button {{
    position: relative;
    display: flex; flex-direction: column; align-items: center;
    gap: 7px; padding: 9px 6px 8px;
    width: calc(25% - 6px); min-width: 74px;
    background: var(--input-bg); border: 1px solid var(--border);
    border-radius: 13px; color: var(--text3);
    font-size: 11.5px; font-weight: 600; font-family: inherit;
    cursor: pointer; transition: all .18s cubic-bezier(.16,1,.3,1);
  }}
  .set-themes button:hover {{
    border-color: rgba(var(--accent-rgb), .6); color: var(--text);
    transform: translateY(-2px);
    box-shadow: 0 6px 16px rgba(0,0,0,.22);
  }}
  .set-themes button.on {{
    background: rgba(var(--accent-rgb), .16);
    border-color: rgba(var(--accent-rgb), .8);
    color: var(--text);
    box-shadow: 0 0 0 1px rgba(var(--accent-rgb), .35),
                0 8px 20px rgba(var(--accent-rgb), .18);
  }}
  .set-themes .tsw {{
    width: 100%; height: 26px; border-radius: 8px;
    border: 1px solid rgba(255,255,255,.18);
  }}
  .set-themes .tnm {{
    flex: none; max-width: 100%; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; line-height: 1.2;
  }}
  .set-themes .tck {{
    position: absolute; top: 5px; inset-inline-end: 5px;
    width: 16px; height: 16px; line-height: 15px; text-align: center;
    border-radius: 50%; font-size: 10px; color: #fff;
    background: rgb(var(--accent-rgb));
    box-shadow: 0 2px 6px rgba(0,0,0,.35);
  }}
  @media (max-width: 420px) {{
    .set-themes button {{ width: calc(33.333% - 6px); }}
  }}
  .set-badge {{
    font-size: 11px; font-weight: 700; color: var(--green);
    background: rgba(34,197,94,.12); border: 1px solid rgba(34,197,94,.3);
    padding: 4px 10px; border-radius: 999px;
  }}
  .set-brand {{
    text-align: center; padding: 6px 0 14px;
    border-bottom: 1px solid var(--border); margin-bottom: 14px;
  }}
  .set-brand .bn {{
    font-size: 16px; font-weight: 800; letter-spacing: -.02em;
  }}
  .set-brand .bv {{
    font-size: 11.5px; color: var(--text3); margin-top: 3px;
  }}

  /* ===== GLASS MODALS ===== */
  .modal-back {{
    position: fixed; inset: 0; z-index: 200;
    background: rgba(5,10,25,.55);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    display: flex; align-items: center; justify-content: center;
    opacity: 0; pointer-events: none;
    transition: opacity .35s cubic-bezier(.16,1,.3,1);
    padding: 16px;
  }}
  [data-theme="light"] .modal-back {{
    background: rgba(20,30,60,.35);
  }}
  .modal-back.open {{ opacity: 1; pointer-events: auto; }}
  .modal {{
    background: var(--glass-bg);
    border: 1px solid var(--border2);
    border-radius: 22px;
    box-shadow: 0 24px 64px rgba(0,0,0,.45), inset 0 1px 0 rgba(255,255,255,.08);
    backdrop-filter: blur(28px) saturate(1.3);
    -webkit-backdrop-filter: blur(28px) saturate(1.3);
    padding: 28px 26px 24px;
    width: 100%; max-width: 380px;
    transform: translateY(24px) scale(.94);
    transition: transform .4s cubic-bezier(.34,1.35,.5,1);
    text-align: center;
  }}
  .modal-back.open .modal {{ transform: translateY(0) scale(1); }}
  .modal h2 {{
    font-size: 18px; font-weight: 700; margin-bottom: 6px;
    color: var(--text);
  }}
  .modal .msub {{
    font-size: 12.5px; color: var(--text2); margin-bottom: 20px;
  }}
  /* upload progress */
  .up-pct {{
    font-size: 34px; font-weight: 800; color: var(--blue);
    letter-spacing: -.02em; margin: 4px 0 2px; font-variant-numeric: tabular-nums;
  }}
  .up-bar {{
    height: 10px; background: var(--panel2); border: 1px solid var(--border);
    border-radius: 999px; overflow: hidden; margin: 14px 0 8px;
  }}
  .up-bar > i {{
    display: block; height: 100%; width: 0%;
    background: linear-gradient(90deg, var(--blue), var(--blue2));
    border-radius: 999px; transition: width .15s linear;
  }}
  .up-meta {{
    font-size: 12.5px; color: var(--text2); word-break: break-all;
    min-height: 1.3em; margin-bottom: 4px;
  }}
  .up-modal {{
    text-align: right;
    max-height: calc(100dvh - 32px); overflow-y: auto;
  }}
  .up-modal h2, .up-modal .msub {{ text-align: center; }}
  .up-modal .up-pct {{ text-align: center; }}
  .up-done {{
    color: var(--green); font-weight: 700; font-size: 14px; margin-top: 6px;
  }}
  /* per-file rows inside the upload modal */
  .up-list {{
    display: flex; flex-direction: column; gap: 9px;
    margin: 14px 0 4px; max-height: min(46vh, 340px); overflow-y: auto;
    text-align: right; padding-inline-end: 2px;
  }}
  .up-item {{
    background: var(--panel2); border: 1px solid var(--border);
    border-radius: 13px; padding: 9px 11px;
    transition: border-color .2s, background .2s;
  }}
  .up-item.done {{ border-color: rgba(34,197,94,.4); }}
  .up-item.paused {{ border-color: rgba(245,158,11,.45); }}
  .up-item.err {{ border-color: rgba(239,68,68,.45); }}
  .up-top {{ display: flex; align-items: center; gap: 9px; }}
  .up-ico {{
    width: 28px; height: 28px; border-radius: 9px; flex-shrink: 0;
    background: var(--panel3); border: 1px solid var(--border);
    display: flex; align-items: center; justify-content: center; font-size: 13px;
  }}
  .up-name {{
    flex: 1; min-width: 0; font-size: 12.5px; font-weight: 600; color: var(--text);
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap; direction: ltr;
    text-align: left;
  }}
  .up-pcti {{
    font-size: 11.5px; font-weight: 700; color: var(--text2);
    font-variant-numeric: tabular-nums; white-space: nowrap;
  }}
  .up-item.done .up-pcti {{ color: var(--green); }}
  .up-item.err .up-pcti {{ color: var(--red); }}
  .up-item.paused .up-pcti {{ color: var(--amber, #f59e0b); }}
  .up-bottom {{ display: flex; align-items: center; gap: 8px; margin-top: 8px; }}
  .up-track {{
    flex: 1; height: 6px; background: var(--panel); border: 1px solid var(--border);
    border-radius: 999px; overflow: hidden;
  }}
  .up-track > i {{
    display: block; height: 100%; width: 0%;
    background: linear-gradient(90deg, var(--blue), var(--blue2));
    border-radius: 999px; transition: width .15s linear;
  }}
  .up-item.done .up-track > i {{ background: linear-gradient(90deg, #16a34a, var(--green)); }}
  .up-item.err .up-track > i {{ background: linear-gradient(90deg, #b91c1c, var(--red)); }}
  .up-item.paused .up-track > i {{ background: linear-gradient(90deg, #b45309, #f59e0b); }}
  .upb {{
    flex-shrink: 0; padding: 5px 10px; border-radius: 8px;
    font-size: 11.5px; font-weight: 700; font-family: inherit; cursor: pointer;
    border: 1px solid var(--border2); background: var(--panel);
    color: var(--text2); transition: all .15s; line-height: 1.4;
  }}
  .upb:hover {{ border-color: var(--blue); color: var(--text); }}
  .upb.stop {{ color: var(--amber, #f59e0b); }}
  .upb.go {{ color: var(--green); }}
  .upb.del {{ color: var(--red); }}
  .upb[disabled] {{ opacity: .4; cursor: default; }}
  .up-size {{
    font-size: 10.5px; color: var(--text3); white-space: nowrap;
    font-variant-numeric: tabular-nums;
  }}
  .choice-grid {{
    display: grid; grid-template-columns: 1fr 1fr; gap: 12px;
  }}
  .choice {{
    display: flex; flex-direction: column; align-items: center; gap: 10px;
    padding: 22px 12px 18px;
    background: var(--panel2); border: 1.5px solid var(--border);
    border-radius: 16px; cursor: pointer;
    color: var(--text); font-family: inherit;
    transition: all .25s cubic-bezier(.16,1,.3,1);
  }}
  .choice:hover {{
    border-color: var(--blue);
    background: var(--sel);
    transform: translateY(-4px);
    box-shadow: 0 10px 28px rgba(var(--accent-rgb),.2);
  }}
  .choice:active {{ transform: translateY(-1px); }}
  .choice .cico {{
    width: 52px; height: 52px; border-radius: 15px;
    display: flex; align-items: center; justify-content: center;
    font-size: 24px; color: #fff;
    box-shadow: 0 6px 18px rgba(0,0,0,.25);
  }}
  .choice .cico.folder {{ background: linear-gradient(135deg,var(--folder-a),var(--folder-b)); }}
  .choice .cico.file {{ background: linear-gradient(135deg,#a78bfa,#7c3aed); }}
  .choice .clabel {{ font-size: 14px; font-weight: 700; }}
  .choice .cdesc {{ font-size: 11px; color: var(--text3); }}

  .field-label {{
    display: block; text-align: right;
    font-size: 12.5px; font-weight: 600; color: var(--text2);
    margin-bottom: 8px;
  }}
  .field-input {{
    width: 100%; padding: 13px 16px;
    background: var(--input-bg); border: 1.5px solid var(--border);
    border-radius: 12px; color: var(--text); font-size: 15px;
    outline: none; text-align: left;
    transition: border-color .2s, box-shadow .2s;
    font-family: inherit;
  }}
  .field-input:focus {{
    border-color: var(--blue);
    box-shadow: 0 0 0 3px rgba(var(--accent-rgb),.2);
  }}
  .field-hint {{
    font-size: 11.5px; color: var(--text3); margin-top: 8px;
    text-align: right; min-height: 16px;
  }}
  .modal-actions {{
    display: flex; gap: 10px; margin-top: 20px;
  }}
  .modal-actions .btn {{ flex: 1; justify-content: center; padding: 11px 14px; }}
  .btn.ghost {{ background: transparent; }}
  .btn.ok {{
    background: linear-gradient(135deg, var(--blue), var(--blue2));
    border: none; color: #fff;
    box-shadow: 0 4px 16px rgba(var(--accent-rgb),.35);
  }}
  .btn.ok:hover {{ transform: translateY(-1px); box-shadow: 0 6px 20px rgba(var(--accent-rgb),.5); }}
  .btn[disabled] {{ opacity: .5; pointer-events: none; }}
  .spinner {{
    display: inline-block; width: 14px; height: 14px;
    border: 2px solid rgba(255,255,255,.35);
    border-top-color: #fff; border-radius: 50%;
    animation: spin .7s linear infinite; vertical-align: -2px;
    margin-left: 6px;
  }}
  @keyframes spin {{ to {{ transform: rotate(360deg); }} }}

  /* ---- responsive ---- */
  @media (max-width: 980px) {{
    body {{
      height: auto;
      min-height: 100vh;
      overflow: auto;
    }}
    .app {{ flex: none; }}
    .layout {{ grid-template-columns: 1fr; grid-template-rows: none; overflow: visible; }}
    .sidebar {{ display: none; }}
    .sidebar-resize {{ display: none; }}
    .center {{
      border-right: none;
      height: calc(100dvh - 110px);
      max-height: calc(100dvh - 110px);
      min-height: 360px;
      overflow: hidden;
    }}
    .list-tools {{ padding: 8px 12px; }}
    .zoom-tools {{ display: none; }}
    .thead {{ padding: 7px 12px; }}
    .frow {{ padding: 6px 8px; }}
    .badge {{ width: 30px; height: 30px; font-size: 10px; }}
    .details {{
      border-top: 1px solid var(--border);
      overflow: visible;
      overscroll-behavior: auto;
      max-height: none;
      min-height: 0;
    }}
    .metrics {{ display: none; }}
    .thead, .frow {{ grid-template-columns: 28px 1fr 80px 40px; }}
    .thead .h-mtime, .frow .fmtime {{ display: none; }}
    .dots {{ opacity: 1; }}
    .tbody.grid-view .dots {{ opacity: 1; }}
  }}
  @media (max-width: 600px) {{
    body {{ padding: 8px; }}
    .app-head {{ flex-direction: column; gap: 10px; align-items: flex-start; }}
    .top-right .machine .mtext {{ display: none; }}
    .scope-menu button {{ padding: 9px 10px; gap: 8px; }}
    .scope-menu .sm-icon {{ width: 28px; height: 28px; font-size: 14px; }}
    .scope-menu .sm-desc {{ display: none; }}
    .scope-menu .sm-title {{ font-size: 12.5px; }}
    .center {{
      height: calc(100dvh - 90px);
      max-height: calc(100dvh - 90px);
      min-height: 320px;
    }}
    .thead, .frow {{ grid-template-columns: 28px 1fr 40px; }}
    .thead .h-size, .frow .fsize {{ display: none; }}
  }}
</style>
</head>
<body>

<!-- top bar -->
<div class="topbar">
  <div class="search">
    <svg viewBox="0 0 24 24" stroke-width="2" stroke-linecap="round">
      <circle cx="11" cy="11" r="7"/><path d="M21 21l-4.3-4.3"/>
    </svg>
    <input type="text" id="searchInput" placeholder="Search files, folders, or commands..."
           oninput="onSearchInput()">
    <div class="search-scope" id="searchScope" title="Search scope">
      <button type="button" class="scope-dots" id="scopeDots" aria-label="Search scope menu"
              onclick="toggleScopeMenu(event)">&#8942;</button>
    </div>
    <div class="scope-menu" id="scopeMenu" role="menu">
      <button type="button" class="on" data-scope="local" role="menuitem"
              onclick="setSearchScope('local')">
        <span class="sm-icon">&#128193;</span>
        <span class="sm-text">
          <span class="sm-title">سرچ در این پوشه</span>
          <span class="sm-desc">فقط مسیر فعلی را جستجو کن</span>
        </span>
        <span class="sm-check">&#10003;</span>
      </button>
      <button type="button" data-scope="server" role="menuitem"
              onclick="setSearchScope('server')">
        <span class="sm-icon">&#127760;</span>
        <span class="sm-text">
          <span class="sm-title">سرچ در کل سرور</span>
          <span class="sm-desc">همه پوشه‌ها و فایل‌ها</span>
        </span>
        <span class="sm-check">&#10003;</span>
      </button>
    </div>
  </div>
  <div class="top-right">
    <div class="theme-wrap" id="themeWrap">
      <button class="theme-btn" id="themeSwitch" title="Themes"
              onclick="toggleTheme(event)" aria-label="Themes">
        <span class="ti">&#127912;</span>
        <span class="tcaret">&#9662;</span>
      </button>
      <div class="theme-menu" id="themeMenu">
        <button data-t="dark" onclick="setTheme('dark')">
          <span class="tsw" style="background:linear-gradient(135deg,#070b14,#3b82f6)"></span>
          <span class="tnm">تیره</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="light" onclick="setTheme('light')">
          <span class="tsw" style="background:linear-gradient(135deg,#ffffff,#93c5fd)"></span>
          <span class="tnm">روشن</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="blue" onclick="setTheme('blue')">
          <span class="tsw" style="background:linear-gradient(135deg,#061224,#38a6ff)"></span>
          <span class="tnm">آبی</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="purple" onclick="setTheme('purple')">
          <span class="tsw" style="background:linear-gradient(135deg,#12071f,#c026d3)"></span>
          <span class="tnm">بنفش</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="orange" onclick="setTheme('orange')">
          <span class="tsw" style="background:linear-gradient(135deg,#130c03,#f59e0b)"></span>
          <span class="tnm">نارنجی</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="green" onclick="setTheme('green')">
          <span class="tsw" style="background:linear-gradient(135deg,#04120b,#22c55e)"></span>
          <span class="tnm">سبز</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="sunset" onclick="setTheme('sunset')">
          <span class="tsw" style="background:linear-gradient(135deg,#150a26,#f59e0b)"></span>
          <span class="tnm">غروب</span><span class="tck">&#10003;</span>
        </button>
      </div>
    </div>
    <div class="machine">
      <span class="mdot"></span>
      <div class="mtext">
        <div class="mtitle">Local Machine</div>
        <div class="mstatus">Online</div>
      </div>
    </div>
    <div class="metrics">
      <div class="metric cpu"><div class="mlabel">CPU</div><div class="mbar"><i></i></div></div>
      <div class="metric ram"><div class="mlabel">RAM</div><div class="mbar"><i></i></div></div>
      <div class="metric disk"><div class="mlabel">DISK</div><div class="mbar"><i></i></div></div>
    </div>
    <button class="gear" type="button" id="gearBtn" title="Settings"
            onclick="openSettings()" aria-label="Settings">&#9881;</button>
  </div>
</div>

<!-- main app -->
<div class="app">
  <div class="app-head">
    <div>
      <h1>File Manager</h1>
      <div class="sub">Browse and manage your server files</div>
    </div>
    <div class="head-actions">
      <button class="btn" onclick="openNewModal()">
        &#10010; جدید
      </button>
      <button class="btn primary" onclick="document.getElementById('fileInput').click()">
        &#8682; Upload
      </button>
      <input type="file" id="fileInput" multiple hidden onchange="uploadFiles(this.files)">
    </div>
  </div>

  <div class="layout">
    <!-- sidebar -->
    <aside class="sidebar" id="sidebar">
      {tree_html}
    </aside>
    <div class="sidebar-resize" id="sidebarResize" title="Drag to resize"></div>

    <!-- center list -->
    <section class="center">
      <div class="list-tools">
        <div class="zoom-tools" title="List size (Ctrl+scroll also works)">
          <button type="button" id="zoomOutBtn" onclick="zoomList(-1)" aria-label="Smaller list">&minus;</button>
          <span class="zval" id="zoomVal">100%</span>
          <button type="button" id="zoomInBtn" onclick="zoomList(1)" aria-label="Larger list">+</button>
        </div>
        <div class="view-toggle">
          <button id="btnGrid" title="Grid view" onclick="setView('grid')">&#9638;</button>
          <button id="btnList" class="on" title="List view" onclick="setView('list')">&#9776;</button>
        </div>
        <div class="sort-wrap">
          <button class="sort-btn" onclick="toggleSort(event)">Sort &#9662;</button>
          <div class="sort-menu" id="sortMenu">
            <button data-k="name" class="on" onclick="sortRows('name',this)">Name</button>
            <button data-k="size" onclick="sortRows('size',this)">Size</button>
            <button data-k="date" onclick="sortRows('date',this)">Last Modified</button>
          </div>
        </div>
      </div>
      <div class="thead">
        <div class="fchk"><input type="checkbox" id="chkAll" onchange="toggleAllChecks(this)" title="Select all"></div>
        <div>Name</div>
        <div class="h-size">Size</div>
        <div class="h-mtime">Last Modified</div>
        <div></div>
      </div>
      <div class="tbody" id="tbody">
{body_rows}
      </div>
    </section>

    <!-- right details -->
    <aside class="details">
      <div class="details-empty" id="detailsEmpty">
        <div class="de-ico">&#128196;</div>
        <div class="de-txt" dir="rtl">فایلی را انتخاب کنید</div>
      </div>
      <div class="detail-top">
        <div class="detail-icon file" id="dIcon">&#128196;</div>
        <div class="detail-name" id="dName">No file selected</div>
        <div class="detail-path" id="dPath">{html.escape(display_path)}</div>
      </div>

      <div class="meta-table">
        <div class="meta-row"><span class="mk">File size</span><span class="mv" id="dSize">&mdash;</span></div>
        <div class="meta-row"><span class="mk">File type</span><span class="mv" id="dType">&mdash;</span></div>
        <div class="meta-row"><span class="mk">Modified</span><span class="mv" id="dMod">&mdash;</span></div>
        <div class="meta-row"><span class="mk">Permissions</span><span class="mv" id="dPerm">&mdash;</span></div>
      </div>

      <div>
        <div class="dl-title">Download Center</div>
        <div class="dl-center" style="margin-top:12px">
          <button class="dl-btn blue" id="btnDownload" onclick="actDownload()">
            <span class="dl-ico">&#8681;</span>
            <span class="dl-txt">
              <span class="t">Download File</span>
              <span class="s" id="dlSub1">Select a file</span>
            </span>
            <span class="dl-arrow">&#8250;</span>
          </button>
          <button class="dl-btn teal" id="btnZip" onclick="actZip()">
            <span class="dl-ico">&#128230;</span>
            <span class="dl-txt">
              <span class="t">Download as ZIP</span>
              <span class="s" id="dlSub2">Compress and download</span>
            </span>
            <span class="dl-arrow">&#8250;</span>
          </button>
          <button class="dl-btn purple" onclick="actShare()">
            <span class="dl-ico">&#128279;</span>
            <span class="dl-txt">
              <span class="t">Create Share Link</span>
              <span class="s">Generate temporary download URL</span>
            </span>
            <span class="dl-arrow">&#8250;</span>
          </button>
          <button class="dl-btn pink" onclick="actCopy()">
            <span class="dl-ico">&#10697;</span>
            <span class="dl-txt">
              <span class="t">Copy Direct Link</span>
              <span class="s">Copy file URL to clipboard</span>
            </span>
            <span class="dl-arrow">&#8250;</span>
          </button>
        </div>
      </div>
    </aside>
  </div>
</div>

<div class="toast" id="toast"></div>

<!-- row context menu -->
<div class="ctx-menu" id="ctxMenu" role="menu">
  <div class="ctx-title" id="ctxTitle"></div>
  <button class="ctx-item" onclick="ctxAction('rename')"><span class="ci">&#9998;</span> تغییر نام</button>
  <button class="ctx-item" onclick="ctxAction('move')"><span class="ci">&#8644;</span> انتقال</button>
  <button class="ctx-item" onclick="ctxAction('copy')"><span class="ci">&#10697;</span> کپی</button>
  <div class="ctx-sep"></div>
  <button class="ctx-item danger" onclick="ctxAction('delete')"><span class="ci">&#128465;</span> حذف</button>
</div>

<!-- glass modal: new folder / new file choice -->
<div class="modal-back" id="newModal" onclick="if(event.target===this)closeNewModal()">
  <div class="modal" role="dialog" aria-modal="true">
    <h2>ایجاد مورد جدید</h2>
    <div class="msub">در مسیر فعلی ساخته می‌شود: <strong dir="ltr">{html.escape(display_path)}</strong></div>
    <div class="choice-grid">
      <button class="choice" onclick="openNameModal('folder')">
        <span class="cico folder">&#128193;</span>
        <span class="clabel">پوشه جدید</span>
        <span class="cdesc">New folder</span>
      </button>
      <button class="choice" onclick="openNameModal('file')">
        <span class="cico file">&#128196;</span>
        <span class="clabel">فایل جدید</span>
        <span class="cdesc">New file</span>
      </button>
    </div>
    <div class="modal-actions">
      <button class="btn ghost" onclick="closeNewModal()">انصراف</button>
    </div>
  </div>
</div>

<!-- glass modal: name prompt -->
<div class="modal-back" id="nameModal" onclick="if(event.target===this)closeNameModal()">
  <div class="modal" role="dialog" aria-modal="true">
    <h2 id="nameTitle">نام را وارد کنید</h2>
    <div class="msub" id="nameSub"></div>
    <label class="field-label" for="nameInput" id="nameLabel">نام</label>
    <input class="field-input" id="nameInput" type="text" autocomplete="off"
           oninput="nameInputChanged()" onkeydown="if(event.key==='Enter')confirmName()">
    <div class="field-hint" id="nameHint"></div>
    <div class="modal-actions">
      <button class="btn ghost" onclick="closeNameModal()">انصراف</button>
      <button class="btn ok" id="nameOk" onclick="confirmName()">تایید</button>
    </div>
  </div>
</div>

<!-- glass modal: delete confirm -->
<div class="modal-back" id="delModal" onclick="if(event.target===this)closeDelModal()">
  <div class="modal" role="dialog" aria-modal="true">
    <h2>حذف مورد</h2>
    <div class="msub">این عملیات برگشت‌پذیر نیست و از سرور هم پاک می‌شود.</div>
    <div class="set-row" style="margin-top:4px">
      <div class="set-info">
        <div class="st" id="delName">&mdash;</div>
        <div class="ss" id="delKind">&mdash;</div>
      </div>
    </div>
    <div class="modal-actions">
      <button class="btn ghost" onclick="closeDelModal()">انصراف</button>
      <button class="btn ok" id="delOk" style="background:linear-gradient(135deg,#ff6b6b,#e11d48);box-shadow:0 4px 16px rgba(225,29,72,.35)"
              onclick="confirmDelete()">حذف</button>
    </div>
  </div>
</div>

<!-- glass modal: rename -->
<div class="modal-back" id="renModal" onclick="if(event.target===this)closeRenModal()">
  <div class="modal" role="dialog" aria-modal="true">
    <h2>تغییر نام</h2>
    <div class="msub" id="renSub"></div>
    <label class="field-label" for="renInput">نام جدید</label>
    <input class="field-input" id="renInput" type="text" autocomplete="off"
           onkeydown="if(event.key==='Enter')confirmRename()">
    <div class="field-hint" id="renHint"></div>
    <div class="modal-actions">
      <button class="btn ghost" onclick="closeRenModal()">انصراف</button>
      <button class="btn ok" id="renOk" onclick="confirmRename()">تایید</button>
    </div>
  </div>
</div>

<!-- glass modal: move / copy destination -->
<div class="modal-back" id="destModal" onclick="if(event.target===this)closeDestModal()">
  <div class="modal" role="dialog" aria-modal="true">
    <h2 id="destTitle">انتخاب مقصد</h2>
    <div class="msub" id="destSub"></div>
    <div class="dest-tree" id="destTree"></div>
    <div class="field-hint" id="destHint">مقصد: /</div>
    <div class="modal-actions">
      <button class="btn ghost" onclick="closeDestModal()">انصراف</button>
      <button class="btn ok" id="destOk" onclick="confirmDest()">تایید</button>
    </div>
  </div>
</div>

<!-- glass modal: upload progress -->
<div class="modal-back" id="upModal" onclick="if(event.target===this)cancelUpload()">
  <div class="modal up-modal" role="dialog" aria-modal="true">
    <h2 id="upTitle">آپلود فایل</h2>
    <div class="msub" id="upSub">در حال انتقال به سرور…</div>
    <div class="up-pct" id="upPct">0%</div>
    <div class="up-bar"><i id="upBar"></i></div>
    <div class="up-meta" id="upMeta"></div>
    <div class="up-list" id="upList"></div>
    <div class="up-done" id="upDone" style="display:none">✓ آپلود شد</div>
    <div class="modal-actions" id="upActions">
      <button class="btn ghost" id="upCancelBtn" onclick="cancelUpload()">لغو همه</button>
      <button class="btn ok" id="upCloseBtn" onclick="closeUploadModal()" style="display:none">بستن</button>
    </div>
  </div>
</div>

<!-- glass modal: settings -->
<div class="modal-back" id="setModal" onclick="if(event.target===this)closeSettings()">
  <div class="modal set-modal" role="dialog" aria-modal="true">
    <div class="set-brand">
      <div class="bn">&#9679; Black Server My System</div>
      <div class="bv">File Manager &middot; Settings</div>
    </div>
    <div class="set-row stack">
      <div class="set-info">
        <div class="st">تم رابط کاربری</div>
        <div class="ss">هر تمی را انتخاب کنید — با انیمیشن نرم</div>
      </div>
      <div class="set-themes">
        <button id="setDark" data-t="dark" onclick="setTheme('dark')">
          <span class="tsw" style="background:linear-gradient(135deg,#070b14,#3b82f6)"></span>
          <span class="tnm">تیره</span><span class="tck">&#10003;</span>
        </button>
        <button id="setLight" data-t="light" onclick="setTheme('light')">
          <span class="tsw" style="background:linear-gradient(135deg,#ffffff,#93c5fd)"></span>
          <span class="tnm">روشن</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="blue" onclick="setTheme('blue')">
          <span class="tsw" style="background:linear-gradient(135deg,#061224,#38a6ff)"></span>
          <span class="tnm">آبی</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="purple" onclick="setTheme('purple')">
          <span class="tsw" style="background:linear-gradient(135deg,#12071f,#c026d3)"></span>
          <span class="tnm">بنفش</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="orange" onclick="setTheme('orange')">
          <span class="tsw" style="background:linear-gradient(135deg,#130c03,#f59e0b)"></span>
          <span class="tnm">نارنجی</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="green" onclick="setTheme('green')">
          <span class="tsw" style="background:linear-gradient(135deg,#04120b,#22c55e)"></span>
          <span class="tnm">سبز</span><span class="tck">&#10003;</span>
        </button>
        <button data-t="sunset" onclick="setTheme('sunset')">
          <span class="tsw" style="background:linear-gradient(135deg,#150a26,#f59e0b)"></span>
          <span class="tnm">غروب</span><span class="tck">&#10003;</span>
        </button>
      </div>
    </div>
    <div class="set-row">
      <div class="set-info">
        <div class="st">نمای پیش‌فرض</div>
        <div class="ss">حالت نمایش فایل‌ها هنگام باز شدن</div>
      </div>
      <div class="set-seg">
        <button id="setListV" class="on" onclick="setPrefView('list')">&#9776; لیست</button>
        <button id="setGridV" onclick="setPrefView('grid')">&#9638; گرید</button>
      </div>
    </div>
    <div class="set-row">
      <div class="set-info">
        <div class="st">وضعیت سرور</div>
        <div class="ss" id="setPath">{html.escape(display_path)}</div>
      </div>
      <span class="set-badge">Online</span>
    </div>
    <div class="set-row">
      <div class="set-info">
        <div class="st">مسیر فایل‌ها</div>
        <div class="ss">downloads/ &mdash; فقط خواندنی در لینک‌ها؛ نوشتن از طریق مدیریت</div>
      </div>
    </div>
    <div class="modal-actions">
      <button class="btn ghost" onclick="closeSettings()">بستن</button>
      <button class="btn ok" onclick="closeSettings();toast('Settings saved')">ذخیره</button>
    </div>
  </div>
</div>

<script>
var BASE = {json.dumps(display_path)};
var SELECTED = {first_file_json};
var TOTAL = {file_count};

var LIST_ZOOM = 1;
function setListZoom(z) {{
  z = Math.max(0.6, Math.min(2, Math.round(z * 100) / 100));
  LIST_ZOOM = z;
  document.documentElement.style.setProperty("--list-zoom", String(z));
  var el = document.getElementById("zoomVal");
  if (el) el.textContent = Math.round(z * 100) + "%";
  try {{ localStorage.setItem("bs-zoom", String(z)); }} catch (e) {{}}
}}
function zoomList(dir) {{
  setListZoom(LIST_ZOOM + dir * 0.1);
}}
document.addEventListener("wheel", function(e) {{
  if (!(e.ctrlKey || e.metaKey)) return;
  var tb = e.target && e.target.closest ? e.target.closest(".tbody, .center, .list-tools") : null;
  if (!tb) return;
  e.preventDefault();
  zoomList(e.deltaY < 0 ? 1 : -1);
}}, {{ passive: false }});

/* ===== SIDEBAR RESIZE (desktop, like Windows Explorer) ===== */
(function initSidebarResize() {{
  var side = document.getElementById("sidebar");
  var handle = document.getElementById("sidebarResize");
  if (!side || !handle) return;
  try {{
    var w = parseInt(localStorage.getItem("bs-sidebar-w") || "", 10);
    if (w >= 140 && w <= 520) document.documentElement.style.setProperty("--sidebar-w", w + "px");
  }} catch (e) {{}}
  var dragging = false;
  function applyW(w) {{
    w = Math.max(140, Math.min(520, Math.round(w)));
    document.documentElement.style.setProperty("--sidebar-w", w + "px");
    try {{ localStorage.setItem("bs-sidebar-w", String(w)); }} catch (e) {{}}
  }}
  handle.addEventListener("mousedown", function(e) {{
    e.preventDefault();
    dragging = true;
    handle.classList.add("active");
    document.body.style.cursor = "col-resize";
    document.body.style.userSelect = "none";
  }});
  document.addEventListener("mousemove", function(e) {{
    if (!dragging) return;
    var r = side.getBoundingClientRect();
    applyW(e.clientX - r.left);
  }});
  document.addEventListener("mouseup", function() {{
    if (!dragging) return;
    dragging = false;
    handle.classList.remove("active");
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  }});
}})();

function setDetailsEmpty(on) {{
  var d = document.querySelector(".details");
  if (!d) return;
  if (on) {{ try {{ d.scrollTop = 0; }} catch (e) {{}} }}
  d.classList.toggle("empty", !!on);
}}

(function init() {{
  if (SELECTED) applyMeta(SELECTED);
  else document.getElementById("dName").textContent = "No file selected";
  setDetailsEmpty(countChecked() === 0);
  try {{
    var v = localStorage.getItem("bs-view");
    if (v === "grid" || v === "list") setView(v);
  }} catch (e) {{}}
  try {{
    var sk = localStorage.getItem("bs-sort");
    if (sk === "size" || sk === "date" || sk === "name") {{
      var sb = document.querySelector('.sort-menu button[data-k="' + sk + '"]');
      if (sb) sortRows(sk, sb);
    }}
  }} catch (e) {{}}
  try {{
    var z = parseFloat(localStorage.getItem("bs-zoom"));
    if (z >= 0.6 && z <= 2) setListZoom(z);
  }} catch (e) {{}}
}})();

function applyMeta(m) {{
  SELECTED = m;
  var icon = document.getElementById("dIcon");
  var ext = (m.ext || "file").toLowerCase();
  var isDir = !!(m.dir || ext === "folder" || m.type === "folder");
  var map = {{html:"</>", css:"#", js:"JS", json:"{{}}", md:"MD", txt:"TXT",
              png:"IMG", jpg:"IMG", jpeg:"IMG", gif:"IMG", ico:"&#9733;",
              pdf:"PDF", zip:"ZIP", py:"PY",
              mp4:"&#9654;", webm:"&#9654;", mov:"&#9654;", mkv:"&#9654;",
              avi:"&#9654;", m4v:"&#9654;", folder:"&#128193;"}};
  var cls = {{html:"html", css:"css", js:"js", css:"css"}};
  var badgeCls = "file";
  if (isDir) badgeCls = "folder";
  else if (ext === "html" || ext === "htm") badgeCls = "html";
  else if (ext === "css") badgeCls = "css";
  else if (ext === "js") badgeCls = "js";
  else if (ext === "json") badgeCls = "json";
  else if (ext === "md") badgeCls = "md";
  else if (ext === "zip" || ext === "rar" || ext === "7z") badgeCls = "zip";
  else if (ext === "pdf") badgeCls = "pdf";
  else if (ext === "png" || ext === "jpg" || ext === "jpeg" || ext === "gif") badgeCls = "html";
  else if (ext === "mp4" || ext === "webm" || ext === "ogg" || ext === "ogv"
           || ext === "mov" || ext === "mkv" || ext === "avi" || ext === "m4v"
           || ext === "flv" || ext === "wmv") badgeCls = "vid";
  icon.className = "detail-icon " + badgeCls;
  icon.innerHTML = map[ext] || "&#128196;";
  document.getElementById("dName").textContent = m.name;
  var dp = document.getElementById("dPath");
  if (dp) {{
    if (isDir) {{ dp.style.display = "none"; dp.textContent = ""; }}
    else {{ dp.style.display = ""; dp.textContent = m.path || ""; }}
  }}
  document.getElementById("dSize").textContent = m.size || "\u2014";
  document.getElementById("dType").textContent = m.type || "\u2014";
  document.getElementById("dMod").textContent = m.mtime || "\u2014";
  document.getElementById("dPerm").textContent = m.perm || "\u2014";
  document.getElementById("dlSub1").textContent = isDir
    ? "Get " + m.name + " as ZIP"
    : "Get " + m.name + (m.size ? " (" + m.size + ")" : "");
  document.getElementById("dlSub2").textContent = "Download " + m.name + " as ZIP";
}}

function selectFile(row) {{
  selectOnly(row);
}}

function isMobile() {{
  return window.matchMedia("(max-width: 980px)").matches;
}}

var LAST_ANCHOR = null;

function onRowClick(e, row) {{
  if (e && e.target && e.target.closest && e.target.closest(".fchk")) return;
  if (row.classList.contains("parent-row")) {{ goParent(); return; }}
  if (isMobile() && row.classList.contains("dir") && !(e && (e.ctrlKey || e.metaKey || e.shiftKey))) {{
    goDir(row.getAttribute("data-href"));
    return;
  }}
  if (e && e.shiftKey && LAST_ANCHOR) {{
    selectRange(LAST_ANCHOR, row);
    return;
  }}
  if (e && (e.ctrlKey || e.metaKey)) {{
    toggleRow(row);
    LAST_ANCHOR = row;
    return;
  }}
  selectOnly(row);
  LAST_ANCHOR = row;
}}

function selectRange(fromRow, toRow) {{
  var rows = Array.prototype.slice.call(document.querySelectorAll("#tbody .frow"))
    .filter(function(r) {{ return !r.classList.contains("parent-row"); }});
  var i = rows.indexOf(fromRow);
  var j = rows.indexOf(toRow);
  if (i < 0 || j < 0) {{ selectOnly(toRow); LAST_ANCHOR = toRow; return; }}
  var a = Math.min(i, j), b = Math.max(i, j);
  clearChecks();
  for (var k = a; k <= b; k++) {{
    var r = rows[k];
    var cb = r.querySelector(".chk");
    if (cb) cb.checked = true;
    r.classList.add("selected");
  }}
  updateSelectionUI();
  if (toRow.classList.contains("file")) {{
    try {{ applyMeta(JSON.parse(toRow.getAttribute("data-meta"))); }} catch (err) {{}}
  }}
}}

function toggleRow(row) {{
  var cb = row.querySelector(".chk");
  if (!cb) return;
  cb.checked = !cb.checked;
  if (cb.checked) row.classList.add("selected");
  else {{ row.classList.remove("selected"); row.style.background = ""; }}
  updateSelectionUI();
  if (cb.checked && row.classList.contains("file")) {{
    try {{ applyMeta(JSON.parse(row.getAttribute("data-meta"))); }} catch (err) {{}}
  }}
}}

function unselectAll() {{
  clearChecks();
  LAST_ANCHOR = null;
  updateSelectionUI();
  var dName = document.getElementById("dName");
  if (dName) dName.textContent = "No file selected";
}}

function onRowDblClick(e, row) {{
  if (e && e.target && e.target.closest && e.target.closest(".fchk")) return;
  if (row.classList.contains("parent-row")) {{ goParent(); return; }}
  if (row.classList.contains("dir")) {{
    goDir(row.getAttribute("data-href"));
    return;
  }}
  openFileRow(row);
}}

function selectOnly(row) {{
  clearChecks();
  var cb = row.querySelector(".chk");
  if (cb) cb.checked = true;
  row.classList.add("selected");
  if (row.classList.contains("file")) {{
    try {{ applyMeta(JSON.parse(row.getAttribute("data-meta"))); }}
    catch (err) {{}}
  }}
  updateSelectionUI();
}}

function onCheckChange(cb) {{
  var row = cb.closest(".frow");
  if (!row) return;
  if (cb.checked) {{
    row.classList.add("selected");
  }} else {{
    row.classList.remove("selected");
    row.style.background = "";
  }}
  updateSelectionUI();
}}

function getCheckedRows() {{
  return Array.prototype.slice.call(document.querySelectorAll("#tbody .frow")).filter(function(r) {{
    if (r.classList.contains("parent-row")) return false;
    var c = r.querySelector(".chk");
    return c && c.checked;
  }});
}}

function getCheckedNames() {{
  return getCheckedRows().map(function(r) {{ return r.getAttribute("data-name") || ""; }});
}}

function countChecked() {{
  return getCheckedRows().length;
}}

function clearChecks() {{
  document.querySelectorAll("#tbody .frow").forEach(function(r) {{
    r.classList.remove("selected");
    r.style.background = "";
    var c = r.querySelector(".chk");
    if (c) c.checked = false;
  }});
  var all = document.getElementById("chkAll");
  if (all) all.checked = false;
}}

function toggleAllChecks(cb) {{
  document.querySelectorAll("#tbody .frow").forEach(function(r) {{
    if (r.classList.contains("parent-row")) return;
    if (r.style.display === "none") return;
    var c = r.querySelector(".chk");
    if (c) {{
      c.checked = cb.checked;
      r.classList.toggle("selected", cb.checked);
      if (!cb.checked) r.style.background = "";
    }}
  }});
  updateSelectionUI();
}}

function updateSelectionUI() {{
  var n = countChecked();
  var sub1 = document.getElementById("dlSub1");
  var sub2 = document.getElementById("dlSub2");
  var dName = document.getElementById("dName");
  var dIcon = document.getElementById("dIcon");
  var btnDownload = document.getElementById("btnDownload");
  if (n > 1) {{
    if (btnDownload) {{
      btnDownload.disabled = true;
      btnDownload.classList.add("disabled");
      btnDownload.setAttribute("aria-disabled", "true");
    }}
    if (dName) dName.textContent = n + " items selected";
    if (dIcon) {{ dIcon.className = "detail-icon file"; dIcon.innerHTML = "&#128193;"; }}
    var dPathEl = document.getElementById("dPath");
    if (dPathEl) dPathEl.style.display = "none";
    if (sub1) sub1.textContent = "Disabled for multi-select";
    if (sub2) sub2.textContent = "ZIP " + n + " items";
    var dSize = document.getElementById("dSize");
    var dType = document.getElementById("dType");
    var dMod = document.getElementById("dMod");
    var dPerm = document.getElementById("dPerm");
    if (dSize) dSize.textContent = "—";
    if (dType) dType.textContent = "Multiple selection";
    if (dMod) dMod.textContent = "—";
    if (dPerm) dPerm.textContent = "—";
  }} else if (n === 1) {{
    if (btnDownload) {{
      btnDownload.disabled = false;
      btnDownload.classList.remove("disabled");
      btnDownload.removeAttribute("aria-disabled");
    }}
    var row = getCheckedRows()[0];
    if (row && row.getAttribute("data-meta")) {{
      try {{ applyMeta(JSON.parse(row.getAttribute("data-meta"))); }} catch (e) {{}}
    }} else if (row) {{
      if (dName) dName.textContent = row.getAttribute("data-name") || "";
      if (sub1) sub1.textContent = "Select a file";
      if (sub2) sub2.textContent = "Compress and download";
    }}
  }} else {{
    if (btnDownload) {{
      btnDownload.disabled = false;
      btnDownload.classList.remove("disabled");
      btnDownload.removeAttribute("aria-disabled");
    }}
    if (dName) dName.textContent = "No file selected";
    if (sub1) sub1.textContent = "Select a file";
    if (sub2) sub2.textContent = "Compress and download";
  }}
  setDetailsEmpty(n === 0);
}}

var IMG_EXT = {{png:1,jpg:1,jpeg:1,gif:1,webp:1,svg:1,ico:1,bmp:1,avif:1}};
var VID_EXT = {{mp4:1,webm:1,ogg:1,ogv:1,mov:1,mkv:1,avi:1,m4v:1,flv:1,wmv:1}};
var TEXT_EXT = {{txt:1,html:1,htm:1,css:1,js:1,json:1,md:1,py:1,xml:1,yml:1,yaml:1,
  csv:1,log:1,sh:1,bat:1,ps1:1,ts:1,jsx:1,tsx:1,vue:1,php:1,java:1,c:1,cpp:1,h:1,
  go:1,rs:1,sql:1,ini:1,cfg:1,conf:1,toml:1,env:1,txt:1,rb:1,pl:1,r:1,m:1,swift:1,
  kt:1,scala:1,dart:1,lua:1,ex:1,exs:1,clj:1,hs:1,ml:1,fs:1,asm:1,s:1,mk:1,cmake:1,
  gradle:1,dockerfile:1,makefile:1,gitignore:1,npmrc:1,babelrc:1,eslintrc:1,prettierrc:1}};

function openFileRow(row) {{
  var meta = null;
  try {{ meta = JSON.parse(row.getAttribute("data-meta")); }} catch (e) {{}}
  if (!meta) {{
    var href = row.getAttribute("data-href");
    if (href) goDir(href);
    return;
  }}
  var href = meta.href || "";
  if (!href) return;
  var ext = (meta.ext || "").toLowerCase();
  var sep = href.indexOf("?") >= 0 ? "&" : "?";
  if (IMG_EXT[ext]) {{
    window.open(href + sep + "inline=1", "_blank");
  }} else if (VID_EXT[ext]) {{
    window.open(href + sep + "inline=1", "_blank");
  }} else {{
    window.open(href + sep + "edit=1", "_blank");
  }}
}}

function goDir(href) {{ window.location.href = href; }}
function goParent() {{ window.location.href = "../"; }}
function hoverRow(r) {{ if (!r.classList.contains("selected")) r.style.background = "var(--hover)"; }}
function unhoverRow(r) {{ if (!r.classList.contains("selected")) r.style.background = ""; }}

function toggleTNode(e, btn) {{
  if (e) {{ e.preventDefault(); e.stopPropagation(); }}
  var node = btn.closest(".tnode");
  if (node) node.classList.toggle("open");
}}

/* ===== DRAG & DROP ===== */
var _dragNames = [];
var _dragFrom = null;

function onDragStart(e, row) {{
  if (row.classList.contains("parent-row")) {{ e.preventDefault(); return; }}
  if (!row.classList.contains("selected")) {{
    selectOnly(row);
    LAST_ANCHOR = row;
  }}
  _dragNames = getCheckedNames();
  _dragFrom = location.pathname;
  if (e.dataTransfer) {{
    e.dataTransfer.effectAllowed = "move";
    try {{ e.dataTransfer.setData("text/plain", _dragNames.join("\\n")); }} catch (err) {{}}
  }}
  row.classList.add("dragging");
}}

function onDragEnd(e) {{
  document.querySelectorAll(".frow.dragging").forEach(function(r) {{ r.classList.remove("dragging"); }});
  document.querySelectorAll(".frow.drag-over").forEach(function(r) {{ r.classList.remove("drag-over"); }});
  _dragNames = [];
  _dragFrom = null;
}}

function onDragOver(e, row) {{
  if (!row.classList.contains("dir") || row.classList.contains("parent-row")) return;
  e.preventDefault();
  if (e.dataTransfer) e.dataTransfer.dropEffect = "move";
  row.classList.add("drag-over");
}}

function onDragLeave(e, row) {{
  row.classList.remove("drag-over");
}}

function onDropRow(e, row) {{
  e.preventDefault();
  e.stopPropagation();
  row.classList.remove("drag-over");
  if (!row.classList.contains("dir") || row.classList.contains("parent-row")) return;
  var names = _dragNames.length ? _dragNames : getCheckedNames();
  if (!names.length) return;
  var destHref = row.getAttribute("data-href") || "";
  if (!destHref) return;
  /* cannot drop into itself */
  var destName = row.getAttribute("data-name") || "";
  names = names.filter(function(n) {{ return n !== destName; }});
  if (!names.length) {{ toast("Cannot drop into itself"); return; }}
  var destPath = destHref.charAt(0) === "/" ? destHref : (location.pathname.replace(/[^/]*$/, "") + destHref);
  moveItems(names, destPath);
}}

function moveItems(names, destPath) {{
  var p = Promise.resolve();
  var failed = 0;
  names.forEach(function(n) {{
    p = p.then(function() {{
      return postApi("move", {{ name: n, dest: destPath }}).then(function(res) {{
        if (!(res && res.j && res.j.ok)) failed++;
      }}).catch(function() {{ failed++; }});
    }});
  }});
  p.then(function() {{
    if (failed) toast("Moved with " + failed + " error(s)");
    else toast("Moved " + names.length + " item(s) to " + destPath);
    setTimeout(function() {{ location.reload(); }}, 500);
  }});
}}

/* click empty area of list -> unselect */
document.addEventListener("click", function(e) {{
  var tb = document.getElementById("tbody");
  if (tb && e.target === tb) unselectAll();
  else if (tb && e.target.classList && e.target.classList.contains("empty-state")) unselectAll();
}});

function filterRows() {{
  var q = document.getElementById("searchInput").value.trim().toLowerCase();
  var rows = document.querySelectorAll("#tbody .frow");
  if (!q) {{
    rows.forEach(function(r) {{ r.style.display = ""; }});
    var emptyOff = document.querySelector("#tbody .empty-state[data-filtered]");
    if (emptyOff) emptyOff.style.display = "";
    return;
  }}
  rows.forEach(function(r) {{
    var t = r.textContent.toLowerCase();
    r.style.display = t.indexOf(q) >= 0 ? "" : "none";
  }});
  var empty = document.querySelector("#tbody .empty-state");
  if (empty) empty.style.display = "none";
}}

/* ---- dual search: local folder vs whole server ---- */
var SEARCH_SCOPE = "local";
var _serverSearchTimer = null;
var _localRowsHTML = null;

function toggleScopeMenu(e) {{
  if (e) e.stopPropagation();
  var menu = document.getElementById("scopeMenu");
  var dots = document.getElementById("scopeDots");
  var open = menu.classList.toggle("open");
  if (dots) dots.classList.toggle("on", open);
}}

function closeScopeMenu() {{
  var menu = document.getElementById("scopeMenu");
  var dots = document.getElementById("scopeDots");
  if (menu) menu.classList.remove("open");
  if (dots) dots.classList.remove("on");
}}

function setSearchScope(scope) {{
  SEARCH_SCOPE = scope;
  document.querySelectorAll("#scopeMenu button").forEach(function(b) {{
    b.classList.toggle("on", b.getAttribute("data-scope") === scope);
  }});
  closeScopeMenu();
  onSearchInput();
}}

function showAllRows() {{
  document.querySelectorAll("#tbody .frow").forEach(function(r) {{ r.style.display = ""; }});
  var empty = document.querySelector("#tbody .empty-state");
  if (empty) empty.style.display = "";
}}

function onSearchInput() {{
  var q = document.getElementById("searchInput").value.trim();
  clearTimeout(_serverSearchTimer);
  if (!q) {{
    /* empty box -> normal full listing in both modes */
    restoreLocalRows();
    showAllRows();
    return;
  }}
  if (SEARCH_SCOPE === "server") {{
    _serverSearchTimer = setTimeout(function() {{ runServerSearch(q); }}, 300);
  }} else {{
    restoreLocalRows();
    filterRows();
  }}
}}

function restoreLocalRows() {{
  var tb = document.getElementById("tbody");
  if (_localRowsHTML !== null) {{
    tb.innerHTML = _localRowsHTML;
    _localRowsHTML = null;
    updateSelectionUI();
  }}
}}

function runServerSearch(q) {{
  var tb = document.getElementById("tbody");
  if (_localRowsHTML === null) _localRowsHTML = tb.innerHTML;
  tb.innerHTML = '<div class="empty-state"><div class="empty-icon">\\u2315</div><div>Searching entire server...</div></div>';
  updateSelectionUI();
  fetch(location.pathname + "?__api=search", {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ q: q }})
  }})
    .then(function(r) {{ return r.json(); }})
    .then(function(j) {{
      if (!j || !j.ok) {{ tb.innerHTML = '<div class="empty-state"><div>Search failed</div></div>'; updateSelectionUI(); return; }}
      var res = j.results || [];
      if (!res.length) {{
        tb.innerHTML = '<div class="empty-state"><div class="empty-icon">\\u2315</div><div>No matches on server for "' + escapeHtml(q) + '"</div></div>';
        updateSelectionUI();
        return;
      }}
      tb.innerHTML = res.map(function(it) {{
        var isDir = it.kind === 1;
        var sub = isDir ? "Folder" : "File";
        if (isDir) {{
          var dmeta = JSON.stringify({{
            name: it.name, href: it.href, size: "", sizeB: 0,
            type: "folder", mtime: "", mtimeTs: 0, perm: "",
            path: it.path, ext: "folder", dir: 1
          }});
          return '<div class="frow dir" data-href="' + it.href + '" data-meta="' + escapeHtml(dmeta) + '" data-name="' + escapeHtml(it.name) + '" ' +
            'data-kind="1" data-size="-1" data-mtime="0" draggable="true" ' +
            'onclick="onRowClick(event, this)" ' +
            'ondblclick="onRowDblClick(event, this)" ' +
            'oncontextmenu="onRowContextMenu(event, this)" ' +
            'ondragstart="onDragStart(event, this)" ' +
            'ondragend="onDragEnd(event)" ' +
            'ondragover="onDragOver(event, this)" ' +
            'ondragleave="onDragLeave(event, this)" ' +
            'ondrop="onDropRow(event, this)" ' +
            'onmouseenter="hoverRow(this)" onmouseleave="unhoverRow(this)">' +
            '<div class="fcell fchk" onclick="event.stopPropagation()">' +
            '<input type="checkbox" class="chk" onchange="onCheckChange(this)"></div>' +
            '<div class="fcell fname"><span class="badge folder">\\ud83d\\udcc1</span>' +
            '<span class="ftext"><span class="flabel">' + escapeHtml(it.name) + '</span>' +
            '<span class="fsub">' + escapeHtml(it.path) + '</span></span></div>' +
            '<div class="fcell fsize">&mdash;</div>' +
            '<div class="fcell fmtime">&mdash;</div>' +
            '<div class="fcell fdot"></div></div>';
        }}
        var fmeta = JSON.stringify({{
          name: it.name, href: it.href, size: "", sizeB: 0,
          type: "file", mtime: "", mtimeTs: 0, perm: "",
          path: it.path, ext: (it.name.split(".").pop() || "file").toLowerCase()
        }});
        return '<div class="frow file" data-meta="' + escapeHtml(fmeta) + '" data-href="' + it.href + '" data-name="' + escapeHtml(it.name) + '" ' +
          'data-kind="2" data-size="0" data-mtime="0" draggable="true" ' +
          'onclick="onRowClick(event, this)" ' +
          'ondblclick="onRowDblClick(event, this)" ' +
          'oncontextmenu="onRowContextMenu(event, this)" ' +
          'ondragstart="onDragStart(event, this)" ' +
          'ondragend="onDragEnd(event)" ' +
          'ondragover="onDragOver(event, this)" ' +
          'ondragleave="onDragLeave(event, this)" ' +
          'ondrop="onDropRow(event, this)" ' +
          'onmouseenter="hoverRow(this)" onmouseleave="unhoverRow(this)">' +
          '<div class="fcell fchk" onclick="event.stopPropagation()">' +
          '<input type="checkbox" class="chk" onchange="onCheckChange(this)"></div>' +
          '<div class="fcell fname"><span class="badge file">FILE</span>' +
          '<span class="ftext"><span class="flabel">' + escapeHtml(it.name) + '</span>' +
          '<span class="fsub">' + escapeHtml(it.path) + '</span></span></div>' +
          '<div class="fcell fsize"></div>' +
          '<div class="fcell fmtime"></div>' +
          '<div class="fcell fdot"></div></div>';
      }}).join("");
      updateSelectionUI();
    }})
    .catch(function() {{
      tb.innerHTML = '<div class="empty-state"><div>Connection error during search</div></div>';
      updateSelectionUI();
    }});
}}

function escapeHtml(s) {{
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}}

function setView(v) {{
  var tb = document.getElementById("tbody");
  var bg = document.getElementById("btnGrid");
  var bl = document.getElementById("btnList");
  var ctr = document.querySelector(".center");
  if (v === "grid") {{ tb.classList.add("grid-view"); bg.classList.add("on"); bl.classList.remove("on"); }}
  else {{ tb.classList.remove("grid-view"); bl.classList.add("on"); bg.classList.remove("on"); }}
  if (ctr) ctr.classList.toggle("grid-mode", v === "grid");
  try {{ localStorage.setItem("bs-view", v); }} catch (e) {{}}
}}

function toggleSort(e) {{
  e.stopPropagation();
  document.getElementById("sortMenu").classList.toggle("open");
}}
document.addEventListener("click", function() {{
  document.getElementById("sortMenu").classList.remove("open");
  closeScopeMenu();
  closeRowMenu();
  closeThemeMenu();
}});

function sortRows(key, btn) {{
  document.querySelectorAll(".sort-menu button").forEach(function(b) {{
    b.classList.remove("on");
  }});
  btn.classList.add("on");
  try {{ localStorage.setItem("bs-sort", key); }} catch (e) {{}}
  var tb = document.getElementById("tbody");
  var empty = tb.querySelector(".empty-state");
  var rows = Array.prototype.slice.call(tb.querySelectorAll(".frow"));
  rows.sort(function(a, b) {{
    var ka = +(a.getAttribute("data-kind") || 2);
    var kb = +(b.getAttribute("data-kind") || 2);
    if (ka !== kb) return ka - kb;
    if (key === "name") {{
      var na = a.getAttribute("data-name") || "";
      var nb = b.getAttribute("data-name") || "";
      try {{ return na.localeCompare(nb, undefined, {{numeric: true, sensitivity: "base"}}); }}
      catch (e) {{ return na < nb ? -1 : na > nb ? 1 : 0; }}
    }}
    if (key === "size") {{
      var sa = +(a.getAttribute("data-size") || -1);
      var sb = +(b.getAttribute("data-size") || -1);
      if (sa !== sb) return sb - sa;
      var xa = a.getAttribute("data-name") || "";
      var xb = b.getAttribute("data-name") || "";
      try {{ return xa.localeCompare(xb, undefined, {{numeric: true, sensitivity: "base"}}); }}
      catch (e) {{ return 0; }}
    }}
    if (key === "date") {{
      var da = +(a.getAttribute("data-mtime") || 0);
      var db = +(b.getAttribute("data-mtime") || 0);
      if (da !== db) return db - da;
      var xa2 = a.getAttribute("data-name") || "";
      var xb2 = b.getAttribute("data-name") || "";
      try {{ return xa2.localeCompare(xb2, undefined, {{numeric: true, sensitivity: "base"}}); }}
      catch (e) {{ return 0; }}
    }}
    return 0;
  }});
  rows.forEach(function(r) {{ tb.appendChild(r); }});
  if (empty) tb.appendChild(empty);
}}

function currentHref() {{
  if (!SELECTED) return null;
  return SELECTED.href;
}}

function actDownload() {{
  var checked = getCheckedRows();
  if (checked.length > 1) {{
    var names = checked.map(function(r) {{ return r.getAttribute("data-name") || ""; }});
    window.location.href = location.pathname + "?" + names.map(function(n) {{
      return "items=" + encodeURIComponent(n);
    }}).join("&");
    return;
  }}
  if (checked.length === 1) {{
    var row = checked[0];
    if (row.classList.contains("file")) {{
      try {{ var m = JSON.parse(row.getAttribute("data-meta")); window.location.href = m.href; return; }} catch (e) {{}}
    }}
    var h = row.getAttribute("data-href");
    if (h) {{ window.location.href = h + "?zip=1"; return; }}
  }}
  if (!SELECTED) {{ toast("Select a file first"); return; }}
  window.location.href = SELECTED.href;
}}
function actZip() {{
  var checked = getCheckedRows();
  if (checked.length > 1) {{
    var names = checked.map(function(r) {{ return r.getAttribute("data-name") || ""; }});
    window.location.href = location.pathname + "?" + names.map(function(n) {{
      return "items=" + encodeURIComponent(n);
    }}).join("&");
    return;
  }}
  if (checked.length === 1) {{
    var row = checked[0];
    var h = row.getAttribute("data-href");
    if (h) {{ window.location.href = h + "?zip=1"; return; }}
    try {{ var m = JSON.parse(row.getAttribute("data-meta")); window.location.href = m.href + "?zip=1"; return; }} catch (e) {{}}
  }}
  if (!SELECTED) {{ toast("Select a file first"); return; }}
  window.location.href = SELECTED.href + "?zip=1";
}}
function actShare() {{
  var checked = getCheckedRows();
  if (checked.length > 1) {{
    var names = checked.map(function(r) {{ return r.getAttribute("data-name") || ""; }});
    var url = location.origin + location.pathname + "?" + names.map(function(n) {{
      return "items=" + encodeURIComponent(n);
    }}).join("&");
    copyText(url, "Share link copied for " + checked.length + " items!");
    return;
  }}
  if (checked.length === 1) {{
    var row = checked[0];
    var h = row.getAttribute("data-href") || "";
    try {{ var m = JSON.parse(row.getAttribute("data-meta")); if (m.href) h = m.href; }} catch (e) {{}}
    if (h) {{
      var u1 = location.origin + (BASE || "/").replace(/\\/$/, "") + "/" + h.replace(/^\\//, "");
      if (h.charAt(0) === "/") u1 = location.origin + h;
      copyText(u1, "Share link copied!");
      return;
    }}
  }}
  if (!SELECTED) {{ toast("Select a file first"); return; }}
  var url = location.origin + BASE.replace(/\\/$/, "") + "/" + SELECTED.href;
  copyText(url, "Share link copied!");
}}
function actCopy() {{
  var checked = getCheckedRows();
  if (checked.length > 1) {{
    var names = checked.map(function(r) {{ return r.getAttribute("data-name") || ""; }});
    var url = location.origin + location.pathname + "?" + names.map(function(n) {{
      return "items=" + encodeURIComponent(n);
    }}).join("&");
    copyText(url, "Direct link copied for " + checked.length + " items!");
    return;
  }}
  if (checked.length === 1) {{
    var row = checked[0];
    var h = row.getAttribute("data-href") || "";
    try {{ var m = JSON.parse(row.getAttribute("data-meta")); if (m.href) h = m.href; }} catch (e) {{}}
    if (h) {{
      var u1 = h.charAt(0) === "/" ? (location.origin + h)
        : (location.origin + (BASE || "/").replace(/\\/$/, "") + "/" + h.replace(/^\\//, ""));
      copyText(u1, "Direct link copied!");
      return;
    }}
  }}
  if (!SELECTED) {{ toast("Select a file first"); return; }}
  var url = location.origin + BASE.replace(/\\/$/, "") + "/" + SELECTED.href;
  copyText(url, "Direct link copied!");
}}
function copyText(t, msg) {{
  if (navigator.clipboard && navigator.clipboard.writeText) {{
    navigator.clipboard.writeText(t).then(function() {{ toast(msg); }},
      function() {{ fallbackCopy(t, msg); }});
  }} else fallbackCopy(t, msg);
}}
function fallbackCopy(t, msg) {{
  var ta = document.createElement("textarea");
  ta.value = t; document.body.appendChild(ta);
  ta.select(); try {{ document.execCommand("copy"); }} catch (e) {{}}
  document.body.removeChild(ta); toast(msg);
}}

var toastTimer = null;
function toast(msg) {{
  var el = document.getElementById("toast");
  el.textContent = msg;
  el.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(function() {{ el.classList.remove("show"); }}, 2400);
}}

/* ===== THEME PICKER ===== */
var THEMES = ["dark", "light", "blue", "purple", "orange", "green", "sunset"];
function isTheme(v) {{ return THEMES.indexOf(v) >= 0; }}
(function initTheme() {{
  var t = localStorage.getItem("bs-theme");
  if (!isTheme(t)) t = "dark";
  document.documentElement.setAttribute("data-theme", t);
  markThemeUI();
}})();
function toggleTheme(e) {{
  if (e) e.stopPropagation();
  var w = document.getElementById("themeWrap");
  var m = document.getElementById("themeMenu");
  var open = !m.classList.contains("open");
  m.classList.toggle("open", open);
  if (w) w.classList.toggle("open", open);
}}
function closeThemeMenu() {{
  var w = document.getElementById("themeWrap");
  var m = document.getElementById("themeMenu");
  if (m) m.classList.remove("open");
  if (w) w.classList.remove("open");
}}
function markThemeUI() {{
  var t = document.documentElement.getAttribute("data-theme");
  document.querySelectorAll("[data-t]").forEach(function(b) {{
    b.classList.toggle("on", b.getAttribute("data-t") === t);
  }});
}}

/* ===== UPLOAD (resumable, one row per file) ===== */
var UP = [];              /* every file the user picked, in order */
var _upDoneTimer = null;
var _upCounter = 0;

function upBytes(n) {{
  if (!n && n !== 0) return "—";
  var u = ["B", "KB", "MB", "GB", "TB"], i = 0, v = n;
  while (v >= 1024 && i < u.length - 1) {{ v /= 1024; i++; }}
  return (i ? v.toFixed(v < 10 ? 1 : 0) : v) + " " + u[i];
}}
function upEsc(s) {{
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}}
function _upJson(xhr) {{
  try {{ return JSON.parse(xhr.responseText); }} catch (e) {{ return null; }}
}}

function openUploadModal(count) {{
  document.getElementById("upTitle").textContent = "آپلود فایل";
  document.getElementById("upSub").textContent = count + " فایل — در حال انتقال به سرور…";
  document.getElementById("upPct").textContent = "0%";
  document.getElementById("upBar").style.width = "0%";
  document.getElementById("upMeta").textContent = "";
  document.getElementById("upDone").style.display = "none";
  document.getElementById("upDone").style.color = "";
  document.getElementById("upCancelBtn").style.display = "";
  document.getElementById("upCloseBtn").style.display = "none";
  document.getElementById("upModal").classList.add("open");
}}
function closeUploadModal() {{
  document.getElementById("upModal").classList.remove("open");
  clearTimeout(_upDoneTimer);
}}

function _upAbort(it) {{
  if (it && it.xhr) {{
    var x = it.xhr;
    it.xhr = null;
    it.gen = (it.gen || 0) + 1;
    try {{ x.abort(); }} catch (e) {{}}
  }}
}}
function _upDropSession(it) {{
  if (!it.sid) return;
  var sid = it.sid;
  it.sid = null;
  try {{
    var x = new XMLHttpRequest();
    x.open("POST", location.pathname + "?__api=updrop&sid=" + encodeURIComponent(sid));
    x.send();
  }} catch (e) {{}}
}}

/* every button of one row */
function uploadItemAction(id, what) {{
  var it = null;
  for (var i = 0; i < UP.length; i++) if (UP[i].id === id) it = UP[i];
  if (!it) return;
  if (what === "cancel") {{
    _upAbort(it);
    it.state = "cancelled";
    _upDropSession(it);
    renderUploads();
    uploadTick();
    return;
  }}
  if (what === "pause") {{
    if (it.state !== "uploading") return;
    _upAbort(it);
    it.inflight = 0;
    it.state = "paused";
    renderUploads();
    uploadTick();
    return;
  }}
  if (what === "resume") {{
    if (it.state !== "paused" && it.state !== "error") return;
    it.state = "queued";
    it.inflight = 0;
    it.error = "";
    renderUploads();
    pumpUploads();
    return;
  }}
}}

function renderUploads() {{
  var host = document.getElementById("upList");
  var active = UP.filter(function(x) {{ return x.state !== "cancelled"; }});
  var acc = 0, tot = 0, doneN = 0, pausedN = 0, errN = 0, busy = 0;
  var html = "";
  active.forEach(function(it) {{
    var sent = it.acked + (it.inflight || 0);
    var pct = it.size > 0 ? Math.min(100, Math.round((sent / it.size) * 100)) : 0;
    if (it.state === "done") pct = 100;
    var label = pct + "%", cap = "";
    var buttons = "";
    if (it.state === "done") {{
      doneN++; label = "✓ کامل";
    }} else if (it.state === "paused") {{
      pausedN++; label = "توقف " + pct + "%"; cap = "paused";
      buttons = '<button class="upb del" onclick="uploadItemAction(\\'' + it.id + '\\',\\'cancel\\')">لغو</button>'
              + '<button class="upb go" onclick="uploadItemAction(\\'' + it.id + '\\',\\'resume\\')">ادامه</button>';
    }} else if (it.state === "error") {{
      errN++; cap = "err"; label = "خطا";
      buttons = '<button class="upb del" onclick="uploadItemAction(\\'' + it.id + '\\',\\'cancel\\')">لغو</button>'
              + '<button class="upb stop" onclick="uploadItemAction(\\'' + it.id + '\\',\\'resume\\')">تلاش دوباره</button>';
    }} else {{
      busy++;
      if (it.state === "queued") {{ cap = "paused"; label = "در صف"; }}
      else label = pct + "%";
      buttons = '<button class="upb del" onclick="uploadItemAction(\\'' + it.id + '\\',\\'cancel\\')">لغو</button>'
              + '<button class="upb stop" onclick="uploadItemAction(\\'' + it.id + '\\',\\'pause\\')">توقف</button>';
    }}
    acc += sent; tot += it.size;
    html += '<div class="up-item ' + cap + '">'
      + '<div class="up-top">'
      + '<span class="up-ico">&#128196;</span>'
      + '<span class="up-name" title="' + upEsc(it.file.name) + '">' + upEsc(it.file.name) + '</span>'
      + '<span class="up-size">' + upBytes(it.size) + '</span>'
      + '<span class="up-pcti">' + label + '</span>'
      + '</div>'
      + '<div class="up-bottom">'
      + '<div class="up-track"><i style="width:' + pct + '%"></i></div>'
      + buttons
      + '</div>'
      + (it.error ? '<div class="up-size" style="margin-top:6px;color:var(--red)">' + upEsc(it.error) + '</div>' : "")
      + '</div>';
  }});
  host.innerHTML = html;
  var p = tot > 0 ? Math.min(100, Math.round((acc / tot) * 100)) : 0;
  document.getElementById("upPct").textContent = p + "%";
  document.getElementById("upBar").style.width = p + "%";
  document.getElementById("upMeta").textContent = active.length
    ? active.length + " فایل • " + upBytes(acc) + " از " + upBytes(tot)
    : "";
  var sub = document.getElementById("upSub");
  if (doneN && doneN === active.length) sub.textContent = "انتقال کامل شد";
  else if (pausedN && !busy) sub.textContent = pausedN + " فایل متوقف است";
  else if (errN && !busy) sub.textContent = errN + " فایل با خطا مواجه شد";
  else sub.textContent = active.length + " فایل — در حال انتقال به سرور…";
}}

/* what the modal should do now that the queue changed */
function uploadTick() {{
  var active = UP.filter(function(x) {{ return x.state !== "cancelled"; }});
  if (!active.length) {{
    var fi0 = document.getElementById("fileInput");
    if (fi0) fi0.value = "";
    closeUploadModal();
    if (location.reload) location.reload();
    return;
  }}
  var busy = active.some(function(x) {{ return x.state === "queued" || x.state === "uploading"; }});
  document.getElementById("upCancelBtn").style.display = busy ? "" : "none";
  document.getElementById("upCloseBtn").style.display = busy ? "none" : "";
  var done = active.every(function(x) {{ return x.state === "done"; }});
  var d = document.getElementById("upDone");
  if (done) {{
    d.style.display = "";
    d.style.color = "";
    d.textContent = "✓ آپلود شد";
    toast("Uploaded: " + active.map(function(x) {{ return x.file.name; }}).join(", "));
    clearTimeout(_upDoneTimer);
    _upDoneTimer = setTimeout(function() {{
      closeUploadModal();
      setTimeout(function() {{ location.reload(); }}, 350);
    }}, 1400);
  }} else {{
    d.style.display = "none";
  }}
}}

/* one request: create the server-side session */
function upStart(it) {{
  return new Promise(function(resolve, reject) {{
    var x = new XMLHttpRequest();
    it.xhr = x;
    var gen = it.gen = (it.gen || 0) + 1;
    var q = "?__api=upstart&name=" + encodeURIComponent(it.file.name) + "&size=" + it.size;
    x.open("POST", location.pathname + q);
    x.onload = function() {{
      if (gen !== it.gen) return reject({{ cancelled: true }});
      it.xhr = null;
      var j = _upJson(x);
      if (x.status >= 200 && x.status < 300 && j && j.ok) resolve(j);
      else reject({{ message: (j && j.error) || ("HTTP " + x.status) }});
    }};
    x.onerror = function() {{ if (gen === it.gen) {{ it.xhr = null; reject({{ message: "خطای شبکه" }}); }} }};
    x.onabort = function() {{ if (gen === it.gen) reject({{ cancelled: true }}); }};
    x.send();
  }});
}}

/* one request: ship a single chunk */
function upPart(it, index) {{
  return new Promise(function(resolve, reject) {{
    var start = index * it.partSize;
    var blob = it.file.slice(start, Math.min(start + it.partSize, it.size));
    var x = new XMLHttpRequest();
    it.xhr = x;
    var gen = it.gen = (it.gen || 0) + 1;
    x.open("POST", location.pathname + "?__api=uppart&sid=" + encodeURIComponent(it.sid)
          + "&part=" + index);
    x.setRequestHeader("Content-Type", "application/octet-stream");
    x.upload.onprogress = function(ev) {{
      if (gen !== it.gen || !ev.lengthComputable) return;
      it.inflight = ev.loaded;
      renderUploads();
    }};
    x.onload = function() {{
      if (gen !== it.gen) return reject({{ cancelled: true }});
      it.xhr = null;
      it.inflight = 0;
      var j = _upJson(x);
      if (x.status >= 200 && x.status < 300 && j && j.ok) resolve(j);
      else reject({{ message: (j && j.error) || ("HTTP " + x.status) }});
    }};
    x.onerror = function() {{ if (gen === it.gen) {{ it.xhr = null; it.inflight = 0; reject({{ message: "خطای شبکه" }}); }} }};
    x.onabort = function() {{
      if (gen !== it.gen) return reject({{ cancelled: true }});
      it.inflight = 0;
      reject({{ cancelled: true }});
    }};
    x.send(blob);
  }});
}}

/* one request: publish the assembled file */
function upFinish(it) {{
  return new Promise(function(resolve, reject) {{
    var x = new XMLHttpRequest();
    it.xhr = x;
    var gen = it.gen = (it.gen || 0) + 1;
    x.open("POST", location.pathname + "?__api=upfinish&sid=" + encodeURIComponent(it.sid));
    x.onload = function() {{
      if (gen !== it.gen) return reject({{ cancelled: true }});
      it.xhr = null;
      var j = _upJson(x);
      if (x.status >= 200 && x.status < 300 && j && j.ok) resolve(j);
      else reject({{ message: (j && j.error) || ("HTTP " + x.status) }});
    }};
    x.onerror = function() {{ if (gen === it.gen) {{ it.xhr = null; reject({{ message: "خطای شبکه" }}); }} }};
    x.onabort = function() {{ if (gen === it.gen) reject({{ cancelled: true }}); }};
    x.send();
  }});
}}

/* drive one file from its first chunk to the finish request */
async function runUpload(it) {{
  it.state = "uploading";
  renderUploads();
  try {{
    if (!it.sid) {{
      var info = await upStart(it);
      if (it.state === "cancelled") return;
      it.sid = info.sid;
      it.partSize = info.part;
      it.parts = info.parts;
      it.acked = 0;
    }}
    for (var i = 0; i < it.parts; i++) {{
      if (it.state !== "uploading") return;
      await upPart(it, i);
      it.acked = Math.min(it.size, (i + 1) * it.partSize);
      renderUploads();
    }}
    await upFinish(it);
    if (it.state === "cancelled") return;
    it.sid = null;
    it.acked = it.size;
    it.inflight = 0;
    it.state = "done";
  }} catch (err) {{
    it.inflight = 0;
    if (err && err.cancelled) return;      /* paused or cancelled on purpose */
    it.state = "error";
    it.error = (err && err.message) || "خطای نامشخص";
  }}
  renderUploads();
  uploadTick();
}}

/* everything not paused, finished or cancelled starts now */
function pumpUploads() {{
  var jobs = [];
  UP.forEach(function(it) {{
    if (it.state === "queued") jobs.push(runUpload(it));
  }});
  uploadTick();
  if (jobs.length) Promise.all(jobs);
}}

function cancelUpload() {{
  var any = false;
  UP.forEach(function(it) {{
    if (it.state === "queued" || it.state === "uploading") {{
      any = true;
      _upAbort(it);
      _upDropSession(it);
      it.state = "cancelled";
    }}
  }});
  renderUploads();
  uploadTick();
  if (any) toast("آپلود لغو شد");
  var fi = document.getElementById("fileInput");
  if (fi) fi.value = "";
}}

function uploadFiles(fileList) {{
  var files = [];
  for (var i = 0; i < (fileList ? fileList.length : 0); i++) files.push(fileList[i]);
  if (!files.length) return;
  openUploadModal(files.length);
  UP = files.map(function(f) {{
    return {{
      id: "u" + (++_upCounter),
      file: f,
      size: f.size,
      acked: 0,
      inflight: 0,
      partSize: 8 * 1024 * 1024,
      parts: 1,
      sid: null,
      xhr: null,
      gen: 0,
      state: "queued",
      error: "",
    }};
  }});
  renderUploads();
  pumpUploads();
}}
/* ===== NEW MODAL (جدید -> folder | file) ===== */
var createKind = null; /* "folder" | "file" | null */

function openNewModal() {{
  document.getElementById("newModal").classList.add("open");
  createKind = null;
}}
function closeNewModal() {{
  document.getElementById("newModal").classList.remove("open");
}}
function openNameModal(kind) {{
  createKind = kind;
  closeNewModal();
  var title = document.getElementById("nameTitle");
  var sub = document.getElementById("nameSub");
  var label = document.getElementById("nameLabel");
  var input = document.getElementById("nameInput");
  var hint = document.getElementById("nameHint");
  var ok = document.getElementById("nameOk");
  ok.disabled = false;
  ok.innerHTML = "تایید";
  input.value = "";
  if (kind === "folder") {{
    title.textContent = "پوشه جدید";
    sub.innerHTML = "مسیر: <strong dir=\\"ltr\\">{html.escape(display_path)}</strong>";
    label.textContent = "نام پوشه";
    hint.textContent = "پوشه در مسیر فعلی ساخته می‌شود";
  }} else {{
    title.textContent = "فایل جدید";
    sub.innerHTML = "مسیر: <strong dir=\\"ltr\\">{html.escape(display_path)}</strong>";
    label.textContent = "نام فایل";
    hint.textContent = "بدون پسوند → .txt اضافه می‌شود";
  }}
  document.getElementById("nameModal").classList.add("open");
  setTimeout(function() {{ input.focus(); }}, 200);
}}
function closeNameModal() {{
  document.getElementById("nameModal").classList.remove("open");
  createKind = null;
}}
function nameInputChanged() {{
  var v = document.getElementById("nameInput").value.trim();
  var hint = document.getElementById("nameHint");
  if (createKind === "file") {{
    if (v && v.indexOf(".") < 0) hint.textContent = "ذخیره می‌شود با: " + v + ".txt";
    else if (v) hint.textContent = "ذخیره می‌شود با: " + v;
    else hint.textContent = "بدون پسوند → .txt اضافه می‌شود";
  }} else {{
    hint.textContent = v ? "پوشه: " + v : "پوشه در مسیر فعلی ساخته می‌شود";
  }}
}}
function confirmName() {{
  var input = document.getElementById("nameInput");
  var name = input.value.trim();
  if (!name) {{ toast("نام را وارد کنید"); input.focus(); return; }}
  if (name.indexOf("/") >= 0 || name.indexOf("\\\\") >= 0 || name === "." || name === "..") {{
    toast("نام نامعتبر است"); return;
  }}
  var api = createKind === "folder" ? "mkdir" : "create";
  var ok = document.getElementById("nameOk");
  ok.disabled = true;
  ok.innerHTML = "در حال ساخت...<span class=\\"spinner\\"></span>";
  fetch(location.pathname + "?__api=" + api, {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify({{ name: name }})
  }})
    .then(function(r) {{ return r.json().then(function(j) {{ return {{s: r.status, j: j}}; }}); }})
    .then(function(res) {{
      if (res.j && res.j.ok) {{
        toast((createKind === "folder" ? "پوشه ساخته شد: " : "فایل ساخته شد: ") + res.j.name);
        closeNameModal();
        setTimeout(function() {{ location.reload(); }}, 500);
      }} else {{
        toast((res.j && res.j.error) || "خطا");
        ok.disabled = false;
        ok.innerHTML = "تایید";
      }}
    }})
    .catch(function() {{
      toast("خطا در ارتباط با سرور");
      ok.disabled = false;
      ok.innerHTML = "تایید";
    }});
}}
document.addEventListener("keydown", function(e) {{
  if (e.key === "Escape") {{
    closeNewModal(); closeNameModal();
    closeDelModal(); closeRenModal(); closeDestModal(); closeSettings();
    closeRowMenu();
    closeThemeMenu();
    if (document.getElementById("upModal").classList.contains("open")
        && document.getElementById("upCancelBtn").style.display !== "none") cancelUpload();
    else if (document.getElementById("upModal").classList.contains("open")
             && document.getElementById("upCloseBtn").style.display !== "none") closeUploadModal();
    if (countChecked() > 0) unselectAll();
    return;
  }}
  if (e.key === "F2") {{
    var rows = getCheckedRows();
    if (rows.length === 1) {{
      e.preventDefault();
      CTX = {{
        name: rows[0].getAttribute("data-name") || "",
        kind: +(rows[0].getAttribute("data-kind") || 2),
        multi: false,
        names: [rows[0].getAttribute("data-name") || ""]
      }};
      openRenModal();
    }}
    return;
  }}
  if (e.key === "a" && (e.ctrlKey || e.metaKey)) {{
    var tag = (e.target && e.target.tagName) || "";
    if (tag === "INPUT" || tag === "TEXTAREA") return;
    e.preventDefault();
    document.getElementById("chkAll").checked = true;
    toggleAllChecks(document.getElementById("chkAll"));
  }}
}});

/* ===== ROW CONTEXT MENU (three dots + right-click) ===== */
var CTX = null; /* {{name, kind, multi, names}} kind: 1=dir 2=file */
var destMode = null; /* "move" | "copy" | null */
var destPath = "/";

function stopRowClick(e) {{
  if (e) {{ e.preventDefault(); e.stopPropagation(); }}
}}

function onRowContextMenu(e, row) {{
  if (!e) return;
  e.preventDefault();
  e.stopPropagation();
  if (!row || row.classList.contains("parent-row")) return;
  if (!row.classList.contains("selected")) {{
    selectOnly(row);
    LAST_ANCHOR = row;
  }}
  showRowMenuAt(e.clientX, e.clientY, row);
}}

function showRowMenuAt(x, y, row) {{
  if (!row || row.classList.contains("parent-row")) return;
  var multi = countChecked() > 1 && row.classList.contains("selected");
  var names = multi ? getCheckedNames() : [row.getAttribute("data-name") || ""];
  CTX = {{
    name: row.getAttribute("data-name") || "",
    kind: +(row.getAttribute("data-kind") || 2),
    multi: multi,
    names: names
  }};
  var menu = document.getElementById("ctxMenu");
  document.getElementById("ctxTitle").textContent =
    multi ? (names.length + " items") : CTX.name;
  document.querySelectorAll(".dots.open").forEach(function(d) {{ d.classList.remove("open"); }});
  menu.classList.add("open");
  var mw = menu.offsetWidth || 180;
  var mh = menu.offsetHeight || 200;
  var left = Math.max(8, Math.min(x, window.innerWidth - mw - 8));
  var top = Math.max(8, Math.min(y, window.innerHeight - mh - 8));
  menu.style.left = left + "px";
  menu.style.top = top + "px";
}}

function openRowMenu(e, btn) {{
  stopRowClick(e);
  var row = btn.closest(".frow");
  if (!row || row.classList.contains("parent-row")) return;
  if (!row.classList.contains("selected")) {{
    selectOnly(row);
    LAST_ANCHOR = row;
  }}
  var multi = countChecked() > 1 && row.classList.contains("selected");
  var names = multi ? getCheckedNames() : [row.getAttribute("data-name") || ""];
  CTX = {{
    name: row.getAttribute("data-name") || "",
    kind: +(row.getAttribute("data-kind") || 2),
    multi: multi,
    names: names
  }};
  var menu = document.getElementById("ctxMenu");
  document.getElementById("ctxTitle").textContent =
    multi ? (names.length + " items") : CTX.name;
  document.querySelectorAll(".dots.open").forEach(function(d) {{ d.classList.remove("open"); }});
  btn.classList.add("open");
  menu.classList.add("open");
  var r = btn.getBoundingClientRect();
  var mw = menu.offsetWidth || 180;
  var mh = menu.offsetHeight || 200;
  var left, top;
  if (isMobile()) {{
    left = Math.max(8, Math.min(r.left, window.innerWidth - mw - 8));
    top = Math.max(8, Math.min(r.top, window.innerHeight - mh - 8));
  }} else {{
    left = Math.max(8, Math.min(r.right - mw, window.innerWidth - mw - 8));
    top = r.bottom + 6;
    if (top + mh > window.innerHeight - 8) top = Math.max(8, r.top - mh - 6);
  }}
  menu.style.left = left + "px";
  menu.style.top = top + "px";
  if (e) e.stopPropagation();
}}

function closeRowMenu() {{
  var menu = document.getElementById("ctxMenu");
  if (menu) menu.classList.remove("open");
  document.querySelectorAll(".dots.open").forEach(function(d) {{ d.classList.remove("open"); }});
}}

function ctxAction(action) {{
  if (!CTX) return;
  closeRowMenu();
  if (action === "delete") openDelModal();
  else if (action === "rename") openRenModal();
  else if (action === "move") openDestModal("move");
  else if (action === "copy") openDestModal("copy");
}}

function postApi(api, body) {{
  return fetchWithRetry(location.pathname + "?__api=" + api, {{
    method: "POST",
    headers: {{ "Content-Type": "application/json" }},
    body: JSON.stringify(body)
  }}).then(function(r) {{
    return r.json().then(function(j) {{ return {{s: r.status, j: j}}; }});
  }}).then(function(res) {{
    if (res.j && res.j.pending && res.j.job) {{
      return pollJob(res.j.job, 0).then(function(done) {{ return done; }});
    }}
    return res;
  }});
}}

function fetchWithRetry(url, opts, attempts) {{
  attempts = attempts === undefined ? 3 : attempts;
  return fetch(url, opts).then(function(r) {{
    if (r.status >= 500 && attempts > 1) {{
      return delay(600).then(function() {{ return fetchWithRetry(url, opts, attempts - 1); }});
    }}
    return r;
  }}, function(err) {{
    if (attempts > 1) {{
      return delay(700).then(function() {{ return fetchWithRetry(url, opts, attempts - 1); }});
    }}
    throw err;
  }});
}}

function delay(ms) {{ return new Promise(function(res) {{ setTimeout(res, ms); }}); }}

function pollJob(jobId, attempt) {{
  attempt = attempt || 0;
  if (attempt > 120) {{
    return Promise.resolve({{s: 500, j: {{ok: false, error: "Operation timed out"}}}});
  }}
  return delay(400).then(function() {{
    return fetch(location.pathname + "?__api=jobstatus", {{
      method: "POST",
      headers: {{ "Content-Type": "application/json" }},
      body: JSON.stringify({{ job: jobId }})
    }}).then(function(r) {{
      return r.json().then(function(j) {{ return {{s: r.status, j: j}}; }});
    }}).then(function(res) {{
      if (res.j && res.j.running) return pollJob(jobId, attempt + 1);
      return res;
    }}, function() {{
      return pollJob(jobId, attempt + 1);
    }});
  }});
}}

/* ---- delete ---- */
function openDelModal() {{
  if (!CTX) return;
  var names = CTX.multi ? CTX.names : [CTX.name];
  document.getElementById("delName").textContent =
    CTX.multi ? (names.length + " items") : CTX.name;
  document.getElementById("delKind").textContent =
    CTX.multi ? "Multiple items - permanent"
    : (CTX.kind === 1 ? "Folder - removes all contents" : "File - permanent");
  document.getElementById("delModal").classList.add("open");
}}
function closeDelModal() {{
  document.getElementById("delModal").classList.remove("open");
}}
function confirmDelete() {{
  if (!CTX) return;
  var names = CTX.multi ? CTX.names : [CTX.name];
  var ok = document.getElementById("delOk");
  ok.disabled = true;
  ok.textContent = "...";
  var p = Promise.resolve();
  names.forEach(function(n) {{
    p = p.then(function() {{ return postApi("delete", {{ name: n }}); }});
  }});
  p.then(function(res) {{
    ok.disabled = false;
    ok.textContent = "حذف";
    if (res && res.j && res.j.ok) {{
      toast(names.length > 1 ? ("Deleted " + names.length + " items") : ("Deleted: " + names[0]));
      closeDelModal();
      setTimeout(function() {{ location.reload(); }}, 500);
    }} else toast((res && res.j && res.j.error) || "Delete failed");
  }})
  .catch(function() {{
    ok.disabled = false;
    ok.textContent = "حذف";
    toast("Connection error — retrying...");
    var p2 = Promise.resolve();
    names.forEach(function(n) {{
      p2 = p2.then(function() {{ return postApi("delete", {{ name: n }}); }});
    }});
    p2.then(function(res) {{
      if (res && res.j && res.j.ok) {{
        toast("Deleted");
        closeDelModal();
        setTimeout(function() {{ location.reload(); }}, 500);
      }} else toast((res && res.j && res.j.error) || "Delete failed");
    }}).catch(function() {{ toast("Still offline — check your connection"); }});
  }});
}}

/* ---- rename ---- */
function openRenModal() {{
  if (!CTX) return;
  if (CTX.multi) {{ toast("Rename works on a single item"); return; }}
  document.getElementById("renSub").textContent = CTX.name;
  var input = document.getElementById("renInput");
  input.value = CTX.name;
  document.getElementById("renHint").textContent =
    CTX.kind === 1 ? "Folder" : "Keep the extension for files";
  document.getElementById("renModal").classList.add("open");
  setTimeout(function() {{
    input.focus();
    var dot = CTX.name.lastIndexOf(".");
    if (CTX.kind === 2 && dot > 0) input.setSelectionRange(0, dot);
    else input.select();
  }}, 200);
}}
function closeRenModal() {{
  document.getElementById("renModal").classList.remove("open");
}}
function confirmRename() {{
  if (!CTX) return;
  var input = document.getElementById("renInput");
  var nn = input.value.trim();
  if (!nn) {{ toast("Enter a name"); input.focus(); return; }}
  if (nn.indexOf("/") >= 0 || nn.indexOf("\\\\") >= 0 || nn === "." || nn === "..") {{
    toast("Invalid name"); return;
  }}
  var ok = document.getElementById("renOk");
  ok.disabled = true;
  ok.innerHTML = "...<span class=\\"spinner\\"></span>";
  postApi("rename", {{ name: CTX.name, newName: nn }})
    .then(function(res) {{
      ok.disabled = false;
      ok.textContent = "تایید";
      if (res.j && res.j.ok) {{
        toast("Renamed to: " + res.j.name);
        closeRenModal();
        setTimeout(function() {{ location.reload(); }}, 500);
      }} else toast((res.j && res.j.error) || "Rename failed");
    }})
    .catch(function() {{
      ok.disabled = false;
      ok.textContent = "تایید";
      toast("Connection error — retrying...");
      postApi("rename", {{ name: CTX.name, newName: nn }}).then(function(res) {{
        if (res.j && res.j.ok) {{
          toast("Renamed to: " + res.j.name);
          closeRenModal();
          setTimeout(function() {{ location.reload(); }}, 500);
        }} else toast((res.j && res.j.error) || "Rename failed");
      }}).catch(function() {{ toast("Still offline — check your connection"); }});
    }});
}}

/* ---- move / copy dest picker ---- */
function openDestModal(mode) {{
  if (!CTX) return;
  destMode = mode;
  destPath = "/";
  document.getElementById("destTitle").textContent =
    mode === "move" ? "انتقال به..." : "کپی به...";
  document.getElementById("destSub").textContent =
    CTX.multi ? (CTX.names.length + " items") : CTX.name;
  document.getElementById("destHint").textContent = "مقصد: /";
  var tree = document.getElementById("destTree");
  tree.innerHTML = "<div style=\\"color:var(--text3);font-size:12px;padding:8px\\">Loading...</div>";
  document.getElementById("destModal").classList.add("open");
  fetchWithRetry(location.pathname + "?__api=tree", {{ method: "POST", headers: {{ "Content-Type": "application/json" }}, body: "{{}}" }})
    .then(function(r) {{ return r.json(); }})
    .then(function(j) {{
      tree.innerHTML = "";
      if (j && j.ok && j.tree) renderDestTree(j.tree, tree, 0);
      else tree.innerHTML = "<div style=\\"color:var(--text3);font-size:12px;padding:8px\\">No folders</div>";
    }})
    .catch(function() {{
      tree.innerHTML = "<div style=\\"color:var(--text3);font-size:12px;padding:8px\\">Error — close and try again</div>";
    }});
}}
function renderDestTree(node, el, depth) {{
  var btn = document.createElement("button");
  btn.type = "button";
  btn.className = "dest-opt" + (node.path === destPath ? " on" : "");
  btn.setAttribute("data-path", node.path);
  btn.innerHTML = '<span class="di">&#128193;</span>' +
    (depth ? '<span class="dest-indent"></span>'.repeat(depth) : "") +
    "<span>" + (depth ? node.name : "downloads /") + "</span>";
  btn.onclick = function() {{
    destPath = node.path;
    document.querySelectorAll(".dest-opt").forEach(function(o) {{ o.classList.remove("on"); }});
    btn.classList.add("on");
    document.getElementById("destHint").textContent = "مقصد: " + destPath;
  }};
  el.appendChild(btn);
  (node.dirs || []).forEach(function(ch) {{ renderDestTree(ch, el, depth + 1); }});
}}
function closeDestModal() {{
  document.getElementById("destModal").classList.remove("open");
  destMode = null;
}}
function destErrorText(err, status) {{
  err = String(err || "");
  if (status === 409 || /exist|already/i.test(err)) return "در مقصد وجود دارد — نام دیگری انتخاب کنید یا ابتدا حذف کنید";
  if (status === 400 && /into itself/i.test(err)) return "انتقال/کپی پوشه به درون خودش مجاز نیست";
  if (status === 400 && /invalid destination/i.test(err)) return "مقصد نامعتبر است";
  if (/locked|in use/i.test(err)) return "فایل در برنامه دیگری باز است — آن را ببندید";
  if (status === 404 || /not found/i.test(err)) return "فایل پیدا نشد — صفحه را رفرش کنید";
  return err || "عملیات ناموفق بود";
}}
function confirmDest() {{
  if (!CTX || !destMode) return;
  var ok = document.getElementById("destOk");
  ok.disabled = true;
  ok.innerHTML = "...<span class=\\"spinner\\"></span>";
  var names = CTX.multi ? CTX.names : [CTX.name];
  var p = Promise.resolve();
  var lastRes = null;
  names.forEach(function(n) {{
    p = p.then(function() {{
      return postApi(destMode, {{ name: n, dest: destPath }});
    }}).then(function(res) {{ lastRes = res; }});
  }});
  p.then(function() {{
    ok.disabled = false;
    ok.textContent = "تایید";
    var res = lastRes;
    if (res && res.j && res.j.ok) {{
      toast((destMode === "move" ? "Moved: " : "Copied: ") +
        (names.length > 1 ? (names.length + " items") : names[0]));
      closeDestModal();
      setTimeout(function() {{ location.reload(); }}, 500);
    }} else toast(destErrorText(res && res.j && res.j.error, res && res.s));
  }})
  .catch(function() {{
    ok.disabled = false;
    ok.textContent = "تایید";
    toast("Connection error — retrying...");
    var p2 = Promise.resolve();
    var last2 = null;
    names.forEach(function(n) {{
      p2 = p2.then(function() {{
        return postApi(destMode, {{ name: n, dest: destPath }});
      }}).then(function(res) {{ last2 = res; }});
    }});
    p2.then(function() {{
      if (last2 && last2.j && last2.j.ok) {{
        toast((destMode === "move" ? "Moved: " : "Copied: ") +
          (names.length > 1 ? (names.length + " items") : names[0]));
        closeDestModal();
        setTimeout(function() {{ location.reload(); }}, 500);
      }} else toast(destErrorText(last2 && last2.j && last2.j.error, last2 && last2.s));
    }}).catch(function() {{ toast("Still offline — check your connection"); }});
  }});
}}

/* ---- settings ---- */
function openSettings() {{
  var m = document.getElementById("setModal");
  if (!m) return;
  try {{ syncSettingsUI(); }} catch (e) {{}}
  m.classList.add("open");
}}
function closeSettings() {{
  var m = document.getElementById("setModal");
  if (m) m.classList.remove("open");
}}
function syncSettingsUI() {{
  markThemeUI();
  var v = "list";
  try {{ v = localStorage.getItem("bs-view") || "list"; }} catch (e) {{}}
  document.getElementById("setListV").classList.toggle("on", v !== "grid");
  document.getElementById("setGridV").classList.toggle("on", v === "grid");
}}
function setTheme(t) {{
  if (!isTheme(t)) t = "dark";
  document.documentElement.setAttribute("data-theme", t);
  try {{ localStorage.setItem("bs-theme", t); }} catch (e) {{}}
  markThemeUI();
  syncSettingsUI();
}}
function setPrefView(v) {{
  setView(v);
  syncSettingsUI();
}}
</script>
</body>
</html>
"""
        payload = page.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        accept_enc = (self.headers.get("Accept-Encoding") or "").lower()
        if "gzip" in accept_enc:
            payload = gzip.compress(payload, compresslevel=5)
            self.send_header("Content-Encoding", "gzip")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        return io.BytesIO(payload)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

class QuietThreadingHTTPServer(ThreadingHTTPServer):
    """Threaded HTTP server that swallows benign connection errors.

    Browsers (and Cloudflare tunnel keep-alives) routinely close idle or
    in-flight connections; the worker thread then raises
    ``ConnectionResetError``/``BrokenPipeError`` while reading the next
    request and the default ``handle_error`` would dump a full traceback
    to the console for every dropped connection.
    """

    daemon_threads = True

    def server_bind(self) -> None:
        """Bind without the reverse-DNS lookup ``HTTPServer`` performs.

        ``HTTPServer.server_bind()`` calls ``socket.getfqdn()``, which stalls
        for seconds (or fails outright) when a container has no working DNS,
        so only the plain socket bookkeeping is kept here.
        """
        socketserver.TCPServer.server_bind(self)
        host, port = self.server_address[:2]
        self.server_name = host if host not in ("0.0.0.0", "::", "") else "localhost"
        self.server_port = port

    def handle_error(self, request, client_address) -> None:
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionError, TimeoutError)):
            return
        super().handle_error(request, client_address)


def create_http_server(
    preferred_port: int,
    host: Optional[str] = None,
    exact_port: bool = False,
) -> Tuple[ThreadingHTTPServer, int]:
    """Create the threaded HTTP server bound to a free port.

    *host* defaults to the module ``HOST`` (localhost).  Cloud platforms pass
    ``0.0.0.0`` and ``exact_port=True`` so the server binds the single port the
    platform routed to us instead of silently scanning to another one.
    """
    bind_host_override = (host or "").strip()

    def bind_host() -> str:
        """Interface to listen on: ``--host`` override, else the host default."""
        return bind_host_override or default_bind_host()

    handler = functools.partial(
        DownloadRequestHandler, directory=str(DOWNLOADS_DIR)
    )
    last_error: Optional[OSError] = None

    if exact_port:
        candidates = [preferred_port]
    else:
        candidates = list(range(preferred_port, preferred_port + PORT_SCAN_LIMIT))
        candidates.append(0)

    for port in candidates:
        try:
            httpd = QuietThreadingHTTPServer((bind_host(), port), handler)
        except OSError as exc:
            last_error = exc
            logger.warning("Port %s is not available: %s", port, exc)
            continue
        httpd.daemon_threads = True
        return httpd, httpd.server_address[1]

    raise OSError(f"Could not start the local server: {last_error}")


def print_banner(
    local_url: str,
    public_url: Optional[str],
    files_dir: Path,
) -> None:
    """Print the final, copy-friendly status banner."""
    public_line = public_url or "(tunnel disabled)"
    print()
    print("========================================")
    print("      BLACK SERVER MY SYSTEM")
    print("========================================")
    print()
    print("Local:")
    print(local_url)
    print()
    print("Public:")
    print(public_line)
    print()
    print("Files:")
    print(str(files_dir))
    print()
    print("Status:")
    print("ONLINE")
    print()
    if public_url:
        print("Note:")
        print("Public URL changes every time the server restarts.")
        print("If the page fails to open, copy the Public link again")
        print("from this window (or check that the server is still running).")
        print()
    print("Press Ctrl+C to stop the server.")
    print("========================================")
    print()


def run(args: argparse.Namespace) -> int:
    """Start the file server and (unless disabled) the Cloudflare tunnel."""
    setup_logging(args.verbose)

    # Make sure the required folders exist.
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    BIN_DIR.mkdir(parents=True, exist_ok=True)

    if not DOWNLOADS_DIR.is_dir():
        print(f"[ERROR] downloads directory is missing: {DOWNLOADS_DIR}")
        return 1

    # Resolve the storage backend up front: on a PaaS host a half-written
    # configuration must stop the boot here, not on the first upload.
    if neon is not None:
        try:
            store = storage()
        except Exception as exc:  # noqa: BLE001 - already fatal, reported below
            print(f"[ERROR] storage backend unavailable: {exc}")
            return 1
        if store is not None:
            print(f"[OK] Storage backend: Neon Object Storage + Postgres "
                  f"(bucket={store.bucket})")
        else:
            print("[i] Storage backend: local disk (Neon not configured).")

    # Prevent duplicate instances (irrelevant in the cloud: one process per box).
    lock_acquired = False
    if args.cloud:
        print("[i] Cloud mode: single-instance lock disabled.")
    else:
        acquired, existing_pid = acquire_single_instance_lock()
        if not acquired:
            print("[ERROR] Another instance of the server is already running.")
            print(f"        Existing process id: {existing_pid}")
            print("        Stop it (Ctrl+C in its window) and try again.")
            return 1
        lock_acquired = True

    httpd: Optional[ThreadingHTTPServer] = None
    tunnel: Optional[CloudflareTunnel] = None
    ssh_tunnel: Optional[SshTunnel] = None
    stop_event = threading.Event()

    def _handle_signal(signum, frame):  # noqa: ARG001
        print("\n[i] Shutdown requested, stopping ...")
        stop_event.set()

    # Clean Ctrl+C / termination handling.  SIGBREAK is Windows-only and is
    # delivered when the console window is closed or CTRL_BREAK is pressed.
    signal.signal(signal.SIGINT, _handle_signal)
    for name in ("SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is not None:
            try:
                signal.signal(sig, _handle_signal)
            except (AttributeError, ValueError, OSError):
                pass

    try:
        # 1) Local file server.
        try:
            httpd, port = create_http_server(
                args.port, args.host, exact_port=args.cloud
            )
        except OSError as exc:
            print(f"[ERROR] Could not start the local server: {exc}")
            return 1

        threading.Thread(
            target=httpd.serve_forever, name="http-server", daemon=True
        ).start()
        bound_host = httpd.server_address[0]
        shown_host = "127.0.0.1" if bound_host in ("0.0.0.0", "::", "") else bound_host
        local_url = f"http://{shown_host}:{port}"

        if not wait_for_port(shown_host, port, timeout=10):
            print("[ERROR] Local server did not come up on localhost.")
            return 1
        if not args.cloud and port != args.port:
            print(f"[i] Port {args.port} was busy; using {port} instead.")
        print(f"[OK] Local server listening on {local_url}")
        print(f"[OK] Serving files from {DOWNLOADS_DIR}")

        # 2) Headless hosting on a PaaS host (Render, Replit, ...): the platform
        #    routes traffic to this port and owns the public URL, so there is
        #    nothing left to start.
        if args.cloud:
            external = (os.environ.get("RENDER_EXTERNAL_URL") or "").strip()
            platform_url = f"https://{external}" if external else None
            print("[OK] Headless hosting mode (PaaS host detected).")
            print_banner(local_url, platform_url, DOWNLOADS_DIR)
            if platform_url:
                print("[i] Tunnel disabled: the platform provides the public URL.")
            else:
                print("[i] Tunnel disabled (--no-tunnel). Local access only.")
            while not stop_event.wait(0.5):
                pass
            return 0

        # 3) Tunnel (optional).
        if args.no_tunnel:
            print_banner(local_url, None, DOWNLOADS_DIR)
            print("[i] Tunnel disabled (--no-tunnel). Local access only.")
            while not stop_event.wait(0.5):
                pass
            return 0

        exe = ensure_cloudflared(force_download=args.download_cloudflared)
        if exe is None:
            print("[ERROR] cloudflared is not available; cannot create a tunnel.")
            print("        Local server is still running for a few seconds ...")
            return 1

        if not check_internet():
            print("[!] Warning: no obvious internet connectivity detected.")
            print("    Trying anyway - the Cloudflare edge may still be reachable ...")

        # Multi-protocol startup.
        #
        # cloudflared normally talks to the edge over QUIC (UDP port 7844).
        # A lot of networks (firewalls, ISPs, national filters) block egress
        # UDP while leaving TCP open, which makes QUIC hang forever.  The
        # tunnel therefore starts with the requested protocol and, for the
        # default "auto", transparently retries once over HTTP/2 (TCP).
        startup_protocols = (
            ("auto", "http2") if args.protocol == "auto" else (args.protocol,)
        )
        probe_timeout = min(args.register_timeout, 25)

        tunnel: Optional[CloudflareTunnel] = None
        ssh_tunnel: Optional[SshTunnel] = None
        public_url: Optional[str] = None
        for index, protocol in enumerate(startup_protocols):
            if tunnel is not None:
                tunnel.stop()
            if index > 0:
                print(f"[i] QUIC/UDP to the edge seems blocked; retrying with '{protocol}' ...")
            tunnel = CloudflareTunnel(
                exe, port, LOGS_DIR / "cloudflared.log", protocol=protocol
            )
            try:
                tunnel.start()
            except OSError as exc:
                print(f"[ERROR] Could not start cloudflared: {exc}")
                tunnel = None
                break

            print("[..] Waiting for the public URL from Cloudflare ...")
            public_url = tunnel.wait_for_url(args.tunnel_timeout)
            if tunnel.fatal_error:
                break  # the quick tunnel could not be created at all
            if not public_url:
                continue  # try the next protocol
            if tunnel.wait_for_registration(probe_timeout):
                break  # data plane connected - we are live

        # --- Decide which tunnel is in charge ------------------------------

        if tunnel is None:
            return 1

        # Cloudflare can register yet the public hostname may still be
        # unreachable from this network (DNS block, ISP filter, VPN).  Probe
        # the URL before we announce ONLINE / open the browser; if it fails,
        # fall back to the SSH tunnel providers.
        if public_url and tunnel is not None and tunnel.registered:
            print("[..] Verifying the Cloudflare public URL ...", flush=True)
            if wait_reachable(public_url, tries=3, delay=2.0, timeout=8.0):
                print("[OK] Cloudflare public URL is live.", flush=True)
            elif args.allow_ssh_fallback:
                print("[!] Cloudflare URL is registered but not reachable here.", flush=True)
                print("    This network may block trycloudflare.com (DNS/SNI).", flush=True)
                print("    Switching to the SSH fallback tunnel ...", flush=True)
                tunnel.stop()
                tunnel = None
                public_url = None
                ssh_tunnel, public_url = try_ssh_fallback(
                    port, args.ssh_timeout,
                    reason="Cloudflare URL failed the reachability check.",
                )
                if ssh_tunnel is None:
                    print("[ERROR] SSH fallback also failed.", flush=True)
                    print(f"        Local server still available at: {local_url}", flush=True)
                    return 1
            else:
                print("[!] Could not verify the public URL (SSH fallback disabled).", flush=True)
                print(f"    Local server still available at: {local_url}", flush=True)

        if public_url is None:
            # Cloudflare could not even mint a URL -> explain, then fall back.
            if tunnel.fatal_error:
                print("[ERROR] Cloudflare refused to create a quick tunnel.")
                print(f"        {tunnel.fatal_error}")
                print("        This is usually a DNS or API/firewall issue between")
                print("        this machine and api.trycloudflare.com.")
            else:
                print("[ERROR] Cloudflare could not produce a public URL.")
                print("        The internet or the Cloudflare edge is unreachable.")
            print_log_tail(tunnel.log_path)
            print("        See logs/cloudflared.log for details.")
            tunnel.stop()
            tunnel = None
            if not args.allow_ssh_fallback:
                return 1
            ssh_tunnel, public_url = try_ssh_fallback(port, args.ssh_timeout)
            if ssh_tunnel is None:
                return 1

        elif tunnel is not None and not tunnel.registered:
            # A URL is reserved as soon as the request reaches Cloudflare's API.
            # The public URL is only really usable once the tunnel's data plane
            # has connected to the edge ("Registered tunnel connection").
            print(f"[!] Public URL reserved: {public_url}")
            print("    ...but the tunnel connection to Cloudflare's edge has")
            print("    not been established yet, so the link is not live.")
            print("    This usually means the network blocks cloudflared traffic")
            print("    (e.g. UDP/TCP to port 7844 or *.argotunnel.com).")
            print("    You can force a transport with: --protocol http2")
            print(f"    Local server still available at: {local_url}")
            if args.allow_ssh_fallback:
                print("    Giving the Cloudflare tunnel a short chance to recover ...")
                deadline = time.monotonic() + max(args.register_timeout, 25)
                while time.monotonic() < deadline:
                    if tunnel.registered:
                        break
                    if tunnel.process is not None and tunnel.process.poll() is not None:
                        break
                    time.sleep(0.5)
                if not tunnel.registered:
                    print("    ...still not live - switching to the SSH fallback tunnel.")
                    tunnel.stop()
                    tunnel = None
                    ssh_tunnel, public_url = try_ssh_fallback(port, args.ssh_timeout)
                    if ssh_tunnel is None:
                        return 1
            else:
                print("    Waiting for the tunnel to recover (Ctrl+C to stop) ...")

        if ssh_tunnel is not None:
            # The SSH tunnel URL is typically live within seconds.
            print(f"[OK] SSH tunnel URL: {public_url}")

        # 3) As soon as the tunnel is usable, show the banner, copy the URL and
        #    open the browser.  The server keeps running either way.
        banner_shown = False
        online_url: Optional[str] = public_url if (
            ssh_tunnel is not None or (tunnel is not None and tunnel.registered)
        ) else None

        while not stop_event.wait(0.5):
            if not banner_shown and online_url is None:
                if ssh_tunnel is not None:
                    online_url = public_url  # show banner immediately
                elif tunnel is not None and tunnel.wait_for_registration(0):
                    online_url = public_url
            if not banner_shown and online_url:
                banner_shown = True
                if ssh_tunnel is not None:
                    print(f"[i] Public URL (via SSH fallback): {online_url}")
                print_banner(local_url, online_url, DOWNLOADS_DIR)
                if copy_to_clipboard(online_url):
                    print("[OK] Public URL copied to the clipboard.")
                if not args.no_browser:
                    # Give DNS a moment, then open; failures are non-fatal.
                    time.sleep(0.4)
                    try:
                        webbrowser.open(online_url)
                    except Exception as exc:  # noqa: BLE001 - browser is optional
                        logger.warning("Could not open the browser: %s", exc)
                print("[i] If the browser says 'This site can't be reached',")
                print("    the Public URL above is still the right link — wait")
                print("    a few seconds and refresh, or reopen it in another")
                print("    network/VPN. Quick Tunnel URLs are temporary.")
            if ssh_tunnel is not None:
                if ssh_tunnel.process and ssh_tunnel.process.poll() is not None:
                    print("[ERROR] The SSH tunnel stopped unexpectedly.")
                    print_log_tail(ssh_tunnel.log_path)
                    print(f"        See logs/{ssh_tunnel.log_path.name} for details.")
                    break
            elif tunnel is not None and tunnel.process and tunnel.process.poll() is not None:
                if banner_shown:
                    print("[ERROR] cloudflared stopped unexpectedly.")
                else:
                    print("[ERROR] cloudflared exited before the tunnel was ready.")
                    print("        See logs/cloudflared.log for details.")
                print_log_tail(tunnel.log_path)
                break
        return 0

    finally:
        # Clean shutdown: always terminate the tunnel and the HTTP server.
        if tunnel is not None:
            tunnel.stop()
        if ssh_tunnel is not None:
            ssh_tunnel.stop()
        if httpd is not None:
            httpd.shutdown()
            httpd.server_close()
        if lock_acquired:
            release_single_instance_lock()
        logger.info("Server stopped cleanly.")
        print("[i] Server stopped.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Public file download server - Cloudflare Quick Tunnel with an "
            "automatic SSH fallback tunnel when Cloudflare is blocked."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--port", type=int, default=DEFAULT_PORT,
        help="preferred local port (falls back to a free port if busy)",
    )
    parser.add_argument(
        "--host", default=None,
        help="bind address (default: 127.0.0.1, or 0.0.0.0 in cloud mode)",
    )
    parser.add_argument(
        "--cloud", action="store_true",
        help="platform-as-a-service mode: bind 0.0.0.0, use $PORT, no tunnel",
    )
    parser.add_argument(
        "--no-tunnel", action="store_true",
        help="serve locally only; do not start cloudflared",
    )
    parser.add_argument(
        "--no-browser", action="store_true",
        help="do not open the public URL in the default browser",
    )
    parser.add_argument(
        "--tunnel-timeout", type=int, default=60,
        help="seconds to wait for the public URL",
    )
    parser.add_argument(
        "--register-timeout", type=int, default=30,
        help="seconds to wait for the tunnel data plane to connect",
    )
    parser.add_argument(
        "--protocol", choices=("auto", "quic", "http2"), default="auto",
        help="cloudflared transport protocol to the Cloudflare edge",
    )
    parser.add_argument(
        "--no-ssh-fallback", action="store_true",
        help="disable the automatic SSH fallback tunnel",
    )
    parser.add_argument(
        "--ssh-timeout", type=int, default=int(SSH_STARTUP_TIMEOUT),
        help="seconds to wait for the SSH fallback tunnel URL",
    )
    parser.add_argument(
        "--ensure-cloudflared", action="store_true",
        help="only make sure bin/cloudflared.exe exists, then exit",
    )
    parser.add_argument(
        "--download-cloudflared", action="store_true",
        help="force a fresh download of cloudflared",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="also print debug logs to the console",
    )
    return parser


def main(argv: Optional[list] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.allow_ssh_fallback = not args.no_ssh_fallback

    # Cloud platforms (Render, Replit, ...) announce themselves through
    # RENDER / BS_PAAS / BS_HOST and inject PORT; there the public URL comes from
    # the platform, so the quick tunnel, the browser launch and the local-only
    # port scan are all skipped.
    if PAAS:
        args.cloud = True
    if args.cloud:
        args.cloud = True
        args.host = args.host or (PAAS_HOST if PAAS else "0.0.0.0")
        args.no_tunnel = True
        args.no_browser = True
        args.port = bind_port(args.port)

    if args.ensure_cloudflared or args.download_cloudflared:
        setup_logging(args.verbose)
        BIN_DIR.mkdir(parents=True, exist_ok=True)
        exe = ensure_cloudflared(force_download=args.download_cloudflared)
        return 0 if exe else 1

    try:
        return run(args)
    except KeyboardInterrupt:
        print("\n[i] Interrupted by user.")
        return 0


if __name__ == "__main__":
    sys.exit(main())
