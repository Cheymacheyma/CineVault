"""CineVault - Flask backend for the personal media library.

Three pieces are wired together:

1. **IMDb search** (``GET /search``) - Cinemagoer is the primary provider, with
   automatic fallbacks (IMDb suggestion API, optional OMDb, Wikidata/Wikipedia)
   so the endpoint keeps returning title, rating, plot, cast, runtime and
   poster URL even when a provider is rate-limited or unavailable. Results are
   memoised in-process for ``SEARCH_CACHE_TTL`` seconds.
2. **Real download engine** (``POST /download``) - validates disk headroom,
   enqueues through ``pik_pickle.add_movie_to_queue`` (idempotent, so there are
   never duplicate movies or tasks) and runs a daemon thread that drives a
   ``yt-dlp`` subprocess to fetch the title's trailer/stream. yt-dlp's progress
   output is parsed live and written to SQLite, so the UI renders real transfer
   percentages, speed and ETA; when the transfer completes the resulting file's
   path is stored so the Vault HTML5 player can stream it.
3. **Control + telemetry routes** - ``/pause/<id>``, ``/resume/<id>``,
   ``/delete/<id>``, plus ``/api/speed`` (transfer-rate cap), ``/api/activity``
   (server event feed for toasts) and ``/api/health`` (disk + engine state).

Real-time updates ride on Flask-SocketIO: the download workers broadcast
``queue.update`` / ``activity`` / ``library.changed`` events and every client
receives a ``snapshot`` on connect. If the socket transport is unavailable the
single-page UI transparently falls back to the ``/api/*`` polling endpoints.

Downloads are real files saved under ``CINEVAULT_DOWNLOAD_DIR``. Use this only
for content you are allowed to download (for example the title's official
trailer on YouTube).

Run it with::

    python app.py                # http://127.0.0.1:5000
    flask --app app run          # equivalent

Environment variables
---------------------
``CINEVAULT_DB``       database target (SQLite path or PostgreSQL DSN)
``CINEVAULT_DOWNLOAD_DIR``  where files land, default ``./library``
``CINEVAULT_SEARCH_SUFFIX``  appended to the title when resolving a source
``CINEVAULT_FORMAT``   yt-dlp format selector (default: best mp4)
``CINEVAULT_MAX_CONCURRENT``  how many transfers may run at once, default 2
``CINEVAULT_PROGRESS_INTERVAL``  seconds between DB progress writes, default 0.5
``CINEVAULT_YTDLP_ARGS``  extra space-separated arguments passed to yt-dlp
``CINEVAULT_TRANSCODE``  set ``0`` to skip post-download web conversion
``CINEVAULT_FFMPEG`` / ``CINEVAULT_FFPROBE``  override the ffmpeg/ffprobe binaries
``PIK_HOST`` / ``PIK_PORT``   bind address, default ``127.0.0.1:5000``
``OMDB_API_KEY``       optional key that adds rating/plot/runtime enrichment
``PIK_HTTP_TIMEOUT``   outbound HTTP timeout in seconds, default ``15``
"""

from __future__ import annotations

import atexit
import importlib.util
import itertools
import json
import logging
import os
import pkgutil
import re
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import AbstractSet, Any, Iterator, Optional, Sequence

import requests
from flask import Flask, Response, jsonify, render_template, request
from werkzeug.exceptions import HTTPException

try:  # optional real-time transport; the SPA polls when this is missing.
    from flask_socketio import SocketIO, emit
except ImportError:  # pragma: no cover - environment specific
    SocketIO = None  # type: ignore[assignment]
    emit = None  # type: ignore[assignment]

import pik_pickle as db

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

BASE_DIR = Path(__file__).resolve().parent

APP_NAME = "CineVault"
APP_VERSION = "1.0.0"

#: SQLite file (or PostgreSQL DSN). ``CINEVAULT_DB`` wins, then the legacy
#: ``PIK_PICKLE_DB``, then the default ``cinevault.db`` next to this file.
DB_TARGET: str = (
    os.environ.get("CINEVAULT_DB")
    or os.environ.get("PIK_PICKLE_DB")
    or str(BASE_DIR / "cinevault.db")
)

#: Where downloaded files land (used by the output template and disk guard).
#: Always an absolute path: the ``CINEVAULT_DOWNLOAD_DIR`` override is expanded
#: (``~``) and resolved, and the default sits next to this file so it never
#: depends on the process working directory. Created on startup and again
#: before every transfer (see :func:`_output_template`/:func:`create_app`).
DOWNLOAD_DIR: str = os.path.abspath(
    os.path.expanduser(
        os.environ.get("CINEVAULT_DOWNLOAD_DIR") or str(BASE_DIR / "library")
    )
)

#: How often a control loop wakes up to re-check pause/stop state.
CONTROL_TICK = 0.25

#: --- real download engine (yt-dlp) ---------------------------------------- #
ENGINE_NAME = "yt-dlp"
#: Optional alternative engine: aria2c (magnet links + multi-connection HTTP).
ARIA2C_ENGINE_NAME = "aria2c"
#: Engines a download may request. yt-dlp stays the default.
SUPPORTED_ENGINES: tuple[str, ...] = (ENGINE_NAME, ARIA2C_ENGINE_NAME)

#: Appended to ``<title> (<year>)`` when resolving a downloadable source.
YTDLP_SEARCH_SUFFIX: str = os.environ.get(
    "CINEVAULT_SEARCH_SUFFIX", "official trailer"
)
#: yt-dlp format selector. Prefer a single self-contained mp4 (video+audio in
#: one file, so the common path needs no external muxer), then the best combined
#: stream. ``--merge-output-format mp4`` below forces the container whenever a
#: merge does happen; ``CINEVAULT_FORMAT`` overrides this for higher-quality
#: muxed grabs (e.g. ``bv*[ext=mp4]+ba[ext=m4a]/b``).
YTDLP_FORMAT: str = os.environ.get("CINEVAULT_FORMAT", "best[ext=mp4]/best")
YTDLP_MERGE_FORMAT: str = os.environ.get("CINEVAULT_MERGE_FORMAT", "mp4")
#: Extra space-separated arguments forwarded verbatim to yt-dlp.
YTDLP_EXTRA_ARGS: tuple[str, ...] = tuple(
    os.environ.get("CINEVAULT_YTDLP_ARGS", "").split()
)


def _locate_ffmpeg() -> Optional[str]:
    """Return the absolute path to a usable ffmpeg binary, or ``None``.

    yt-dlp shells out to ffmpeg whenever it has to mux separate video/audio
    streams. If the app is launched from an environment whose ``PATH`` omits
    ffmpeg, such a download fails *after* reporting progress and leaves only
    ``*.part``/``*.fNNN.mp4`` fragments behind — the classic "progress ran but
    no .mp4 appeared" symptom. Pinning the binary we found via
    ``--ffmpeg-location`` makes the merge reliable regardless of ``PATH``.
    """
    override = os.environ.get("CINEVAULT_FFMPEG") or os.environ.get("FFMPEG_BINARY")
    if override and Path(override).is_file():
        return str(Path(override).resolve())
    found = shutil.which("ffmpeg")
    if found:
        return found
    # Windows package managers (winget/choco/scoop) often install off-PATH.
    local = os.environ.get("LOCALAPPDATA")
    if local:
        for pattern in (
            "Microsoft/WinGet/Packages/**/ffmpeg*/**/bin/ffmpeg.exe",
            "Microsoft/WinGet/Links/ffmpeg.exe",
            "Programs/**/ffmpeg*/**/bin/ffmpeg.exe",
        ):
            for candidate in Path(local).glob(pattern):
                if candidate.is_file():
                    return str(candidate)
    return None


#: Absolute path to ffmpeg (or ``None``) forwarded to yt-dlp as ``--ffmpeg-location``.
FFMPEG_LOCATION: Optional[str] = _locate_ffmpeg()


def _locate_ffprobe() -> Optional[str]:
    """Return the absolute path to a usable ``ffprobe`` binary, or ``None``.

    ``ffprobe`` ships alongside ``ffmpeg`` and lets us inspect a finished
    download's codecs: an already-H.264/AAC file only needs a fast lossless
    remux, whereas anything else must be re-encoded. We prefer the copy sitting
    next to the resolved ``ffmpeg`` so the pair always matches, then ``PATH``.
    """
    override = os.environ.get("CINEVAULT_FFPROBE")
    if override and Path(override).is_file():
        return str(Path(override).resolve())
    if FFMPEG_LOCATION:
        sibling = Path(FFMPEG_LOCATION).with_name(
            "ffprobe.exe" if os.name == "nt" else "ffprobe"
        )
        if sibling.is_file():
            return str(sibling)
    return shutil.which("ffprobe")


#: Absolute path to ffprobe (or ``None``) used to choose remux vs. re-encode.
FFPROBE_LOCATION: Optional[str] = _locate_ffprobe()


def _locate_aria2c() -> Optional[str]:
    """Return the absolute path to a usable ``aria2c`` binary, or ``None``.

    The alternate download engine shells out to aria2c for magnet links and
    multi-connection HTTP transfers. Package managers on Windows (winget/choco/
    scoop) frequently install it off ``PATH``, so we probe the same well-known
    locations we use for ffmpeg before giving up.
    """
    override = os.environ.get("CINEVAULT_ARIA2C") or os.environ.get("ARIA2C_BINARY")
    if override and Path(override).is_file():
        return str(Path(override).resolve())
    found = shutil.which("aria2c")
    if found:
        return found
    local = os.environ.get("LOCALAPPDATA")
    if local:
        for pattern in (
            "Microsoft/WinGet/Packages/**/aria2*/**/aria2c.exe",
            "Microsoft/WinGet/Links/aria2c.exe",
            "Programs/**/aria2*/**/aria2c.exe",
        ):
            for candidate in Path(local).glob(pattern):
                if candidate.is_file():
                    return str(candidate)
    return None


#: Absolute path to aria2c (or ``None``) used by the alternate engine.
ARIA2C_LOCATION: Optional[str] = _locate_aria2c()
#: Simultaneous transfers (each transfer is one yt-dlp subprocess).
MAX_CONCURRENT_DOWNLOADS: int = max(
    1, int(os.environ.get("CINEVAULT_MAX_CONCURRENT", "2"))
)
#: Seconds between progress writes to the database (keeps SQLite calm).
PROGRESS_DB_INTERVAL: float = max(
    0.05, float(os.environ.get("CINEVAULT_PROGRESS_INTERVAL", "0.5"))
)

#: --- real-time transport (Flask-SocketIO) --------------------------------- #
#: Set ``CINEVAULT_SOCKETIO=0`` to force the polling fallback.
SOCKETIO_ENABLED: bool = os.environ.get("CINEVAULT_SOCKETIO", "1").lower() not in {
    "0",
    "false",
    "no",
}
#: Any allowed origins for the socket handshake (defaults to same-origin).
SOCKETIO_CORS: str = os.environ.get("CINEVAULT_SOCKETIO_CORS", "")
#: Optional Redis/RabbitMQ URL so several WSGI workers share one socket bus.
SOCKETIO_MESSAGE_QUEUE: Optional[str] = (
    os.environ.get("CINEVAULT_SOCKETIO_QUEUE") or None
)

#: Named speed presets -> yt-dlp ``--limit-rate`` cap (``None`` = uncapped).
SPEED_PRESETS: dict[str, Optional[str]] = {
    "cinema": "1M",    # gentle, keeps the link usable
    "normal": "5M",    # default
    "fast": "20M",
    "turbo": None,     # uncapped
}

#: Ring-buffer size for the in-memory activity feed (toasts + activity panel).
ACTIVITY_MAX = 250

SEARCH_LIMIT_DEFAULT = 8
SEARCH_LIMIT_MAX = 25
ENRICH_LIMIT = 6
SEARCH_CACHE_TTL = 600.0
SEARCH_CACHE_MAX = 128

HTTP_TIMEOUT = float(os.environ.get("PIK_HTTP_TIMEOUT", "15"))
HTTP_HEADERS = {
    "User-Agent": "CineVault/1.0 (personal media library)",
    "Accept-Language": "en-US,en;q=0.9",
}

IMDB_SUGGEST_URL = "https://v3.sg.media-imdb.com/suggestion/x/{query}.json"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
WIKIPEDIA_SUMMARY_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
OMDB_URL = "https://www.omdbapi.com/"

IMDB_ENTITY = "Q37312"  # Wikidata item for IMDb (review-score reviewer)
OMDB_API_KEY = os.environ.get("OMDB_API_KEY", "").strip()

#: Optional fallback clip so the library player stays demonstrable before a
#: real download exists. The engine itself always writes real files.
SAMPLE_VIDEO: Optional[str] = os.environ.get("PIK_SAMPLE_VIDEO") or None

#: Matches review-score strings like ``8.8/10`` or ``74/100``.
_RATING_PATTERN = re.compile(r"(\d+(?:[.,]\d+)?)\s*/\s*(\d+(?:[.,]\d+)?)")

logging.basicConfig(
    level=os.environ.get("PIK_LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
LOGGER = logging.getLogger("cinevault.app")


# --------------------------------------------------------------------------- #
# Activity feed (server-side event stream that drives the UI toasts)
# --------------------------------------------------------------------------- #

_activity_seq = itertools.count(1)
_activity_lock = threading.Lock()
ACTIVITY: deque[dict[str, Any]] = deque(maxlen=ACTIVITY_MAX)

#: The Flask-SocketIO instance, bound later once ``app`` exists. It stays
#: ``None`` when Flask-SocketIO is unavailable, in which case the frontend
#: keeps using the ``/api/activity`` + ``/api/queue`` polling endpoints.
socketio: Any = None


def _broadcast(
    event: str, payload: dict[str, Any], *, namespace: Optional[str] = None
) -> None:
    """Push one real-time event to every connected client (best effort).

    Called from request handlers *and* from the download worker threads, so it
    must never raise into the caller.
    """
    sock = socketio
    if sock is None:
        return
    try:
        sock.emit(event, payload, namespace=namespace)
    except Exception as exc:  # pragma: no cover - transport hiccup
        LOGGER.debug("socket emit %r failed: %s", event, exc)


def _activity_latest() -> int:
    """Highest activity id currently in the ring buffer (0 when empty)."""
    with _activity_lock:
        return ACTIVITY[-1]["id"] if ACTIVITY else 0


def log_activity(
    event: str,
    message: str,
    *,
    level: str = "info",
    movie_id: Optional[int] = None,
    **extra: Any,
) -> dict[str, Any]:
    """Append an event to the ring buffer and return the stored entry.

    The frontend polls ``GET /api/activity`` and turns new entries into
    top-right toasts - and plays the completion chime for
    ``download.completed``.
    """
    with _activity_lock:
        entry: dict[str, Any] = {
            "id": next(_activity_seq),
            "ts": time.time(),
            "event": event,
            "level": level,
            "message": message,
            "movie_id": movie_id,
        }
        if extra:
            entry.update(extra)
        ACTIVITY.append(entry)
    LOGGER.info("[activity] %s: %s", event, message)
    _broadcast("activity", entry)
    return entry


def _speed_payload(preset: Optional[str], rate_limit: Optional[str]) -> dict[str, Any]:
    """Describe the transfer-rate cap applied to new yt-dlp downloads."""
    return {
        "preset": preset,
        "rate_limit": rate_limit,
        "capped": bool(rate_limit),
    }

# --------------------------------------------------------------------------- #
# JSON payload helpers
# --------------------------------------------------------------------------- #


def _payload() -> dict[str, Any]:
    """Merge JSON body, form fields and query string into one dict."""
    merged: dict[str, Any] = dict(request.args)
    form = request.form.to_dict() if request.form else {}
    merged.update(form)
    body = request.get_json(silent=True)
    if isinstance(body, dict):
        merged.update(body)
    return merged


def _int_arg(value: Any, *, name: str, default: Optional[int] = None) -> int:
    if value in (None, ""):
        if default is None:
            raise ValueError(f"'{name}' is required")
        return default
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"'{name}' must be an integer") from exc


def _clip(value: Any, limit: int = 4000) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def _optional_float(value: Any, *, name: str) -> Optional[float]:
    """Parse an optional numeric field, raising a clear error when invalid."""
    if value in (None, ""):
        return None
    try:
        return float(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ValueError(f"'{name}' must be a number") from exc


# --------------------------------------------------------------------------- #
# 1. IMDb search providers
# --------------------------------------------------------------------------- #


def _empty_result() -> dict[str, Any]:
    return {
        "imdb_id": None,
        "title": None,
        "year": None,
        "type": None,
        "rating": None,
        "plot": None,
        "cast": [],
        "runtime": None,
        "poster_url": None,
        "yt_trailer_code": None,
        "sources": [],
    }


def _merge_into(base: dict[str, Any], extra: Optional[dict[str, Any]]) -> None:
    """Fill only the missing fields of ``base`` from ``extra``."""
    if not extra:
        return
    for key, value in extra.items():
        if key == "sources":
            for source in value:
                if source not in base["sources"]:
                    base["sources"].append(source)
        elif key == "cast":
            if value and not base.get("cast"):
                base["cast"] = list(value)
        elif value not in (None, "", []) and base.get(key) in (None, "", []):
            base[key] = value


# -- Cinemagoer (primary provider) ------------------------------------------ #

_cinemagoer_lock = threading.Lock()
_cinemagoer: Any = None
_cinemagoer_ready = False


def _install_pkgutil_shim() -> None:
    """Cinemagoer still imports ``pkgutil.find_loader`` (removed in 3.12+)."""
    if not hasattr(pkgutil, "find_loader"):
        def find_loader(name: str) -> Any:
            spec = importlib.util.find_spec(name)
            return None if spec is None else spec.loader

        pkgutil.find_loader = find_loader  # type: ignore[attr-defined]


def _get_cinemagoer() -> Any:
    """Return a Cinemagoer client, or ``None`` when not installed/usable."""
    global _cinemagoer, _cinemagoer_ready
    with _cinemagoer_lock:
        if _cinemagoer_ready:
            return _cinemagoer
        _cinemagoer_ready = True
        try:
            _install_pkgutil_shim()
            from imdb import Cinemagoer
        except Exception as exc:  # pragma: no cover - depends on environment
            LOGGER.warning("Cinemagoer unavailable (%s); using fallback providers", exc)
            _cinemagoer = None
        else:
            _cinemagoer = Cinemagoer()
            LOGGER.info("Cinemagoer provider ready")
        return _cinemagoer


def _minutes_text(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if text.isdigit():
        return f"{text} min"
    return text


def _search_cinemagoer(query: str, limit: int) -> list[dict[str, Any]]:
    ia = _get_cinemagoer()
    if ia is None:
        return []

    try:
        matches = list(ia.search_movie(query))[: max(limit, 1) * 2]
    except Exception as exc:  # pragma: no cover - network dependent
        LOGGER.warning("Cinemagoer search failed: %s", exc)
        return []

    results: list[dict[str, Any]] = []
    for match in matches:
        try:
            ia.update(match, info=["plot", "cast", "runtime", "rating"])
        except Exception:  # pragma: no cover - partial data is fine
            pass

        movie_id = str(match.movieID)
        item = _empty_result()
        item["imdb_id"] = f"tt{int(movie_id):07d}" if movie_id.isdigit() else movie_id
        item["title"] = match.get("title")
        item["year"] = match.get("year")
        item["rating"] = _as_float(match.get("rating"))
        item["cast"] = [str(person) for person in (match.get("cast") or [])][:10]
        item["poster_url"] = match.get("cover url") or match.get("full-size cover url")
        runtimes = match.get("runtimes") or []
        item["runtime"] = _minutes_text(runtimes[0]) if runtimes else None
        plot = match.get("plot")
        if isinstance(plot, (list, tuple)):
            plot = plot[0] if plot else None
        item["plot"] = _clip(plot)
        item["sources"] = ["cinemagoer"]
        results.append(item)
    return results


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    text = str(value).strip().replace(",", ".")
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


# -- Shared HTTP helper ----------------------------------------------------- #

#: Wikidata / Wikipedia start returning 429 if we hammer them, which is exactly
#: what a 25-title discovery warm-up does. Requests to these hosts are spaced by
#: ``HTTP_MIN_INTERVAL`` seconds so the burst stays polite (and keeps its data).
POLITE_HOSTS = ("wikidata.org", "wikipedia.org")
HTTP_MIN_INTERVAL = float(os.environ.get("PIK_HTTP_MIN_INTERVAL", "0.5"))
HTTP_RETRY_AFTER_MAX = float(os.environ.get("PIK_HTTP_RETRY_AFTER_MAX", "5"))

_http_gate = threading.Lock()
_http_last_call: dict[str, float] = {}


def _throttle(url: str) -> None:
    """Space requests to the politely rate-limited hosts, across all threads.

    Each caller atomically reserves the next free slot for the host, then sleeps
    until it comes up. Reserving before sleeping (rather than checking the last
    call time) is what actually keeps concurrent threads from bursting through
    together and tripping a 429.
    """
    host = urllib.parse.urlparse(url).netloc.lower()
    if not any(host.endswith(suffix) for suffix in POLITE_HOSTS):
        return
    with _http_gate:
        now = time.time()
        slot = max(now, _http_last_call.get(host, 0.0) + HTTP_MIN_INTERVAL)
        _http_last_call[host] = slot
        delay = slot - now
    if delay > 0:
        time.sleep(delay)


def _retry_delay(response: Any, attempt: int, backoff: float) -> float:
    """Prefer a server-supplied ``Retry-After`` over the local backoff."""
    header = response.headers.get("Retry-After") if response is not None else None
    delay = _as_float(header)
    if delay is None:
        delay = backoff * (attempt + 1)
    return max(0.1, min(delay, HTTP_RETRY_AFTER_MAX))


def _get_json(
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    timeout: Optional[float] = None,
    retries: int = 3,
    backoff: float = 0.8,
) -> Any:
    """GET a JSON document, politely retrying rate limits (429) and 5xx errors."""
    last: Optional[Exception] = None
    response: Any = None
    for attempt in range(retries + 1):
        try:
            _throttle(url)
            response = requests.get(
                url,
                params=params,
                headers=HTTP_HEADERS,
                timeout=timeout or HTTP_TIMEOUT,
            )
            if response.status_code in {429, 500, 502, 503, 504} and attempt < retries:
                time.sleep(_retry_delay(response, attempt, backoff))
                continue
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last = exc
            if attempt < retries:
                time.sleep(_retry_delay(response, attempt, backoff))
                continue
            break
    raise last if last is not None else RuntimeError(f"request failed: {url}")


# -- IMDb suggestion API (keyless fallback) --------------------------------- #


def _search_suggestion(
    query: str, limit: int, timeout: Optional[float] = None
) -> list[dict[str, Any]]:
    url = IMDB_SUGGEST_URL.format(query=urllib.parse.quote(query))
    try:
        response = requests.get(url, headers=HTTP_HEADERS, timeout=timeout or HTTP_TIMEOUT)
        response.raise_for_status()
        items = response.json().get("d", [])
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("IMDb suggestion lookup failed: %s", exc)
        return []

    results: list[dict[str, Any]] = []
    for item in items[:limit]:
        imdb_id = item.get("id")
        if not imdb_id or not str(imdb_id).startswith("tt"):
            continue
        poster = (item.get("i") or {}).get("imageUrl")
        stars = [name.strip() for name in (item.get("s") or "").split(",") if name.strip()]
        results.append(
            {
                "imdb_id": imdb_id,
                "title": item.get("l"),
                "year": item.get("y"),
                "type": item.get("qid") or item.get("q"),
                "rating": None,
                "plot": None,
                "cast": stars,
                "runtime": None,
                "poster_url": poster,
                "yt_trailer_code": None,
                "sources": ["imdb-suggestion"],
            }
        )
    return results


# -- Enrichment: OMDb (when a key is configured) ---------------------------- #


def _enrich_omdb(imdb_id: str) -> Optional[dict[str, Any]]:
    if not OMDB_API_KEY:
        return None
    try:
        response = requests.get(
            OMDB_URL,
            params={"apikey": OMDB_API_KEY, "i": imdb_id, "plot": "short"},
            headers=HTTP_HEADERS,
            timeout=HTTP_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("OMDb lookup failed for %s: %s", imdb_id, exc)
        return None

    if str(data.get("Response")) != "True":
        return None

    enriched: dict[str, Any] = {"sources": ["omdb"]}
    if data.get("Rating") not in (None, "", "N/A"):
        enriched["rating"] = _as_float(data["Rating"])
    if data.get("imdbRating") not in (None, "", "N/A"):
        enriched["rating"] = enriched.get("rating") or _as_float(data["imdbRating"])
    if data.get("Plot") not in (None, "", "N/A"):
        enriched["plot"] = _clip(data["Plot"])
    if data.get("Actors") not in (None, "", "N/A"):
        enriched["cast"] = [name.strip() for name in data["Actors"].split(",") if name.strip()]
    if data.get("Runtime") not in (None, "", "N/A"):
        enriched["runtime"] = _clip(data["Runtime"])
    if data.get("Poster") not in (None, "", "N/A"):
        enriched["poster_url"] = data["Poster"]
    return enriched


# -- Enrichment: Wikidata + Wikipedia (keyless) ----------------------------- #


def _claim_number(claims: dict[str, Any], pid: str) -> Optional[float]:
    try:
        value = claims[pid][0]["mainsnak"]["datavalue"]["value"]
    except (KeyError, IndexError, TypeError):
        return None
    if isinstance(value, dict):
        return _as_float(str(value.get("amount", "")).lstrip("+"))
    return _as_float(value)


def _imdb_review_score(claims: dict[str, Any]) -> Optional[float]:
    """Read P444 (review score) and normalise it to a 0-10 scale.

    Wikidata stores these as free-text strings such as ``"8.8/10"`` (IMDb,
    ``Q37312``) or ``"87%"`` (Rotten Tomatoes), so the IMDb-qualified entry is
    preferred and anything else is only used as a last resort.
    """
    fallback: Optional[float] = None
    for statement in claims.get("P444", []):
        try:
            raw = statement["mainsnak"]["datavalue"]["value"]
        except (KeyError, TypeError):
            continue

        if isinstance(raw, dict):
            score = _as_float(str(raw.get("amount", "")).lstrip("+"))
            scale = 10.0
        else:
            match = _RATING_PATTERN.search(str(raw))
            if match is None:
                continue
            score = _as_float(match.group(1))
            scale = _as_float(match.group(2)) or 10.0
        if score is None or not scale:
            continue

        normalised = round(score * (10.0 / scale), 1)
        reviewers = statement.get("qualifiers", {}).get("P447", [])
        reviewer_ids = {
            str(item.get("datavalue", {}).get("value", {}).get("id", "")).upper()
            for item in reviewers
        }
        if IMDB_ENTITY in reviewer_ids:
            return normalised
        if not reviewer_ids and fallback is None:
            fallback = normalised
    return fallback


def _enrich_wikidata(imdb_id: str) -> Optional[dict[str, Any]]:
    try:
        search = _get_json(
            WIKIDATA_API,
            params={
                "action": "query",
                "list": "search",
                "srsearch": f'haswbstatement:"P345={imdb_id}"',
                "format": "json",
            },
        )
        hits = search.get("query", {}).get("search", [])
        if not hits:
            return None
        qid = hits[0]["title"]

        entity = _get_json(WIKIDATA_ENTITY_URL.format(qid=qid)).get("entities", {}).get(qid, {})
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("Wikidata lookup failed for %s: %s", imdb_id, exc)
        return None

    claims = entity.get("claims", {})
    enriched: dict[str, Any] = {"sources": ["wikidata"]}

    minutes = _claim_number(claims, "P2047")
    if minutes:
        enriched["runtime"] = f"{int(round(minutes))} min"

    score = _imdb_review_score(claims)
    if score is not None:
        enriched["rating"] = score

    sitelink = (entity.get("sitelinks") or {}).get("enwiki", {}).get("title")
    if sitelink:
        try:
            summary = _get_json(
                WIKIPEDIA_SUMMARY_URL.format(
                    title=urllib.parse.quote(sitelink.replace(" ", "_"))
                )
            )
            extract = summary.get("extract")
            if extract:
                enriched["plot"] = _clip(extract)
                enriched["sources"].append("wikipedia")
        except (requests.RequestException, ValueError) as exc:
            LOGGER.warning("Wikipedia summary failed for %s: %s", sitelink, exc)

    return enriched if len(enriched) > 1 else None


# -- Search orchestration (cached) ------------------------------------------ #

_cache: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
_cache_lock = threading.Lock()


def _cache_get(key: tuple[str, int]) -> Optional[dict[str, Any]]:
    with _cache_lock:
        entry = _cache.get(key)
        if entry is None:
            return None
        stored_at, value = entry
        if time.time() - stored_at > SEARCH_CACHE_TTL:
            _cache.pop(key, None)
            return None
        return value


def _cache_put(key: tuple[str, int], value: dict[str, Any]) -> None:
    with _cache_lock:
        if len(_cache) >= SEARCH_CACHE_MAX:
            oldest = min(_cache.items(), key=lambda item: item[1][0])[0]
            _cache.pop(oldest, None)
        _cache[key] = (time.time(), value)


def _enrich_result(result: dict[str, Any]) -> dict[str, Any]:
    imdb_id = result.get("imdb_id")
    if imdb_id and OMDB_API_KEY:
        _merge_into(result, _enrich_omdb(imdb_id))
        if result["rating"] is not None and result["plot"] is not None:
            return _attach_trailer(result)
    if imdb_id:
        _merge_into(result, _enrich_wikidata(imdb_id))
    return _attach_trailer(result)


def _attach_trailer(result: dict[str, Any]) -> dict[str, Any]:
    """Best-effort ``yt_trailer_code`` for a finished movie record.

    The trailer is the one field no metadata provider here supplies, so it is
    resolved from YTS (cached in :func:`_trailer_code`) and simply left
    ``None`` when it cannot be found.
    """
    if result.get("yt_trailer_code"):
        return result
    try:
        code = _trailer_code(result.get("title"), result.get("year"))
    except Exception as exc:  # pragma: no cover - defensive; never break search
        LOGGER.warning("trailer lookup failed for %r: %s", result.get("title"), exc)
        code = None
    if code:
        result["yt_trailer_code"] = code
    return result


def search_movies(query: str, limit: int = SEARCH_LIMIT_DEFAULT) -> dict[str, Any]:
    """Search IMDb and return structured, enriched movie records."""
    query = query.strip()
    if not query:
        raise ValueError("'q' (movie title) is required")

    cached = _cache_get((query.lower(), limit))
    if cached is not None:
        return {**cached, "cached": True}

    results = _search_suggestion(query, limit)
    cinemagoer_results = _search_cinemagoer(query, limit)

    if not results:
        results = cinemagoer_results[:limit]
    elif cinemagoer_results:
        by_id = {item["imdb_id"]: item for item in cinemagoer_results}
        for result in results:
            _merge_into(result, by_id.get(result["imdb_id"]))

    results = results[:limit]

    if results:
        with ThreadPoolExecutor(max_workers=min(4, len(results))) as pool:
            futures = {
                pool.submit(_enrich_result, result): result
                for result in results[:ENRICH_LIMIT]
            }
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:  # pragma: no cover - defensive
                    LOGGER.warning("Enrichment failed: %s", exc)

    payload = {
        "query": query,
        "limit": limit,
        "count": len(results),
        "providers": ["cinemagoer", "imdb-suggestion"]
        + (["omdb"] if OMDB_API_KEY else [])
        + ["wikidata", "wikipedia"],
        "results": results,
    }
    _cache_put((query.lower(), limit), payload)
    return {**payload, "cached": False}


# --------------------------------------------------------------------------- #
# 1b. Discovery - curated homepage rows (hero + Trending + Latest Releases)
# --------------------------------------------------------------------------- #

#: How long a resolved homepage payload stays warm, and the wall-clock budget
#: for the (concurrent) provider lookups behind a cold build.
DISCOVER_TTL = float(os.environ.get("CINEVAULT_DISCOVER_TTL", "3600"))
DISCOVER_BUDGET = float(os.environ.get("CINEVAULT_DISCOVER_BUDGET", "90"))
DISCOVER_LOOKUP_TIMEOUT = float(os.environ.get("CINEVAULT_DISCOVER_TIMEOUT", "6"))

#: The resolved homepage is persisted here so a restart can serve the full
#: hero/rows instantly (last run's data) while a polite refresh runs in the
#: background, instead of blocking the first visitor behind provider lookups.
DISCOVER_CACHE_FILE = os.environ.get(
    "CINEVAULT_DISCOVER_CACHE",
    str(Path(__file__).resolve().with_name("discover_cache.json")),
)

#: Curated titles for the streaming-style homepage. They are resolved live
#: through the same provider chain as /search, so nothing is hard-coded except
#: which titles to feature.
FEATURED_TITLE: dict[str, Any] = {
    "title": "Dune: Part Two",
    "year": "2024",
    "tagline": "Long live the fighters.",
}
TRENDING_TITLES: list[dict[str, Any]] = [
    {"title": "Oppenheimer", "year": "2023"},
    {"title": "The Batman", "year": "2022"},
    {"title": "Top Gun: Maverick", "year": "2022"},
    {"title": "Interstellar", "year": "2014"},
    {"title": "Inception", "year": "2010"},
    {"title": "The Dark Knight", "year": "2008"},
    {"title": "Parasite", "year": "2019"},
    {"title": "Everything Everywhere All at Once", "year": "2022"},
    {"title": "Blade Runner 2049", "year": "2017"},
    {"title": "Mad Max: Fury Road", "year": "2015"},
    {"title": "Spider-Man: Across the Spider-Verse", "year": "2023"},
    {"title": "Whiplash", "year": "2014"},
]
LATEST_TITLES: list[dict[str, Any]] = [
    {"title": "Deadpool & Wolverine", "year": "2024"},
    {"title": "Gladiator II", "year": "2024"},
    {"title": "Inside Out 2", "year": "2024"},
    {"title": "Furiosa: A Mad Max Saga", "year": "2024"},
    {"title": "Kingdom of the Planet of the Apes", "year": "2024"},
    {"title": "Alien: Romulus", "year": "2024"},
    {"title": "The Wild Robot", "year": "2024"},
    {"title": "Conclave", "year": "2024"},
    {"title": "Nosferatu", "year": "2024"},
    {"title": "Anora", "year": "2024"},
    {"title": "Wicked", "year": "2024"},
    {"title": "The Substance", "year": "2024"},
]

_discover_cache: dict[str, Any] = {}
_discover_lock = threading.Lock()
_build_lock = threading.Lock()
_discover_built_at: float = 0.0
_discover_loaded: bool = False
_discover_refreshing: bool = False


def _ensure_discover_loaded() -> None:
    """Pull last run's persisted homepage into memory (once)."""
    global _discover_loaded, _discover_built_at
    if _discover_loaded:
        return
    with _discover_lock:
        if _discover_loaded:
            return
        _discover_loaded = True
        try:
            with open(DISCOVER_CACHE_FILE, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            return
        except (OSError, ValueError) as exc:
            LOGGER.warning("Could not read discovery cache: %s", exc)
            return
        if isinstance(payload, dict) and payload.get("hero") and payload.get("rows"):
            _discover_cache.clear()
            _discover_cache.update(payload)
            _discover_built_at = float(payload.get("generated_at") or 0.0)
            LOGGER.info("Loaded homepage discovery from %s", DISCOVER_CACHE_FILE)


def _store_discover(payload: dict[str, Any]) -> None:
    """Publish a fresh payload in memory and persist it for the next start."""
    global _discover_built_at
    with _discover_lock:
        _discover_cache.clear()
        _discover_cache.update(payload)
        _discover_built_at = float(payload.get("generated_at") or time.time())
    tmp = f"{DISCOVER_CACHE_FILE}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp, DISCOVER_CACHE_FILE)
    except OSError as exc:  # pragma: no cover - defensive
        LOGGER.warning("Could not persist discovery cache: %s", exc)


def _refresh_discover_async() -> None:
    """Rebuild the homepage in the background (one at a time)."""
    global _discover_refreshing
    with _discover_lock:
        if _discover_refreshing:
            return
        _discover_refreshing = True

    def _run() -> None:
        global _discover_refreshing
        try:
            with _build_lock:
                _store_discover(_build_discover())
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning("Discovery refresh failed: %s", exc)
        finally:
            with _discover_lock:
                _discover_refreshing = False

    threading.Thread(target=_run, name="discover-refresh", daemon=True).start()


def _title_key(text: Any) -> str:
    """Normalise a title for fuzzy comparison (lowercase alphanumerics only)."""
    return re.sub(r"[^a-z0-9]+", "", str(text or "").lower())


def _fallback_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """A renderable record for a title the providers could not resolve."""
    return {
        "imdb_id": None,
        "title": entry.get("title"),
        "year": entry.get("year"),
        "type": "movie",
        "rating": None,
        "plot": None,
        "cast": [],
        "runtime": None,
        "poster_url": None,
        "yt_trailer_code": None,
        "sources": [],
        "tagline": entry.get("tagline"),
    }


def _pick_best_match(
    candidates: list[dict[str, Any]], title: str, year: Any
) -> Optional[dict[str, Any]]:
    """Choose the suggestion that best matches a curated title/year pair."""
    want = _title_key(title)
    want_year = str(year or "").strip()

    def score(item: dict[str, Any]) -> int:
        name = _title_key(item.get("title"))
        points = 0
        if name and name == want:
            points += 100
        elif want and name.startswith(want):
            points += 60
        elif want and want in name:
            points += 30
        if want_year and str(item.get("year") or "").strip() == want_year:
            points += 45
        if str(item.get("type") or "").lower() in {"movie", "feature", "tvmovie"}:
            points += 10
        return points

    if not candidates:
        return None
    best = max(candidates, key=score)
    return None if score(best) <= 0 else best


def _resolve_catalog_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Resolve one curated title into a full movie record. Never raises."""
    title = str(entry.get("title") or "").strip()
    try:
        candidates = _search_suggestion(
            title, 6, timeout=min(HTTP_TIMEOUT, DISCOVER_LOOKUP_TIMEOUT)
        )
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("Discover lookup failed for %r: %s", title, exc)
        return _fallback_entry(entry)

    best = _pick_best_match(candidates, title, entry.get("year"))
    if best is None:
        return _fallback_entry(entry)
    try:
        _enrich_result(best)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("Discover enrichment failed for %r: %s", title, exc)
    best["tagline"] = entry.get("tagline")
    return best


def _discover_fresh() -> bool:
    """Whether the memoised discovery payload is still within its TTL."""
    return bool(_discover_cache) and time.time() - _discover_built_at < DISCOVER_TTL


def _build_discover() -> dict[str, Any]:
    """Resolve every curated title - the slow, network-bound discovery work."""
    entries = [FEATURED_TITLE, *TRENDING_TITLES, *LATEST_TITLES]
    resolved: dict[int, dict[str, Any]] = {}
    pool = ThreadPoolExecutor(max_workers=min(4, len(entries)))
    try:
        futures = {
            pool.submit(_resolve_catalog_entry, entry): index
            for index, entry in enumerate(entries)
        }
        done, pending = wait(list(futures), timeout=DISCOVER_BUDGET)
        for future in pending:  # ran out of budget - render a plain fallback
            future.cancel()
        for future in done:
            index = futures[future]
            try:
                resolved[index] = future.result()
            except Exception:  # pragma: no cover - defensive
                resolved[index] = _fallback_entry(entries[index])
    finally:
        pool.shutdown(wait=False)

    ordered = [
        resolved.get(index) or _fallback_entry(entries[index])
        for index in range(len(entries))
    ]
    hero = ordered[0]
    trending = [
        movie
        for movie in ordered[1 : 1 + len(TRENDING_TITLES)]
        if not (hero.get("imdb_id") and movie.get("imdb_id") == hero.get("imdb_id"))
    ]
    latest = ordered[1 + len(TRENDING_TITLES) :]

    payload: dict[str, Any] = {
        "hero": hero,
        "rows": [
            {
                "key": "trending",
                "title": "Trending / Most Watched",
                "subtitle": "What everyone is queuing right now",
                "movies": trending,
            },
            {
                "key": "latest",
                "title": "Latest Releases",
                "subtitle": "Fresh on the shelves",
                "movies": latest,
            },
        ],
        "generated_at": time.time(),
    }
    return payload


def discover_home(force: bool = False) -> dict[str, Any]:
    """Featured movie + homepage rows, resolved concurrently and cached.

    Returns ``hero`` and ``rows`` (Trending / Most Watched, Latest Releases).

    A warm payload is served straight from memory; a stale one is served too,
    but a single polite refresh is kicked off in the background so the homepage
    is never blocked behind provider lookups. Only a truly cold start (no
    persisted payload) builds synchronously. Builds are serialised so a burst of
    callers cannot hammer Wikidata into a 429 storm.
    """
    _ensure_discover_loaded()
    with _discover_lock:
        have_cache = bool(_discover_cache)
        fresh = _discover_fresh()
        snapshot = {**_discover_cache} if have_cache else None
    if have_cache and not force:
        if not fresh:
            _refresh_discover_async()
        return {**snapshot, "cached": True}
    with _build_lock:
        with _discover_lock:  # another thread may have built while we waited
            if _discover_cache and not force:
                return {**_discover_cache, "cached": True}
        payload = _build_discover()
        _store_discover(payload)
        return {**payload, "cached": False}


# --------------------------------------------------------------------------- #
# 2. Real download engine (yt-dlp subprocesses)
# --------------------------------------------------------------------------- #

#: Matches a yt-dlp progress line, e.g.
#: ``[download]  42.3% of 12.34MiB at 3.10MiB/s ETA 00:05``.
_YTDLP_PROGRESS_RE = re.compile(
    r"^\[download\]\s+(?P<pct>\d+(?:\.\d+)?)%"
    r"(?:\s+of\s+(?P<total>~?\s?[\d.]+\s?[KMGTP]?i?B))?"
    r"(?:\s+at\s+(?P<speed>[\d.]+\s?[KMGTP]?i?B/s))?"
    r"(?:\s+ETA\s+(?P<eta>\d+(?::\d{2}){0,2}))?",
    re.IGNORECASE,
)
_YTDLP_DEST_RE = re.compile(r"^\[download\]\s+Destination:\s+(?P<path>.+)$")
_YTDLP_MERGE_RE = re.compile(
    r"^(?:\[Merger\]|\[VideoMerge\]|\[FixupM3u8\])\s+.*?[\"'](?P<path>[^\"']+)[\"']"
)
_SAFE_NAME_RE = re.compile(r"[^\w\-. ()]+", re.UNICODE)

#: File extensions we accept as a finished local media file.
_MEDIA_SUFFIXES = {".mp4", ".mkv", ".webm", ".mov", ".m4v", ".avi", ".flv", ".ts"}

#: Containers the aria2c worker accepts as a finished transfer. A torrent or
#: HTTP download that lands on anything else (.torrent sidecars, .rar parts,
#: .nfo, HTML error pages, ...) is a failure, not a completed movie.
ARIA2C_VIDEO_SUFFIXES: frozenset[str] = frozenset({".mp4", ".mkv", ".avi"})

#: Size floor for accepting an aria2c output as a real feature (100 MB).
#: Anything smaller is a sample, artwork or bundled metadata.
ARIA2C_MIN_VIDEO_BYTES = 100 * 1024 * 1024

#: Sub-directory of the download root holding aria2c's own resume state (the
#: ``--save-session`` file). Kept out of the media tree so a resume file is
#: never mistaken for a download, and so a paused transfer can be replayed.
ARIA2C_STATE_DIR = os.environ.get("CINEVAULT_ARIA2C_STATE_DIR", ".cinevault")

#: Seconds a transfer may take to stop *gracefully* before it is force-killed.
#: Long enough for aria2c to flush its control file -- which is what makes a
#: resume continue from the last verified piece instead of 0%.
ARIA2C_STOP_GRACE = max(1.0, float(os.environ.get("CINEVAULT_ARIA2C_GRACE", "8")))

#: "Stream while downloading": how much of the head of each torrent file
#: aria2c gives top priority to, so the first bytes of the movie land first and
#: playback can start before the whole transfer finishes. The default (32 MiB)
#: is a few minutes of 1080p video -- enough of a head start that the player
#: rarely catches up to the download. Override with ``CINEVAULT_STREAM_HEAD``.
STREAM_HEAD_PRIORITY = os.environ.get("CINEVAULT_STREAM_HEAD", "32M")

#: How long ``/stream`` will hold a request open waiting for the next bytes of a
#: still-growing file before answering ``416`` (seconds). This is what lets an
#: HTML5 player glide over the gap between "player wants byte N" and "aria2c
#: just wrote byte N" instead of stalling. Set ``CINEVAULT_STREAM_WAIT=0`` to
#: disable the wait (tests do this for determinism).
STREAM_WAIT_TIMEOUT = float(os.environ.get("CINEVAULT_STREAM_WAIT", "12"))

#: Poll interval while waiting for a growing file (seconds).
STREAM_WAIT_INTERVAL = 0.25

#: BitTorrent trackers handed to every aria2c transfer through ``--bt-tracker``.
#: A magnet only carries the announce list its publisher embedded, and for YTS
#: releases that is routinely one UDP tracker that no longer answers. When it is
#: dead the transfer never meets a peer and sits at 0 B/s until it gives up.
#: Measured against a magnet with no trackers at all: 5 peer connections without
#: this list, 44 with it -- so this, not the DHT node, is what gets a download
#: moving. Trackers go offline constantly, which is why several are listed.
DEFAULT_BITTORRENT_TRACKERS: tuple[str, ...] = (
    "udp://tracker.opentrackr.org:1337/announce",
    "udp://tracker.openbittorrent.com:80/announce",
    "udp://open.demonii.com:1337/announce",
    "udp://exodus.desync.com:6969/announce",
    "udp://tracker.torrent.eu.org:451/announce",
)

#: IPv4 DHT bootstrap node, overridable for a network where the default is
#: unreachable. aria2 accepts exactly *one* ``--dht-entry-point``: repeating the
#: flag overwrites the previous value and a comma-separated value is rejected
#: outright (aria2c then exits with code 28 before its RPC ever comes up). Extra
#: peer sources therefore come from :data:`DEFAULT_BITTORRENT_TRACKERS`, not
#: from additional DHT nodes. After one successful run aria2 also caches its
#: routing table in ``dht.dat``, so later runs start already populated.
DHT_ENTRY_POINT = os.environ.get(
    "CINEVAULT_DHT_ENTRY_POINT", "dht.transmissionbt.com:6881"
)

#: Video *sites* that only serve a player page. aria2c speaks HTTP/BitTorrent
#: only, so pointed at one of these it downloads the page's HTML -- saved as a
#: file named after the last path segment, i.e. a file literally called
#: ``watch`` that looks like a finished movie.
_ARIA2C_PAGE_HOSTS = ("youtube.com", "youtu.be")


def _player_page_host(url: str) -> Optional[str]:
    """Return ``url``'s host when it is a video *site* player page, else ``None``.

    These hosts never expose a media file; the only way to reach the video is
    yt-dlp's extractor. Direct media URLs (including googlevideo
    ``videoplayback`` links) are plain HTTP and therefore return ``None``.
    """
    try:
        host = (urllib.parse.urlsplit(url.strip()).hostname or "").lower().rstrip(".")
    except ValueError:  # malformed URL: let the engine itself report it
        return None
    if not host:
        return None
    if any(host == page or host.endswith("." + page) for page in _ARIA2C_PAGE_HOSTS):
        return host
    return None


def _aria2c_unsupported_target(url: str) -> Optional[str]:
    """Return why ``url`` cannot be fetched by aria2c, or ``None``.

    Last line of defence. Routing keeps player pages away from aria2c; this
    refuses any that still get through, e.g. a queued row resumed on a later
    run where the URL was stored before the routing rule existed.
    """
    host = _player_page_host(url)
    if not host:
        return None
    return (
        f"aria2c cannot extract video from {host} - it would only save the "
        "player page as HTML. Use the yt-dlp engine for this link"
    )


# --------------------------------------------------------------------------- #
# YTS magnet pipeline -- the aria2c engine's only automatic source
# --------------------------------------------------------------------------- #

#: YTS mirrors, probed in order. They rotate in and out of Cloudflare's good
#: graces, so any one going down must not take the torrent pipeline with it.
YTS_DOMAINS: tuple[str, ...] = (
    "https://yts.mx",
    "https://yts.am",
    "https://yts.pm",
)

#: Torrent qualities in the order we prefer them. 1080p leads because it is the
#: usual choice for a personal library; 2160p releases are routinely several
#: times larger and would quietly fill the disk. Pass ``quality="2160p"`` to
#: :func:`_yts_magnet` to opt into 4K for a specific download.
YTS_QUALITY_PREFERENCE: tuple[str, ...] = ("1080p", "720p", "2160p", "480p", "360p")

#: Results pulled per search. Generous because the fallback query is a bare
#: title, and the film we want can sit well down YTS's relevance list.
YTS_LIMIT = 20

#: Per-mirror budget. YTS resolution runs inside the /download request, so it
#: stays short; three dead mirrors must not cost the client half a minute.
YTS_TIMEOUT = float(os.environ.get("CINEVAULT_YTS_TIMEOUT", "6"))

#: Per-mirror budget for the lighter, on-demand trailer lookup. Shorter than
#: ``YTS_TIMEOUT`` because it runs on the ``/search`` and ``/api/trailer``
#: request paths, where a dead mirror must not stall the visitor.
TRAILER_TIMEOUT = float(os.environ.get("CINEVAULT_TRAILER_TIMEOUT", "4"))

#: Resolved trailer codes - and the negatives - keyed by ``(title, year)``.
#: A trailer is immutable, so this never needs an expiry; only a size bound.
_YT_TRAILER_CACHE: dict[tuple[str, str], Optional[str]] = {}
_YT_TRAILER_LOCK = threading.Lock()
_YT_TRAILER_CACHE_MAX = 256

#: YouTube embed endpoint. The ``autoplay`` flag matches the SPA's iframe so
#: the trailer starts as soon as the modal opens.
YT_EMBED_URL = "https://www.youtube.com/embed/{code}?autoplay=1"

#: Leading articles dropped before comparing titles, so "The Matrix" == "Matrix".
_TITLE_ARTICLES: tuple[str, ...] = ("the ", "a ", "an ")


def _normalize_title(text: str) -> str:
    """Fold a title to bare alphanumerics so punctuation stops mattering.

    ``"The Matrix"``, ``"the matrix"`` and ``"The.Matrix"`` all collapse to
    ``"the matrix"``. A trailing year is kept (``"The Matrix (1999)"`` ->
    ``"the matrix 1999"``) because :func:`_yts_magnet` scores a year match
    separately; it matches a bare title through its containment tier.
    """
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").casefold()).strip()


def _title_variants(text: str) -> tuple[str, str]:
    """Return ``(full, primary)`` normalised forms of a title.

    ``primary`` is the part before a subtitle separator, with a leading article
    dropped -- the bit of a release name that actually identifies the film. YTS
    files sequels and parts as ``"Dune: Part One"``, so a request for ``"Dune"``
    has to match that as strongly as it matches a literal ``"Dune"``.
    """
    raw = str(text or "")
    full = _normalize_title(raw)
    primary = _normalize_title(raw.split(":", 1)[0])
    for prefix in ("the ", "a ", "an "):
        if full.startswith(prefix):
            full = full[len(prefix):]
            break
    for prefix in ("the ", "a ", "an "):
        if primary.startswith(prefix):
            primary = primary[len(prefix):]
            break
    return full, primary


def _title_rank(needle: tuple[str, str], name: tuple[str, str]) -> int:
    """Score how well ``name`` matches ``needle``; lower is better, 3 is a miss.

    0 is an exact match of either the full or the primary title, 1 is a prefix
    match (``"The Matrix Resurrections"`` for ``"The Matrix"``), 2 is a
    containment match (``"A Glitch in the Matrix"``).
    """
    needle_full, needle_primary = needle
    name_full, name_primary = name
    if not name_full or not needle_full:
        return 3
    if name_full == needle_full or name_primary == needle_primary:
        return 0
    if name_full.startswith(needle_full) or needle_full.startswith(name_full):
        return 1
    if needle_full in name_full or name_full in needle_full:
        return 2
    return 3


def _build_magnet(file_hash: str, display_name: str) -> str:
    """Assemble a magnet URI from a torrent hash, trackers included."""
    trackers = "".join(f"&tr={tracker}" for tracker in DEFAULT_BITTORRENT_TRACKERS)
    return f"magnet:?xt=urn:btih:{file_hash}&dn={urllib.parse.quote_plus(display_name or '')}{trackers}"


def _yts_pick_torrent(
    movie: dict[str, Any], quality: Optional[str]
) -> Optional[dict[str, Any]]:
    """Choose a torrent entry from a YTS movie payload, honouring ``quality``."""
    torrents = [
        entry
        for entry in movie.get("torrents") or []
        if isinstance(entry, dict) and str(entry.get("hash") or "").strip()
    ]
    if not torrents:
        return None
    if quality:
        wanted = str(quality).lower().strip()
        for entry in torrents:
            if str(entry.get("quality") or "").lower() == wanted:
                return entry
    for wanted in YTS_QUALITY_PREFERENCE:
        for entry in torrents:
            if str(entry.get("quality") or "").lower() == wanted:
                return entry
    return torrents[0]


def _yts_fetch(
    domain: str, term: str, *, timeout: Optional[float] = None
) -> tuple[Optional[list[Any]], Optional[str]]:
    """Query one mirror for ``term``.

    Returns ``(movies, None)`` on success -- possibly an empty list, which is a
    valid "nothing matched" answer -- or ``(None, error)`` when the mirror
    itself could not be reached.
    """
    try:
        response = requests.get(
            f"{domain}/api/v2/list_movies.json",
            params={"query_term": term, "limit": YTS_LIMIT},
            timeout=timeout or YTS_TIMEOUT,
            headers=HTTP_HEADERS,
        )
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 - a dead mirror must not abort the probe
        return None, f"{domain}: {type(exc).__name__}"
    return (payload.get("data") or {}).get("movies") or [], None


def _pick_trailer_code(
    movies: list[Any], needle: tuple[str, str], year: Optional[str]
) -> Optional[str]:
    """The best ``yt_trailer_code`` from a YTS result list, or ``None``.

    Reuses the download pipeline's title ranking so a literal ``"The Matrix"``
    beats ``"The Matrix Resurrections"`` here too; an exact year match breaks
    the remaining ties.
    """
    year_text = str(year or "").strip()
    best: Optional[tuple[int, int, str]] = None
    for movie in movies:
        if not isinstance(movie, dict):
            continue
        if _title_rank(needle, _title_variants(movie.get("title"))) >= 3:
            continue
        code = str(movie.get("yt_trailer_code") or "").strip()
        if not code:
            continue
        same_year = 0 if year_text and str(movie.get("year") or "").strip() == year_text else 1
        candidate = (
            _title_rank(needle, _title_variants(movie.get("title"))),
            same_year,
            code,
        )
        if best is None or candidate[:2] < best[:2]:
            best = candidate
    return best[2] if best is not None else None


def _trailer_code(title: str, year: Any = None) -> Optional[str]:
    """Resolve ``title`` to a YouTube trailer code via the YTS API, memoised.

    The movie cards returned to the front-end already carry a
    ``yt_trailer_code``; this is the shared resolver behind that field and the
    ``/api/trailer`` click-path. Only the first mirror that *answers* is used,
    and a total blackout is left uncached so a later click can still succeed.
    """
    title = str(title or "").strip()
    if not title:
        return None
    key = (title.casefold(), str(year or "").strip())
    with _YT_TRAILER_LOCK:
        if key in _YT_TRAILER_CACHE:
            return _YT_TRAILER_CACHE[key]

    needle = _title_variants(title)
    code: Optional[str] = None
    answered = False
    for domain in YTS_DOMAINS:
        movies, failure = _yts_fetch(domain, title, timeout=TRAILER_TIMEOUT)
        if failure:
            continue
        answered = True
        code = _pick_trailer_code(movies or [], needle, year)
        break
    if not answered:
        return None

    with _YT_TRAILER_LOCK:
        if len(_YT_TRAILER_CACHE) >= _YT_TRAILER_CACHE_MAX:
            _YT_TRAILER_CACHE.clear()
        _YT_TRAILER_CACHE[key] = code
    return code


#: A single background worker resolves vault trailer codes; it is restarted on
#: demand and never runs more than one pass at a time.
_LIBRARY_WARM_LOCK = threading.Lock()
_LIBRARY_WARM_RUNNING = False

#: ``movies.unique_key`` ends in `` (YYYY)`` when the year is known.
_UNIQUE_KEY_YEAR = re.compile(r"\((\d{4})\)\s*$")


def _row_year(row: dict[str, Any]) -> str:
    """Best-effort year for a movie row.

    Vault rows have no ``year`` column - the year only survives inside
    ``unique_key`` as a trailing ``(YYYY)``. Recovering it keeps the trailer
    lookup year-aware, which is what tells a remake apart from the original.
    """
    year = row.get("year")
    if year not in (None, ""):
        return str(year).strip()
    match = _UNIQUE_KEY_YEAR.search(str(row.get("unique_key") or ""))
    return match.group(1) if match else ""


def _warm_library_trailers(rows: Sequence[dict[str, Any]]) -> None:
    """Fill the trailer memo for vault rows off the request thread.

    A vault row can be resolved with a YTS round trip, which is far too slow to
    block ``/api/library`` (fetched on boot and on every ``library.changed``
    event). Instead the codes still missing from the memo are resolved by one
    background thread; when it finds anything new the clients are asked to
    resync, so the next ``/api/library`` payload already carries the codes.
    """
    global _LIBRARY_WARM_RUNNING

    pending: list[tuple[str, str]] = []
    for row in rows:
        if row.get("yt_trailer_code"):
            continue
        title = str(row.get("title") or "").strip()
        if not title:
            continue
        year = _row_year(row)
        with _YT_TRAILER_LOCK:
            if (title.casefold(), year) in _YT_TRAILER_CACHE:
                continue
        pending.append((title, year))
    if not pending:
        return

    with _LIBRARY_WARM_LOCK:
        if _LIBRARY_WARM_RUNNING:
            return
        _LIBRARY_WARM_RUNNING = True

    def _run() -> None:
        global _LIBRARY_WARM_RUNNING
        found = False
        try:
            for title, year in pending:
                try:
                    if _trailer_code(title, year or None):
                        found = True
                except Exception as exc:  # pragma: no cover - best effort
                    LOGGER.warning("vault trailer warm failed for %r: %s", title, exc)
        finally:
            with _LIBRARY_WARM_LOCK:
                _LIBRARY_WARM_RUNNING = False
        if found:
            # Let every client refetch so the vault cards pick the codes up.
            _broadcast("library.changed", {"reason": "trailers"})

    threading.Thread(target=_run, name="trailer-warm", daemon=True).start()


def _attach_trailer_codes(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return vault rows with an explicit ``yt_trailer_code`` on every record.

    ``/search`` and ``/api/discover`` movie cards already carry the trailer
    code, but rows read straight from SQLite have no such field - which is why
    the vault's "Watch Trailer" button had nothing to read. Codes already in
    the memo are applied instantly; the rest are scheduled for a background
    warm (:func:`_warm_library_trailers`) and stay ``None`` for now, leaving
    the SPA's lazy ``/api/trailer`` click-path as the fallback.
    """
    movies: list[dict[str, Any]] = []
    for row in rows:
        movie = dict(row)
        title = str(movie.get("title") or "").strip()
        year = _row_year(movie)
        if not movie.get("yt_trailer_code"):
            code: Optional[str] = None
            if title:
                with _YT_TRAILER_LOCK:
                    code = _YT_TRAILER_CACHE.get((title.casefold(), year))
            movie["yt_trailer_code"] = code
        if year and not movie.get("year"):
            # Surface the recovered year so the SPA's lazy /api/trailer call is
            # year-qualified too, and the card can show it.
            movie["year"] = year
        movies.append(movie)
    _warm_library_trailers(movies)
    return movies


def _yts_best(
    movies: list[Any],
    needle: tuple[str, str],
    year: Optional[str],
    quality: Optional[str],
) -> tuple[Optional[str], Optional[str]]:
    """Pick the best ``(magnet, label)`` from a YTS result list, or ``(None, None)``.

    Candidates are ordered by title rank first and year match second, so a
    literal ``"The Matrix"`` always beats ``"The Matrix Resurrections"``, and a
    ``"Dune"`` request prefers ``"Dune: Part One"`` (same primary title) over
    the unrelated ``"The Dunes"``.
    """
    year_text = str(year or "").strip()
    best: Optional[tuple[int, int, dict[str, Any], dict[str, Any]]] = None
    for movie in movies:
        if not isinstance(movie, dict):
            continue
        rank = _title_rank(needle, _title_variants(movie.get("title")))
        if rank >= 3:
            continue
        entry = _yts_pick_torrent(movie, quality)
        if not entry:
            continue
        same_year = 0 if year_text and str(movie.get("year") or "").strip() == year_text else 1
        candidate = (rank, same_year, movie, entry)
        if best is None or candidate[:2] < best[:2]:
            best = candidate

    if best is None:
        return None, None
    _, _, movie, entry = best
    display = str(movie.get("title_long") or movie.get("title") or "")
    magnet = _build_magnet(str(entry.get("hash") or "").strip(), display)
    quality_text = str(entry.get("quality") or "").strip()
    label = f"YTS {display}".strip()
    if quality_text:
        label = f"{label} [{quality_text}]"
    return magnet, label


def _yts_magnet(
    title: str, year: Optional[str] = None, quality: Optional[str] = None
) -> tuple[Optional[str], Optional[str]]:
    """Resolve ``title`` to a YTS magnet, or say why it could not be resolved.

    Returns ``(magnet, label)`` on success and ``(None, reason)`` otherwise.
    This is the aria2c engine's only automatic source. The previous fallback was
    a yt-dlp YouTube search, which cannot yield a torrent -- it handed aria2c a
    watch page and left an HTML file masquerading as a completed movie.

    Each mirror is asked precisely first (``"The Matrix 1999"``, which narrows
    YTS to the exact film) and then broadly (``"The Matrix"``), because a
    mistyped or absent year returns zero results on its own.
    """
    needle = _title_variants(title)
    if not needle[0]:
        return None, "no movie title to look up on YTS"

    year_text = str(year or "").strip()
    terms = [title]
    if year_text and year_text not in str(title):
        terms.insert(0, f"{title} {year_text}".strip())

    errors: list[str] = []
    for domain in YTS_DOMAINS:
        before = len(errors)
        for term in terms:
            movies, failure = _yts_fetch(domain, term)
            if failure:
                # The mirror is unreachable; its broader term will not help.
                errors.append(failure)
                break
            if not movies:
                continue
            magnet, label = _yts_best(movies, _title_variants(title), year, quality)
            if magnet:
                return magnet, label
            errors.append(
                f"{domain}: {len(movies)} result(s) for {term!r}, none matching {title!r}"
            )
            break
        if len(errors) == before:
            errors.append(f"{domain}: no results for {title!r}")

    detail = "; ".join(errors[-3:]) if errors else "no YTS mirror responded"
    return None, f"no YTS torrent found for {title!r} ({detail})"

_SIZE_UNITS = {"": 0, "K": 1, "M": 2, "G": 3, "T": 4, "P": 5}


def _ytdlp_available() -> bool:
    """True when the ``yt_dlp`` package can be imported."""
    try:
        return importlib.util.find_spec("yt_dlp") is not None
    except (ImportError, ValueError):  # pragma: no cover - defensive
        return False


def _safe_name(text: Any, *, fallback: str = "download", limit: int = 120) -> str:
    """Turn a title into a filesystem-friendly base name."""
    cleaned = _SAFE_NAME_RE.sub(" ", str(text or "")).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    cleaned = cleaned[:limit].strip(" .")
    return cleaned or fallback


def _parse_size_bytes(text: str) -> int:
    """Parse ``1.5MiB`` / ``900KB`` into a byte count (0 when unknown)."""
    match = re.match(r"~?\s?([\d.]+)\s*([KMGTP]?)(i?)B", text.strip(), re.IGNORECASE)
    if not match:
        return 0
    number = float(match.group(1))
    prefix = match.group(2).upper()
    step = 1024 if match.group(3) else 1000
    return int(number * (step ** _SIZE_UNITS[prefix]))


def _parse_speed_kbps(text: str) -> float:
    """Parse ``3.10MiB/s`` into kilobytes per second."""
    match = re.match(r"([\d.]+)\s*([KMGTP]?)(i?)B", text.strip(), re.IGNORECASE)
    if not match:
        return 0.0
    number = float(match.group(1))
    prefix = match.group(2).upper()
    step = 1024 if match.group(3) else 1000
    return number * (step ** _SIZE_UNITS[prefix]) / 1024.0


def _parse_eta_s(text: str) -> float:
    """Parse ``00:05`` / ``1:02:03`` / ``42`` into seconds."""
    parts = [int(p) for p in str(text).strip().split(":") if p.strip().isdigit()]
    if not parts:
        return 0.0
    if len(parts) == 1:
        return float(parts[0])
    if len(parts) == 2:
        return float(parts[0] * 60 + parts[1])
    return float(parts[0] * 3600 + parts[1] * 60 + parts[2])


def _looks_like_media_path(line: str) -> bool:
    """True for a bare path printed by ``--print after_move:filepath``."""
    text = line.strip().strip('"')
    if not text or text.startswith("["):
        return False
    return Path(text).suffix.lower() in _MEDIA_SUFFIXES


def _search_first_url(query: str) -> Optional[str]:
    """Resolve ``query`` to a single downloadable URL using yt-dlp's search."""
    try:
        import yt_dlp
    except ImportError:  # pragma: no cover - environment specific
        LOGGER.error("yt-dlp is not installed; cannot resolve %r", query)
        return None
    options = {
        "quiet": True,
        "no_warnings": True,
        "noplaylist": True,
        "skip_download": True,
        "socket_timeout": int(HTTP_TIMEOUT),
    }
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            info = ydl.extract_info(f"ytsearch1:{query}", download=False)
    except Exception as exc:  # pragma: no cover - network dependent
        LOGGER.warning("yt-dlp search failed for %r: %s", query, exc)
        return None
    if isinstance(info, dict) and info.get("entries"):
        info = info["entries"][0]
    if not isinstance(info, dict):
        return None
    return info.get("webpage_url") or info.get("original_url") or info.get("url")


def _free_tcp_port() -> int:
    """Reserve a free localhost TCP port for aria2c's RPC listener."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

#: Substrings that mark an aria2c console line as a diagnosis rather than
#: routine chatter. When the process dies, its stderr has already been merged
#: into the drained tail, so these are how we find the line worth reporting.
_ARIA2C_ERROR_MARKERS: tuple[str, ...] = (
    "error",
    "exception",
    "failed",
    "failure",
    "could not",
    "cannot",
    "unrecognized",
    "unknown option",
    "not found",
    "refused",
    "invalid",
)

#: Lines aria2c prints at the end of *every* run. They name no failure, so they
#: must not be what a failure report leads with.
_ARIA2C_BOILERPLATE: tuple[str, ...] = (
    "if there are any errors",
    "see the log file",
    "aria2 will resume download if the transfer is restarted",
    "download results:",
    "status legend:",
    "(ok):download completed",
    "|stat|",
    "path/uri",
)


def _aria2c_failure_reason(exit_code: Optional[int], tail: Sequence[str]) -> str:
    """Turn aria2c's own console output into an actionable failure message.

    ``tail`` is the merged stdout/stderr drained while it ran. aria2c explains
    itself on stderr -- ``Exception: ... errorCode=28`` for a rejected option,
    ``errorCode=N`` for a transfer that died -- but the last line is often a
    blank or a ``Download Results`` banner. Preferring the lines that look like
    a diagnosis (and falling back to the last few non-blank ones) keeps the
    Activity Log showing the real cause instead of a bare exit code.
    """
    cleaned = [_ANSI_RE.sub("", line).strip() for line in tail]
    cleaned = [line for line in cleaned if line]
    # aria2c closes every run with a results table and a "see the log file"
    # pointer. Those lines name no failure, yet the last of them is exactly what
    # the old code reported. Drop them, and drop separator lines with no text, so
    # a silent failure says "exited with code N" instead of quoting boilerplate.
    meaningful = [
        line
        for line in cleaned
        if any(ch.isalnum() for ch in line)
        and not any(noise in line.casefold() for noise in _ARIA2C_BOILERPLATE)
    ]
    flagged = [
        line
        for line in meaningful
        if any(marker in line.casefold() for marker in _ARIA2C_ERROR_MARKERS)
    ]
    detail = " | ".join((flagged or meaningful)[-3:])
    if exit_code is None:
        return detail
    if detail:
        return f"aria2c exited with code {exit_code}: {detail}"
    return f"aria2c exited with code {exit_code}"


def _aria2c_followed_gid(status: dict[str, Any]) -> Optional[str]:
    """Return the GID aria2c handed this download off to, if any.

    A magnet's first download only fetches the torrent *metadata*. aria2c then
    creates a separate download for the real payload and lists its GID in
    ``followedBy``. The metadata download reports ``complete`` within seconds,
    carrying a single ``[METADATA]<name>`` pseudo-file a few tens of KiB long,
    so a poller that stops there believes the movie is finished and shuts aria2c
    down mid-transfer -- leaving zero-byte files, a bogus 100 % progress and a
    non-zero exit. The returned GID is the newest in the chain.
    """
    followed = status.get("followedBy")
    if not isinstance(followed, list) or not followed:
        return None
    newest = str(followed[-1] or "").strip()
    return newest or None


#: How many console lines to keep in the application log when a transfer dies.
#: The user-facing Activity Log gets the short reason; this is for the log file,
#: where the full context (tracker failures, peer errors) is worth having.
_ARIA2C_LOG_TAIL_LINES = 15


def _log_aria2c_output(movie_id: int, tail: Sequence[str]) -> None:
    """Write the captured aria2c console tail to the application log.

    ``job.error`` carries only the few informative lines, which is what the
    Activity Log should show. The whole tail goes to the log file so a failure
    that only manifests as tracker or peer warnings is still diagnosable.
    """
    if not tail:
        return
    LOGGER.error(
        "aria2c output for movie=%s:\n%s",
        movie_id,
        "\n".join(tail[-_ARIA2C_LOG_TAIL_LINES:]),
    )


class _Aria2cRpc:
    """Minimal JSON-RPC client for a running aria2c instance.

    aria2c is started with ``--enable-rpc`` on a private localhost port so the
    manager can read exact transfer metrics (``completedLength`` /
    ``totalLength`` / ``downloadSpeed``) and the finished file path instead of
    scraping its console output. Every call is best-effort: transport hiccups
    return ``None`` and the caller falls back to exit-code handling.
    """

    def __init__(self, port: int, secret: str, *, timeout: float = 5.0) -> None:
        self.url = f"http://127.0.0.1:{port}/jsonrpc"
        self.token = f"token:{secret}"
        self.timeout = timeout

    def _call(self, method: str, params: Optional[list[Any]] = None) -> Any:
        body = {
            "jsonrpc": "2.0",
            "id": "cv",
            "method": method,
            "params": [self.token, *(params or [])],
        }
        try:
            resp = requests.post(self.url, json=body, timeout=self.timeout)
            payload = resp.json()
        except (requests.RequestException, ValueError):
            return None
        if not isinstance(payload, dict) or payload.get("error"):
            return None
        return payload.get("result")

    def ready(self) -> bool:
        """True once the RPC listener is accepting calls."""
        return self._call("aria2.getVersion") is not None

    def add_uri(self, uri: str, *, out_dir: str, limit: Optional[str] = None) -> Optional[str]:
        """Queue ``uri`` and return its GID (or ``None`` when it never lands)."""
        options: dict[str, Any] = {"dir": out_dir}
        if limit:
            options["max-download-limit"] = limit
        result = self._call("aria2.addUri", [[uri], options])
        return str(result) if result else None

    def status(self, gid: str) -> Optional[dict[str, Any]]:
        keys = [
            "gid",
            "status",
            "totalLength",
            "completedLength",
            "downloadSpeed",
            "files",
            "bitfield",
            "pieceLength",
            "numPieces",
            "errorCode",
            "errorMessage",
            "followedBy",
        ]
        result = self._call("aria2.tellStatus", [gid, keys])
        return result if isinstance(result, dict) else None

    def shutdown(self, *, force: bool = False) -> None:
        self._call("aria2.forceShutdown" if force else "aria2.shutdown")

    def save_session(self) -> bool:
        """Flush aria2c's ``--save-session`` file (best effort).

        Called before a graceful shutdown so the queue (and its options) is on
        disk even if the process is killed afterwards.
        """
        return self._call("aria2.saveSession") is not None


#: --- network availability (offline playback + smart resume) ---------------- #
#: Probing a DNS resolver is the cheapest reliable "is there a route out"
#: check; no HTTP request is made, so a dead web host is never mistaken for a
#: dead connection. ``CINEVAULT_NETCHECK=0`` disables monitoring entirely.
NETWORK_ENABLED: bool = (
    os.environ.get("CINEVAULT_NETCHECK", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)
NETWORK_PROBE_TIMEOUT = max(0.5, float(os.environ.get("CINEVAULT_NET_TIMEOUT", "3")))
NETWORK_POLL_INTERVAL = max(1.0, float(os.environ.get("CINEVAULT_NET_INTERVAL", "5")))
NETWORK_PROBES = os.environ.get("CINEVAULT_NET_PROBES") or (
    "1.1.1.1:53,8.8.8.8:53,1.1.1.1:443,8.8.8.8:443"
)


def _network_probe_targets() -> tuple[tuple[str, int], ...]:
    """Parse ``CINEVAULT_NET_PROBES`` into ``(host, port)`` pairs."""
    targets: list[tuple[str, int]] = []
    for item in NETWORK_PROBES.split(","):
        host, _, raw_port = item.strip().partition(":")
        if not host:
            continue
        try:
            targets.append((host, int(raw_port or "53")))
        except ValueError:
            continue
    return tuple(targets) or (("1.1.1.1", 53),)


class _NetworkMonitor:
    """Best-effort "is the host online" signal shared by the workers.

    When the route disappears the download worker parks the transfer (state
    saved, aria2c session flushed) and revives it the moment the link returns,
    so a Wi-Fi drop never makes a title start over from 0%.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._online = True
        self._known = False
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._targets = _network_probe_targets()

    @property
    def online(self) -> bool:
        with self._lock:
            return self._online

    @property
    def known(self) -> bool:
        """True once at least one probe has run (avoids false OFF concurrency)."""
        with self._lock:
            return self._known

    @property
    def enabled(self) -> bool:
        return NETWORK_ENABLED

    def _set(self, value: bool) -> None:
        value = bool(value)
        with self._lock:
            changed = value != self._online
            self._online = value
            self._known = True
        self._wake.set()
        if changed:
            LOGGER.info("network %s", "online" if value else "offline")
            _broadcast("network.changed", {"online": value})
            log_activity(
                "network.online" if value else "network.offline",
                (
                    "Internet connection restored - resuming downloads"
                    if value
                    else "Internet connection lost - downloads paused, partial files stay playable"
                ),
                level="info" if value else "warning",
            )

    def check(self) -> bool:
        """Probe the configured hosts once and record the result."""
        if not NETWORK_ENABLED:
            self._set(True)
            return True
        reachable = False
        for host, port in self._targets:
            try:
                with socket.create_connection(
                    (host, port), timeout=NETWORK_PROBE_TIMEOUT
                ):
                    reachable = True
                    break
            except OSError:
                continue
        self._set(reachable)
        return reachable

    def wait(self, timeout: float) -> bool:
        """Block up to ``timeout`` for a transition, then report the state."""
        self._wake.wait(timeout)
        self._wake.clear()
        return self.online

    def start(self) -> None:
        if not NETWORK_ENABLED or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="network-monitor", daemon=True
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.check()
            except Exception:  # pragma: no cover - defensive
                LOGGER.debug("network probe failed", exc_info=True)
            self._stop.wait(NETWORK_POLL_INTERVAL)

    def stop(self) -> None:
        self._stop.set()


NETWORK = _NetworkMonitor()


class _PausedMidway(Exception):
    """Raised internally when a running transfer is paused and killed."""


class _OfflineMidway(Exception):
    """Raised when a transfer must park because the connection dropped."""


@dataclass
class DownloadJob:
    """In-memory state for one real download (a single yt-dlp subprocess)."""

    movie_id: int
    task_id: int
    title: str = ""
    year: str = ""
    url: Optional[str] = None
    engine: str = ENGINE_NAME
    magnet: Optional[str] = None
    progress: float = 0.0
    speed_kbps: float = 0.0
    eta_s: float = 0.0
    downloaded_bytes: int = 0
    total_bytes: int = 0
    filename: Optional[str] = None
    #: "Stream while downloading" state: the media file a live transfer is
    #: writing, its eventual size, and the contiguous verified run of bytes
    #: from the start of that file (the only region safe to serve).
    stream_path: Optional[str] = None
    stream_total: int = 0
    stream_available: int = 0
    #: True while the worker is parked on a pause or a lost connection. The
    #: thread is still alive, but nothing is being fetched, so the UI must not
    #: report it as "running" (that would hide the OFFLINE_READY state).
    parked: bool = False
    error: Optional[str] = None
    running: threading.Event = field(default_factory=threading.Event)
    stopped: threading.Event = field(default_factory=threading.Event)
    proc: Optional["subprocess.Popen[str]"] = None
    thread: Optional[threading.Thread] = None

    def __post_init__(self) -> None:
        self.running.set()

    @property
    def alive(self) -> bool:
        return self.thread is not None and self.thread.is_alive()


class DownloadManager:
    """Runs and controls real yt-dlp downloads, one job per movie."""

    def __init__(self, target: str, *, preset: str = "normal") -> None:
        self.target = target
        self._preset: Optional[str] = preset
        self._rate_limit: Optional[str] = SPEED_PRESETS.get(preset)
        self._jobs: dict[int, DownloadJob] = {}
        self._lock = threading.RLock()
        self._slots = threading.Semaphore(MAX_CONCURRENT_DOWNLOADS)

    # -- speed controls (yt-dlp --limit-rate caps) -------------------------- #
    def speed(self) -> dict[str, Any]:
        """Current transfer-rate cap; new downloads pick it up immediately."""
        with self._lock:
            return _speed_payload(self._preset, self._rate_limit)

    def set_speed(
        self,
        *,
        preset: Optional[str] = None,
        rate_limit: Optional[str] = None,
    ) -> dict[str, Any]:
        """Adjust the ``--limit-rate`` cap applied to new yt-dlp downloads."""
        with self._lock:
            if preset is not None:
                name = str(preset).strip().lower()
                if name not in SPEED_PRESETS:
                    raise ValueError(
                        f"unknown preset {preset!r}; expected one of "
                        f"{sorted(SPEED_PRESETS)}"
                    )
                self._preset = name
                self._rate_limit = SPEED_PRESETS[name]
            if rate_limit is not None:
                text = str(rate_limit).strip()
                self._rate_limit = (
                    None if text.lower() in {"", "0", "none", "unlimited"} else text
                )
                self._preset = None
            return _speed_payload(self._preset, self._rate_limit)

    # -- lifecycle ---------------------------------------------------------- #
    def start(
        self,
        movie_id: int,
        task_id: int,
        *,
        progress: Optional[float] = None,
        autostart: bool = True,
        url: Optional[str] = None,
        title: Optional[str] = None,
        year: Optional[str] = None,
        engine: Optional[str] = None,
        magnet: Optional[str] = None,
    ) -> DownloadJob:
        """Start (or return) the real download job for ``movie_id``."""
        with self._lock:
            existing = self._jobs.get(movie_id)
            if existing is not None and existing.alive:
                if url and not existing.url:
                    existing.url = url
                if magnet and not existing.magnet:
                    existing.magnet = magnet
                if autostart:
                    existing.running.set()
                return existing

            # Always re-read the row: even when the caller pre-supplies
            # progress/engine/magnet, the persisted stream window is what keeps a
            # resumed or re-attached transfer correctly readable (and correctly
            # OFFLINE_READY) before the engine reports its first bitfield.
            task = db.get_task_for_movie(movie_id, target=self.target)
            if task is not None:
                if progress is None:
                    progress = float(task["progress"])
                if engine is None:
                    engine = task.get("engine") or ENGINE_NAME
                if magnet is None:
                    magnet = task.get("magnet")
            elif progress is None:
                progress = 0.0
            movie = db.get_movie(movie_id, target=self.target) or {}

            job = DownloadJob(
                movie_id=movie_id,
                task_id=task_id,
                title=title or movie.get("title") or f"Movie #{movie_id}",
                year=str(year or movie.get("year") or ""),
                url=url,
                engine=(engine or ENGINE_NAME),
                magnet=magnet,
                progress=float(progress or 0.0),
                # Carry the last known playable prefix forward so a resumed job
                # cannot temporarily erase it (and its OFFLINE_READY state) until
                # the engine reports fresh piece data.
                stream_path=(task or {}).get("stream_path") or None,
                stream_total=int((task or {}).get("stream_total") or 0),
                stream_available=int((task or {}).get("stream_available") or 0),
            )
            if not autostart:
                job.running.clear()
            job.thread = threading.Thread(
                target=self._run,
                args=(job,),
                name=f"download-{movie_id}",
                daemon=True,
            )
            self._jobs[movie_id] = job
            job.thread.start()
            LOGGER.info(
                "download started: movie=%s task=%s from %.1f%%",
                movie_id,
                task_id,
                job.progress,
            )
            return job

    def _run(self, job: DownloadJob) -> None:
        """Worker: wait for a slot, transfer the file, then finalize state."""
        state = "active"
        acquired = False
        try:
            # Park until a worker slot frees up (keeps the host polite when many
            # titles are queued). Pause/stop still work while waiting.
            while not job.stopped.is_set():
                if self._slots.acquire(timeout=CONTROL_TICK):
                    acquired = True
                    break
            state = self._transfer(job) if acquired else "cancelled"
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.exception("download crashed (movie=%s): %s", job.movie_id, exc)
            job.error = str(exc)
            state = "failed"
        finally:
            if acquired:
                self._slots.release()
            self._finalize(job, state)

    def _transfer(self, job: DownloadJob) -> str:
        """Run the download loop, honoring pause/stop and the connection.

        Every attempt is gated on :meth:`_await_ready`, so the two things that
        must never be treated as an engine error -- a user pausing and the
        network dropping -- cleanly park the transfer and resume it later.
        """
        while True:
            if job.stopped.is_set():
                return "cancelled"
            if not self._await_ready(job):
                return "cancelled"
            try:
                if job.engine == ARIA2C_ENGINE_NAME:
                    result = self._download_aria2c(job)
                else:
                    if not job.url:
                        job.url = self._resolve_url(job)
                        if not job.url:
                            job.error = "no downloadable source found"
                            return "failed"
                    result = self._download(job)
            except _OfflineMidway:
                result = "offline"
            except _PausedMidway:
                result = "paused"
            if result == "failed" and not NETWORK.online:
                # It did not fail on its own merits: the route went away. Park
                # it so the retry resumes from the partial file, not from 0%.
                result = "offline"
            if result == "offline":
                job.parked = True
                self._persist(job, state="paused", offline=True)
                continue
            if result == "paused":
                job.parked = True
                self._persist(job, state="paused", offline=not NETWORK.online)
                continue
            return result

    def _await_ready(self, job: DownloadJob) -> bool:
        """Block until the job is running *and* the host is online.

        Returns ``False`` when the job was stopped. Gating every attempt (not
        just the first) is what turns a lost connection into a pause instead of
        an engine error, and it is what marks a partial file OFFLINE_READY:
        once the network is gone, a transfer with verified bytes on disk is
        watchable, so it must not look merely "paused".
        """
        marked: Optional[str] = None
        while not job.stopped.is_set():
            if not job.running.wait(timeout=CONTROL_TICK):
                job.parked = True
                if marked != "paused":
                    self._kill(job)
                    job.speed_kbps = 0.0
                    job.eta_s = 0.0
                    self._persist(job, state="paused", offline=not NETWORK.online)
                    marked = "paused"
                continue
            if not NETWORK.online:
                job.parked = True
                if marked != "offline":
                    self._kill(job)
                    job.speed_kbps = 0.0
                    job.eta_s = 0.0
                    self._persist(job, state="paused", offline=True)
                    log_activity(
                        "download.waiting",
                        (
                            f"{job.title or ('Movie #' + str(job.movie_id))}: "
                            "waiting for the connection to come back"
                        ),
                        movie_id=job.movie_id,
                        level="warning",
                    )
                    marked = "offline"
                continue
            if marked is not None:
                self._persist(job, state="active")
            job.parked = False
            return True
        return False

    def _resolve_url(self, job: DownloadJob) -> Optional[str]:
        """Find the best downloadable URL for the job's title."""
        parts = [job.title, job.year, YTDLP_SEARCH_SUFFIX]
        if job.url and job.url.strip().lower().startswith(("http://", "https://")):
            LOGGER.info("using direct URL for movie=%s: %s", job.movie_id, job.url)
            return job.url.strip()
        query = " ".join(p for p in parts if p).strip()
        LOGGER.info("resolving source for %r", query)
        found = _search_first_url(query)
        if not found and job.year:
            found = _search_first_url(f"{job.title} {YTDLP_SEARCH_SUFFIX}".strip())
        return found

    def _output_template(self, job: DownloadJob) -> str:
        base = _safe_name(job.title, fallback=f"movie-{job.movie_id}")
        if job.year and str(job.year) not in base:
            base = f"{base} ({job.year})"
        # Strict absolute output path: files always land in DOWNLOAD_DIR no
        # matter where the process was launched. The ``[cv<task>]`` tag keeps
        # names unique and lets _find_output() recover a renamed file.
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        return os.path.join(DOWNLOAD_DIR, f"{base} [cv{job.task_id}].%(ext)s")

    def _build_command(self, job: DownloadJob, outtmpl: str) -> list[str]:
        cmd = [
            sys.executable,
            "-m",
            "yt_dlp",
            "--newline",
            "--progress",
            "--no-color",
            "--no-playlist",
            "--continue",
            "--no-overwrites",
            "--retries",
            "5",
            "--fragment-retries",
            "5",
            "--socket-timeout",
            str(int(HTTP_TIMEOUT)),
            "--format",
            YTDLP_FORMAT,
            "--merge-output-format",
            YTDLP_MERGE_FORMAT,
            "--output",
            outtmpl,
            "--print",
            "after_move:filepath",
        ]
        if FFMPEG_LOCATION:
            # Guarantee merges/muxes succeed even when ffmpeg is off PATH.
            cmd += ["--ffmpeg-location", FFMPEG_LOCATION]
        if os.name == "nt":
            cmd.append("--windows-filenames")
        with self._lock:
            rate_limit = self._rate_limit
        if rate_limit:
            cmd += ["--limit-rate", rate_limit]
        cmd += list(YTDLP_EXTRA_ARGS)
        cmd.append(job.url or "")
        return cmd

    def _download(self, job: DownloadJob) -> str:
        """Spawn yt-dlp and stream its progress into the database."""
        outtmpl = self._output_template(job)
        cmd = self._build_command(job, outtmpl)
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"] = "1"
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        LOGGER.info("yt-dlp start (movie=%s): %s", job.movie_id, job.url)
        LOGGER.debug("yt-dlp command: %s", cmd)
        job.error = None
        job.filename = None
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        tail: list[str] = []
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                env=env,
                creationflags=creationflags,
            )
        except OSError as exc:
            # e.g. the interpreter is gone or the output directory is unwritable.
            job.error = f"could not start download engine: {exc}"
            LOGGER.error("yt-dlp spawn failed (movie=%s): %s", job.movie_id, exc)
            return "failed"
        job.proc = proc
        last_write = 0.0
        try:
            assert proc.stdout is not None
            for raw_line in proc.stdout:
                line = raw_line.rstrip("\r\n")
                if not line:
                    continue
                if job.stopped.is_set():
                    break
                self._consume_line(job, line, tail)
                now = time.monotonic()
                if now - last_write >= PROGRESS_DB_INTERVAL:
                    last_write = now
                    self._persist(job, state="active")
        finally:
            code = proc.wait()
            job.proc = None

        if job.stopped.is_set():
            return "cancelled"
        if not job.running.is_set():
            raise _PausedMidway()
        if code == 0:
            if not job.filename or not Path(job.filename).is_file():
                job.filename = self._find_output(job) or job.filename
            if job.filename:
                return self._complete(job)
            job.error = "yt-dlp finished but no output file was found"
            LOGGER.error(
                "yt-dlp exited 0 but produced no file (movie=%s, outtmpl=%s)",
                job.movie_id,
                outtmpl,
            )
            return "failed"
        job.error = job.error or (
            tail[-1] if tail else f"yt-dlp exited with code {code}"
        )
        LOGGER.error(
            "yt-dlp failed (movie=%s, exit=%s): %s", job.movie_id, code, job.error
        )
        return "failed"

    def _aria2c_target(self, job: DownloadJob) -> Optional[str]:
        """Pick the source for an aria2c transfer: magnet, direct URL, then YTS.

        aria2c only speaks HTTP + BitTorrent, so a player page can never be a
        valid target. Priority is therefore an explicit magnet, then an explicit
        non-player URL, then a magnet resolved from YTS for the movie's title.
        The old last resort -- a yt-dlp YouTube search -- is gone on purpose: it
        yielded an HTML page that looked like a completed movie.

        On failure ``job.error`` carries the reason so the worker reports why
        instead of a generic "no source".
        """
        if job.magnet:
            return job.magnet.strip()
        if job.url and not _player_page_host(job.url):
            return job.url.strip()

        magnet, detail = _yts_magnet(job.title, job.year)
        if magnet:
            job.magnet = magnet
            LOGGER.info("aria2c source for movie=%s: %s", job.movie_id, detail)
            log_activity(
                "download.resolve",
                f"{job.title or 'movie ' + str(job.movie_id)}: {detail}",
                movie_id=job.movie_id,
            )
            return magnet
        job.error = detail or "no aria2c source could be resolved"
        LOGGER.error(
            "aria2c has no source for movie=%s: %s", job.movie_id, job.error
        )
        return None

    def _stop_aria2c(
        self, job: DownloadJob, rpc: _Aria2cRpc, *, save: bool
    ) -> None:
        """Stop an aria2c transfer, letting it flush its control file first.

        A graceful ``aria2.shutdown`` makes aria2c write the ``.aria2`` control
        file and the session, which is exactly what lets a pause, a crash or a
        reboot resume from the last piece. If the daemon lingers it is killed so
        a worker slot is never held by a dead process.
        """
        if save:
            rpc.save_session()
            rpc.shutdown(force=False)
        else:
            rpc.shutdown(force=True)
        proc = job.proc
        if proc is not None and proc.poll() is None:
            try:
                proc.wait(timeout=ARIA2C_STOP_GRACE)
            except subprocess.TimeoutExpired:
                self._kill(job)
        job.proc = None

    def _aria2c_session_path(self, job: DownloadJob) -> Path:
        """Per-transfer ``--save-session`` file (persistent resume state).

        Lives under :data:`ARIA2C_STATE_DIR` rather than the media tree, so a
        restart can recover a title's magnet without touching the network and
        never has to scan a session file as if it were the download.
        """
        directory = Path(DOWNLOAD_DIR) / ARIA2C_STATE_DIR
        directory.mkdir(parents=True, exist_ok=True)
        base = _safe_name(job.title, fallback=f"movie-{job.movie_id}")
        return directory / f"{base} [cv{job.task_id}].aria2"

    def _build_aria2c_command(self, job: DownloadJob, port: int, secret: str) -> list[str]:
        session_path = self._aria2c_session_path(job)
        cmd = [
            ARIA2C_LOCATION or "aria2c",
            "--enable-rpc=true",
            "--rpc-listen-all=false",
            f"--rpc-listen-port={port}",
            f"--rpc-secret={secret}",
            f"--dir={DOWNLOAD_DIR}",
            "--seed-time=0",
            # Peer discovery. --enable-dht turns on IPv4 DHT *and* UDP tracker
            # support; --bt-tracker adds a fallback announce list so a magnet
            # with thin or dead trackers still reaches peers. Only one DHT entry
            # point is accepted -- see DHT_ENTRY_POINT above.
            "--enable-dht=true",
            f"--dht-entry-point={DHT_ENTRY_POINT}",
            f"--bt-tracker={','.join(DEFAULT_BITTORRENT_TRACKERS)}",
            "--allow-overwrite=true",
            "--auto-file-renaming=false",
            "--continue=true",
            "--summary-interval=1",
            "--console-log-level=warn",
            "--download-result=full",
            "--max-tries=2",
            "--retry-wait=2",
            # Durable state. aria2c rewrites its ``.aria2`` control file every
            # --auto-save-interval seconds and this session file every
            # --save-session-interval, both flushed on a graceful exit, so a
            # pause/crash/reboot resumes from the last verified piece instead of
            # 0%. --bt-save-metadata keeps the resolved magnet's .torrent so it
            # can be reloaded offline (--bt-load-saved-metadata).
            "--auto-save-interval=30",
            "--save-session-interval=30",
            f"--save-session={session_path}",
            "--bt-save-metadata=true",
            "--bt-load-saved-metadata=true",
            # "Stream while downloading". --file-allocation=none keeps the file
            # on disk growing with real data instead of reserving a sparse block
            # of zeros, so its size tracks what has actually arrived.
            # --bt-prioritize-piece=head raises the priority of the first
            # STREAM_HEAD_PRIORITY bytes of every torrent file, so the movie's
            # header and opening scenes are fetched first. --stream-piece-selector
            # does the same for HTTP/FTP segment downloads (its documented use
            # is "viewing movies while downloading"), with --min-split-size
            # lowered so that mode can pick small sequential segments.
            "--file-allocation=none",
            f"--bt-prioritize-piece=head={STREAM_HEAD_PRIORITY}",
            "--stream-piece-selector=inorder",
            "--min-split-size=1M",
        ]
        with self._lock:
            rate_limit = self._rate_limit
        if rate_limit:
            cmd.append(f"--max-download-limit={rate_limit}")
        return cmd

    def _download_aria2c(self, job: DownloadJob) -> str:
        """Run an aria2c transfer, mirroring its RPC metrics into the database.

        aria2c is started in RPC-only mode (no positional URI); the magnet/direct
        URL is queued through ``aria2.addUri`` so we can poll ``aria2.tellStatus``
        for exact percent/speed/ETA and recover the finished file path.
        """
        if not ARIA2C_LOCATION:
            job.error = "aria2c is not installed (set CINEVAULT_ARIA2C or add it to PATH)"
            LOGGER.error("aria2c unavailable (movie=%s)", job.movie_id)
            return "failed"
        target = self._aria2c_target(job)
        if not target:
            # _aria2c_target already explained itself (no YTS torrent, empty
            # title, every mirror down) -- keep that rather than flatten it.
            job.error = job.error or "no magnet link or downloadable URL provided"
            return "failed"
        unsupported = _aria2c_unsupported_target(target)
        if unsupported:
            job.error = unsupported
            LOGGER.error(
                "aria2c refused target (movie=%s): %s", job.movie_id, unsupported
            )
            return "failed"
        # Remember the resolved magnet so a restart (or an offline boot) can
        # resume this transfer with no lookup of its own.
        if job.magnet:
            try:
                db.update_task(job.task_id, magnet=job.magnet, target=self.target)
            except db.DatabaseError:  # pragma: no cover - row deleted mid-run
                pass

        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        port = _free_tcp_port()
        secret = secrets.token_urlsafe(18)
        cmd = self._build_aria2c_command(job, port, secret)
        rpc = _Aria2cRpc(port, secret)
        creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        LOGGER.info("aria2c start (movie=%s): %s", job.movie_id, target)
        LOGGER.debug("aria2c command: %s", cmd)
        job.error = None
        job.filename = None
        tail: list[str] = []
        started_at = time.time()
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creationflags,
            )
        except OSError as exc:
            job.error = f"could not start aria2c: {exc}"
            LOGGER.error("aria2c spawn failed (movie=%s): %s", job.movie_id, exc)
            return "failed"
        job.proc = proc

        def _drain_output() -> None:
            # Keep the pipe clear so aria2c never blocks on a full buffer.
            try:
                assert proc.stdout is not None
                for raw_line in proc.stdout:
                    line = raw_line.rstrip("\r\n")
                    if line:
                        tail.append(line)
                        del tail[:-40]
            except (OSError, ValueError):  # pragma: no cover - pipe closed on kill
                pass

        drain = threading.Thread(
            target=_drain_output, name=f"aria2c-{job.movie_id}", daemon=True
        )
        drain = threading.Thread(
            target=_drain_output, name=f"aria2c-{job.movie_id}", daemon=True
        )
        drain.start()

        def _settle_output() -> None:
            """Let the drain thread capture the last lines once the pipe closed.

            stderr is merged into the stream we read, so the line that explains a
            failure can still be in flight when the process exits. Only waited
            for once the process is gone, so a live transfer is never delayed.
            """
            if proc.poll() is not None:
                drain.join(timeout=2.0)

        # Wait for the RPC listener, then queue the transfer and get its GID.
        # Watch the process too: a rejected option makes aria2c print a usage
        # block and exit immediately, and without this check we would burn the
        # whole deadline before blaming the RPC for a process already dead.
        deadline = time.monotonic() + max(15.0, HTTP_TIMEOUT)
        while time.monotonic() < deadline:
            if job.stopped.is_set():
                break
            if rpc.ready():
                break
            if proc.poll() is not None:
                break
            time.sleep(CONTROL_TICK)
        if job.stopped.is_set():
            self._kill(job)
            return "cancelled"
        if proc.poll() is not None:
            _settle_output()
            job.error = _aria2c_failure_reason(proc.returncode, tail)
            job.proc = None
            LOGGER.error(
                "aria2c exited before its RPC was ready (movie=%s, exit=%s): %s",
                job.movie_id,
                proc.returncode,
                job.error,
            )
            return "failed"
        with self._lock:
            rate_limit = self._rate_limit
        gid = rpc.add_uri(target, out_dir=DOWNLOAD_DIR, limit=rate_limit)
        if not gid:
            _settle_output()
            job.error = _aria2c_failure_reason(proc.poll(), tail) or (
                "aria2c RPC did not accept the download request"
            )
            LOGGER.error(
                "aria2c RPC rejected the transfer (movie=%s): %s",
                job.movie_id,
                job.error,
            )
            self._kill(job)
            return "failed"

        last_write = 0.0
        final_path: Optional[str] = None
        while True:
            if job.stopped.is_set():
                # Ask aria2c to flush its control file/session before exiting, so
                # a stop or a process shutdown never restarts from 0%.
                self._stop_aria2c(job, rpc, save=True)
                return "cancelled"
            if not job.running.is_set():
                self._stop_aria2c(job, rpc, save=True)
                raise _PausedMidway()
            if not NETWORK.online:
                # The route vanished mid-transfer. Stop gracefully so aria2c
                # keeps its control file -- the next attempt (once the link is
                # back) continues from here, and the verified prefix stays
                # playable offline while we wait.
                self._stop_aria2c(job, rpc, save=True)
                raise _OfflineMidway()
            status = rpc.status(gid)
            if status is None:
                if proc.poll() is not None:
                    break
                time.sleep(CONTROL_TICK)
                continue
            state = str(status.get("status") or "")
            followed = _aria2c_followed_gid(status)
            if followed and followed != gid:
                # Magnet metadata is in; the real payload has its own GID, so
                # poll that one instead. Breaking on the metadata download's
                # "complete" is what previously aborted every torrent transfer
                # seconds in, leaving an empty file tree behind.
                LOGGER.info(
                    "aria2c metadata ready (movie=%s); following download %s",
                    job.movie_id,
                    followed,
                )
                gid = followed
                job.progress = 0.0
                job.downloaded_bytes = 0
                job.total_bytes = 0
                continue
            self._apply_aria2c_status(job, status)
            now = time.monotonic()
            if now - last_write >= PROGRESS_DB_INTERVAL:
                last_write = now
                self._persist(job, state="active")
            if state == "complete":
                final_path = self._aria2c_output_path(status)
                break
            if state in {"error", "removed"}:
                code = status.get("errorCode")
                message = status.get("errorMessage") or f"aria2c error {code}"
                job.error = str(message)
                break
            time.sleep(CONTROL_TICK)

        if job.stopped.is_set():
            self._stop_aria2c(job, rpc, save=True)
            return "cancelled"
        if not job.running.is_set():
            self._stop_aria2c(job, rpc, save=True)
            raise _PausedMidway()

        rpc.shutdown()
        try:
            code = proc.wait(timeout=15.0)
        except subprocess.TimeoutExpired:  # pragma: no cover - stubborn RPC daemon
            self._kill(job)
            code = proc.wait()
        job.proc = None
        _settle_output()

        if job.error:
            LOGGER.error(
                "aria2c failed (movie=%s): %s", job.movie_id, job.error
            )
            _log_aria2c_output(job.movie_id, tail)
            return "failed"

        # aria2c's exit code is authoritative: a non-zero exit means the
        # transfer failed, so it must never become a COMPLETED row even when a
        # (partial) file happens to exist on disk.
        if code != 0:
            job.error = _aria2c_failure_reason(code, tail)
            LOGGER.error(
                "aria2c failed (movie=%s, exit=%s): %s",
                job.movie_id,
                code,
                job.error,
            )
            _log_aria2c_output(job.movie_id, tail)
            return "failed"

        # Only a validated feature that is really on disk may reach the
        # database; everything else is failed with the exact reason, which the
        # worker broadcasts as ``queue.update`` + ``activity``.
        path, reason = self._resolve_aria2c_video(final_path, started_at)
        if path is None:
            job.error = reason or "aria2c finished but produced no valid video"
            LOGGER.error(
                "aria2c produced no valid video (movie=%s): %s",
                job.movie_id,
                job.error,
            )
            return "failed"
        job.filename = str(path)
        return self._complete(job)

    def _apply_aria2c_status(self, job: DownloadJob, status: dict[str, Any]) -> None:
        """Mirror one ``aria2.tellStatus`` payload onto the live job metrics."""
        try:
            total = int(status.get("totalLength") or 0)
            done = int(status.get("completedLength") or 0)
            speed = int(status.get("downloadSpeed") or 0)
        except (TypeError, ValueError):  # pragma: no cover - defensive
            return
        if total > 0:
            job.total_bytes = total
            job.downloaded_bytes = done
            job.progress = max(0.0, min(100.0, done * 100.0 / total))
        job.speed_kbps = speed / 1024.0
        remaining = max(0, total - done)
        job.eta_s = (remaining / speed) if speed > 0 and total > 0 else 0.0
        # Track the playable head of the file so /stream can serve it mid-flight.
        window = _aria2c_stream_window(status)
        if window is not None:
            job.stream_path, job.stream_total, job.stream_available = window

    def _aria2c_output_path(self, status: dict[str, Any]) -> Optional[str]:
        """Return the largest selected file path from a completed torrent/HTTP."""
        files = status.get("files")
        if not isinstance(files, list):
            return None
        best_path: Optional[str] = None
        best_size = -1
        for entry in files:
            if not isinstance(entry, dict):
                continue
            path = str(entry.get("path") or "").strip()
            if not path:
                continue
            try:
                size = int(entry.get("length") or 0)
            except (TypeError, ValueError):
                size = 0
            if size > best_size:
                best_size = size
                best_path = path
        return best_path

    def _consume_line(self, job: DownloadJob, line: str, tail: list[str]) -> None:
        """Interpret one yt-dlp output line, updating the live metrics."""
        match = _YTDLP_PROGRESS_RE.match(line)
        if match:
            job.progress = max(0.0, min(100.0, float(match.group("pct"))))
            total = match.group("total")
            if total:
                parsed = _parse_size_bytes(total)
                if parsed:
                    job.total_bytes = parsed
            if job.total_bytes:
                job.downloaded_bytes = int(job.total_bytes * job.progress / 100.0)
            speed = match.group("speed")
            if speed:
                job.speed_kbps = _parse_speed_kbps(speed)
            eta = match.group("eta")
            if eta:
                job.eta_s = _parse_eta_s(eta)
            return

        dest = _YTDLP_DEST_RE.match(line)
        if dest:
            job.filename = dest.group("path").strip()
            return
        merge = _YTDLP_MERGE_RE.match(line)
        if merge:
            job.filename = merge.group("path").strip()
            return
        if _looks_like_media_path(line):
            job.filename = line.strip().strip('"')
            return
        if line.startswith(("ERROR:", "[error]")):
            job.error = line
            tail.append(line)
        elif line.startswith(("WARNING:", "[warning]")):
            tail.append(line)

    def _find_output(self, job: DownloadJob) -> Optional[str]:
        """Fallback: locate the finished file by its unique ``[cv<task>]`` tag."""
        directory = Path(DOWNLOAD_DIR)
        if not directory.is_dir():
            return None
        # ``[...]`` is a glob character class, so the literal tag has to be
        # escaped with ``[[]``. rglob (not glob) also covers yt-dlp nesting the
        # output inside a sub-folder.
        candidates = [
            path
            for path in directory.rglob(f"*[[]cv{job.task_id}].*")
            if path.is_file() and path.suffix.lower() in _MEDIA_SUFFIXES
        ]
        if not candidates:
            return None
        return str(max(candidates, key=lambda p: p.stat().st_mtime))

    def _resolve_aria2c_video(
        self, reported: Optional[str], started_at: float
    ) -> tuple[Optional[Path], Optional[str]]:
        """Locate the real movie aria2c produced, or say why there is none.

        aria2c reports whichever file it considers largest, which for a torrent
        normally sits in a nested folder -- but is a ``.torrent`` sidecar, a
        ``.rar`` part or a bundled sample when the payload is wrong. The
        reported path is vetted first (it is the exact one aria2c knows), then a
        recursive scan of the library covers every other layout. A candidate is
        accepted only when it is a supported video, inside the download
        directory and large enough to be the feature, and the winner is always
        an absolute path so ``local_path`` never ends up as the bare root
        folder. The second return value is the reason nothing was accepted.
        """
        rejects: list[str] = []
        directory = Path(DOWNLOAD_DIR)

        # 1. Vet the path aria2c itself reported.
        if reported:
            reported_path = Path(reported).expanduser()
            if reported_path.is_dir():
                candidates = _video_files_in(reported_path, ARIA2C_VIDEO_SUFFIXES)
                if not candidates:
                    rejects.append(
                        f"aria2c produced the folder '{reported_path.name}' "
                        "with no video file inside it"
                    )
            elif reported_path.exists():
                candidates = [reported_path]  # vet the exact file it saved
            else:
                candidates = []
                rejects.append(
                    f"aria2c reported '{reported_path}', which does not exist"
                )
            for candidate in candidates:
                reason = _validate_video_file(
                    candidate,
                    root=directory,
                    suffixes=ARIA2C_VIDEO_SUFFIXES,
                    min_bytes=ARIA2C_MIN_VIDEO_BYTES,
                )
                if reason is None:
                    return candidate.resolve(), None
                rejects.append(reason)

        # 2. Fall back to a recursive scan of the library, largest first. The
        #    ``started_at`` guard keeps us from latching onto another movie's
        #    already-finished download.
        for candidate in _video_files_in(directory, ARIA2C_VIDEO_SUFFIXES):
            try:
                stat = candidate.stat()
            except OSError:  # pragma: no cover - file vanished mid-scan
                continue
            touched = max(stat.st_mtime, stat.st_ctime)
            if touched + 1.0 < started_at:  # 1s slack for fs granularity
                continue
            reason = _validate_video_file(
                candidate,
                root=directory,
                suffixes=ARIA2C_VIDEO_SUFFIXES,
                min_bytes=ARIA2C_MIN_VIDEO_BYTES,
            )
            if reason is None:
                return candidate.resolve(), None
            rejects.append(reason)

        if not rejects:
            rejects.append(
                f"no .mp4/.mkv/.avi file was written to {directory} "
                "during this transfer"
            )
        return None, rejects[0]

    def _complete(self, job: DownloadJob) -> str:
        """Stamp the movie's absolute ``local_path`` and return the final state.

        Returns ``"completed"`` only once the file is proven to be on disk;
        otherwise the job is failed with the exact reason and ``"failed"`` is
        returned. Verifying existence in one shared place (rather than per
        engine) is what stops a vanished or never-written output from leaving a
        ``COMPLETED`` row whose ``local_path`` cannot be streamed.
        """
        path = job.filename or self._find_output(job)
        if path:
            # Persist an absolute path so the player/stream endpoint never
            # depends on the process working directory.
            path = str(Path(path).expanduser().resolve())
        if not path or not Path(path).is_file():
            job.error = job.error or (
                "the download reported success but no file is on disk"
                + (f" (expected {path})" if path else "")
            )
            LOGGER.error(
                "refusing to mark movie=%s completed: %s", job.movie_id, job.error
            )
            return "failed"
        job.filename = path
        job.progress = 100.0
        job.speed_kbps = 0.0
        job.eta_s = 0.0
        if job.total_bytes:
            job.downloaded_bytes = job.total_bytes
        try:
            db.update_task(
                job.task_id,
                state="completed",
                progress=100.0,
                speed_kbps=0.0,
                eta_s=0.0,
                downloaded_bytes=job.downloaded_bytes or None,
                total_bytes=job.total_bytes or None,
                error="",  # clear any stale failure text
                local_path=path,
                target=self.target,
            )
        except db.DatabaseError as exc:  # pragma: no cover - row deleted mid-run
            LOGGER.warning("could not finalize task %s: %s", job.task_id, exc)
        self._publish(job, state="completed", progress=100.0)
        LOGGER.info("download completed: movie=%s -> %s", job.movie_id, path)
        return "completed"

    def _persist(
        self,
        job: DownloadJob,
        *,
        state: Optional[str] = None,
        offline: Optional[bool] = None,
    ) -> None:
        """Write the current live metrics (and playable head) to the database.

        ``stream_path``/``stream_total``/``stream_available`` are the exact
        bytes already safe to serve, and they are pinned here rather than
        computed on demand so ``/stream`` keeps working with *no* live job: a
        paused queue, a restart, or a fully disconnected machine.

        ``offline`` (None = leave alone) additionally records whether the paused
        transfer is watchable with no connection. Only a paused transfer with a
        verified prefix qualifies -- a zero-byte title is merely PAUSED.
        """
        ready = bool(offline) and job.stream_available > 0
        try:
            db.update_task(
                job.task_id,
                state=state,
                progress=job.progress,
                speed_kbps=job.speed_kbps,
                eta_s=job.eta_s,
                downloaded_bytes=job.downloaded_bytes or None,
                total_bytes=job.total_bytes or None,
                stream_path=job.stream_path,
                stream_total=job.stream_total or None,
                stream_available=job.stream_available or None,
                offline_ready=ready if offline is not None else None,
                target=self.target,
            )
        except db.DatabaseError as exc:
            LOGGER.warning("progress update failed (task %s): %s", job.task_id, exc)
        # Mirror the same throttled cadence onto the real-time channel.
        self._publish(job, state=state)

    # -- real-time fan-out -------------------------------------------------- #
    def _metric_payload(
        self,
        job: DownloadJob,
        *,
        state: Optional[str] = None,
        progress: Optional[float] = None,
    ) -> dict[str, Any]:
        """One queue row's live metrics, ready to broadcast or JSON-encode."""
        running = bool(job.alive and job.running.is_set() and not job.parked)
        pct = job.progress if progress is None else progress
        # The reported state is authoritative: a job parked on a pause (manual
        # or because the link dropped) is not "running", and a partial transfer
        # with no connection is OFFLINE_READY, i.e. playable from disk.
        effective_state = state or ("active" if running else "paused")
        running = running and effective_state != "paused"
        offline_ready = effective_state == "paused" and not NETWORK.online
        offline_ready = offline_ready and job.stream_available > 0
        return {
            "movie_id": job.movie_id,
            "task_id": job.task_id,
            "state": effective_state,
            "progress": round(float(pct), 2),
            "speed_kbps": round(job.speed_kbps, 1),
            "eta_s": round(job.eta_s, 1),
            "downloaded_bytes": job.downloaded_bytes,
            "total_bytes": job.total_bytes,
            "running": running,
            "filename": job.filename,
            "error": job.error,
            "engine": job.engine,
            "magnet": job.magnet,
            "stream_path": job.stream_path,
            "stream_total": job.stream_total,
            "stream_available": job.stream_available,
            "streamable": bool(job.stream_available > 0),
            "stream_bytes": job.stream_available,
            "offline_ready": offline_ready,
        }

    def _publish(
        self,
        job: DownloadJob,
        *,
        state: Optional[str] = None,
        progress: Optional[float] = None,
    ) -> None:
        """Broadcast a single ``queue.update`` event for this job."""
        _broadcast(
            "queue.update",
            self._metric_payload(job, state=state, progress=progress),
        )

    def publish(self, movie_id: int) -> None:
        """Broadcast the live metrics for ``movie_id`` if it has a job."""
        with self._lock:
            job = self._jobs.get(movie_id)
        if job is not None:
            self._publish(job)

    def _kill(self, job: DownloadJob) -> None:
        """Terminate the job's yt-dlp process tree, if any."""
        proc = job.proc
        if proc is None or proc.poll() is not None:
            return
        if os.name == "nt":
            try:
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=10,
                    check=False,
                )
            except (OSError, subprocess.SubprocessError):
                pass
        try:
            proc.kill()
        except OSError:  # pragma: no cover - already gone
            pass

    def _finalize(self, job: DownloadJob, state: str) -> None:
        """Clean up the job and emit the completion/failure activity events."""
        with self._lock:
            if self._jobs.get(job.movie_id) is job:
                self._jobs.pop(job.movie_id, None)
        if state == "cancelled":
            # Cancelled == stopped (user or process shutdown). A stale
            # "downloading" would hide the partial file from the vault and from
            # auto-resume, so record what is really on disk: PAUSED, or
            # OFFLINE_READY when the transfer had verified bytes to play.
            self._persist(job, state="paused", offline=not NETWORK.online)
        if state == "failed":
            reason = job.error or "download failed"
            LOGGER.error(
                "download failed: movie=%s task=%s: %s",
                job.movie_id,
                job.task_id,
                reason,
            )
            try:
                db.update_task(
                    job.task_id,
                    state="failed",
                    speed_kbps=0.0,
                    eta_s=0.0,
                    error=reason,
                    target=self.target,
                )
            except db.DatabaseError:  # pragma: no cover - row deleted mid-run
                pass
        movie = db.get_movie(job.movie_id, target=self.target) or {}
        title = movie.get("title") or job.title or f"Movie #{job.movie_id}"
        # Final state change onto the socket (completed / failed / cancelled).
        self._publish(job, state=state)
        if state == "completed":
            log_activity(
                "download.completed",
                f"{title} finished downloading",
                movie_id=job.movie_id,
                progress=100.0,
            )
            _broadcast(
                "library.changed",
                {"reason": "completed", "movie_id": job.movie_id},
            )
            # If the container isn't natively streamable in an HTML5 player,
            # make a browser-playable copy in the background.
            self._schedule_web_conversion(job.movie_id, job.task_id, job.filename)
        elif state == "failed":
            reason = job.error or "download failed"
            log_activity(
                "download.failed",
                f"{title} could not be downloaded: {reason}",
                level="error",
                movie_id=job.movie_id,
                error=reason,
            )
            # The movie row flipped to FAILED too; refresh the library so the
            # card stops showing a stale "downloading" state.
            _broadcast(
                "library.changed",
                {"reason": "failed", "movie_id": job.movie_id},
            )
        LOGGER.info(
            "download finished: movie=%s at %.1f%% (%s)",
            job.movie_id,
            job.progress,
            state,
        )

    # -- post-download web conversion --------------------------------------- #
    def _schedule_web_conversion(
        self, movie_id: int, task_id: int, path: Optional[str]
    ) -> None:
        """Convert a finished download to a browser-playable copy off-thread.

        Runs on a daemon thread so a long transcode never blocks the download
        queue or the request that started it. Skipped entirely when conversion
        is disabled, ffmpeg is missing, or the file is already playable.
        """
        if not TRANSCODE_ENABLED or not FFMPEG_LOCATION or not path:
            return
        candidate = Path(path)
        if candidate.is_dir():
            # Torrent-style download: convert the feature, not the folder.
            videos = _video_files_in(candidate)
            if not videos:
                return
            candidate = videos[0]
        if not _needs_web_conversion(str(candidate)):
            return
        threading.Thread(
            target=self._run_web_conversion,
            args=(movie_id, task_id, str(candidate)),
            name=f"web-convert-{movie_id}",
            daemon=True,
        ).start()

    def _run_web_conversion(self, movie_id: int, task_id: int, source: str) -> None:
        """Transcode ``source`` and, on success, repoint the movie at the copy."""
        LOGGER.info("web conversion started: movie=%s <- %s", movie_id, source)
        try:
            converted = convert_to_web_mp4(source)
        except Exception as exc:  # pragma: no cover - never crash the worker
            LOGGER.warning("web conversion crashed (movie=%s): %s", movie_id, exc)
            return
        if converted == source:
            LOGGER.info("web conversion made no change for movie %s", movie_id)
            return
        # The row may have been deleted while ffmpeg ran; drop the orphan copy.
        movie = db.get_movie(movie_id, target=self.target)
        if movie is None:
            _remove_quietly(Path(converted))
            LOGGER.info("movie %s vanished; discarded web copy", movie_id)
            return
        # A re-download may have queued a fresh task while ffmpeg ran; a stale
        # conversion must not clobber the movie's new state.
        if db.get_task_for_movie(movie_id, live_only=True, target=self.target) is not None:
            _remove_quietly(Path(converted))
            LOGGER.info("movie %s was re-queued; discarded web copy", movie_id)
            return
        try:
            # ``update_task`` stamps the movie's local_path alongside the task's,
            # so the /stream route serves the converted file from now on.
            db.update_task(
                task_id,
                state="completed",
                local_path=converted,
                target=self.target,
            )
        except db.DatabaseError as exc:  # pragma: no cover - row deleted mid-run
            LOGGER.warning("could not record web copy for movie %s: %s", movie_id, exc)
            return
        title = movie.get("title") or f"Movie #{movie_id}"
        LOGGER.info("web conversion complete: movie=%s -> %s", movie_id, converted)
        log_activity(
            "transcode.completed",
            f"{title} is now ready to stream",
            movie_id=movie_id,
        )
        _broadcast("library.changed", {"reason": "converted", "movie_id": movie_id})

    # -- controls ----------------------------------------------------------- #
    def pause(self, movie_id: int) -> dict[str, Any]:
        task = self._require_task(movie_id)
        if task["state"] == "completed" or float(task["progress"]) >= 100.0:
            raise ValueError("download already completed")
        with self._lock:
            job = self._jobs.get(movie_id)
            if job is not None:
                job.running.clear()
                job.speed_kbps = 0.0
                job.eta_s = 0.0
        if job is not None:
            self._kill(job)
            # Keep the offline marker honest: pausing while connected is a plain
            # PAUSED, while pausing offline with verified bytes is OFFLINE_READY.
            self._persist(job, state="paused", offline=not NETWORK.online)
            progress = job.progress
        else:
            db.update_task(
                task["id"],
                state="paused",
                speed_kbps=0.0,
                eta_s=0.0,
                offline_ready=(
                    (not NETWORK.online)
                    and int(task.get("stream_available") or 0) > 0
                ),
                target=self.target,
            )
            progress = float(task["progress"])
            _broadcast(
                "queue.update",
                {
                    "movie_id": movie_id,
                    "task_id": task["id"],
                    "state": "paused",
                    "progress": round(progress, 2),
                    "speed_kbps": 0.0,
                    "eta_s": 0.0,
                    "running": False,
                    "engine": task.get("engine") or ENGINE_NAME,
                },
            )
        return self._snapshot(movie_id, task_id=task["id"], progress=progress)

    def resume(self, movie_id: int) -> dict[str, Any]:
        task = self._require_task(movie_id)
        if task["state"] == "completed" or float(task["progress"]) >= 100.0:
            raise ValueError("download already completed")

        with self._lock:
            job = self._jobs.get(movie_id)
            alive = job is not None and job.alive
        if alive and job is not None:
            job.running.set()
            job.error = None
            db.update_task(task["id"], state="active", target=self.target)
        else:
            # Nothing running (first start or a server restart): yt-dlp resumes
            # the partial file with --continue from the last recorded state.
            job = self.start(movie_id, task["id"], progress=float(task["progress"]))
        self.publish(movie_id)
        return self._snapshot(movie_id, task_id=job.task_id, progress=job.progress)

    def stop(self, movie_id: int) -> None:
        """Stop a job, letting the worker flush the engine's resume state.

        The worker owns the transfer, so it is the one that can ask aria2c to
        write its ``.aria2``/session files before exiting. We only hard-kill the
        tree if it fails to unwind within a grace period, so a stop (or a whole
        process shutdown) resumes from the last verified piece instead of 0%.
        """
        with self._lock:
            job = self._jobs.get(movie_id)
        if job is None:
            return
        job.stopped.set()
        job.running.set()  # wake it out of any pause wait
        if job.thread is not None:
            job.thread.join(timeout=ARIA2C_STOP_GRACE + 5.0)
        self._kill(job)
        with self._lock:
            if self._jobs.get(movie_id) is job:
                self._jobs.pop(movie_id, None)

    def shutdown(self) -> None:
        for movie_id in list(self._jobs):
            self.stop(movie_id)

    # -- helpers ------------------------------------------------------------ #
    def _require_task(self, movie_id: int) -> dict[str, Any]:
        task = db.get_task_for_movie(movie_id, live_only=False, target=self.target)
        if task is None:
            raise LookupError(f"movie {movie_id} has no download task")
        if task["state"] in db.FINAL_TASK_STATES and task["state"] != "completed":
            raise ValueError(f"download task is {task['state']}")
        return task

    def _snapshot(
        self, movie_id: int, *, task_id: int, progress: float
    ) -> dict[str, Any]:
        with self._lock:
            job = self._jobs.get(movie_id)
            running = bool(job is not None and job.alive and job.running.is_set())
            speed = job.speed_kbps if job else 0.0
            eta = job.eta_s if job else 0.0
            engine = job.engine if job is not None else ENGINE_NAME
        return {
            "movie_id": movie_id,
            "task_id": task_id,
            "progress": round(float(progress), 2),
            "running": running,
            "speed_kbps": round(speed, 1),
            "eta_s": round(eta, 1),
            "engine": engine,
        }

    def live(self) -> dict[int, dict[str, Any]]:
        """Current in-memory transfer metrics, keyed by movie id."""
        with self._lock:
            return {
                movie_id: {
                    "task_id": job.task_id,
                    "progress": round(job.progress, 2),
                    "running": bool(job.alive and job.running.is_set() and not job.parked),
                    "speed_kbps": round(job.speed_kbps, 1),
                    "eta_s": round(job.eta_s, 1),
                    "downloaded_bytes": job.downloaded_bytes,
                    "total_bytes": job.total_bytes,
                    "filename": job.filename,
                    "error": job.error,
                    "engine": job.engine,
                    "magnet": job.magnet,
                    "stream_path": job.stream_path,
                    "stream_total": job.stream_total,
                    "stream_available": job.stream_available,
                }
                for movie_id, job in self._jobs.items()
            }

    def stream_source(self, movie_id: int) -> Optional[tuple[Path, int, int]]:
        """The partial file that can be served *right now*, live or not.

        Returns ``(path, total_bytes, available_bytes)`` where
        ``available_bytes`` is the contiguous verified run from the start of the
        file -- the only region that can be served without splicing in
        unwritten holes.

        The in-memory job has the freshest aria2c bitfield and wins; when there
        is no live job (paused, interrupted, app restarted, or disconnected) the
        persisted window is used instead. That fallback is what makes offline
        range streaming work with the network fully down.
        """
        with self._lock:
            job = self._jobs.get(movie_id)
            window = (
                None
                if job is None
                else (job.stream_path, job.stream_total, job.stream_available)
            )
        if window is None or not window[0]:
            persisted = db.get_stream_state(movie_id, target=self.target)
            if persisted is None:
                return None
            window = (persisted["path"], persisted["total"], persisted["available"])
        path_value, total, available = window
        if not path_value or int(available or 0) <= 0:
            return None
        path = Path(str(path_value))
        if not path.is_file():
            return None
        size = _file_size(path)
        return path, (int(total or 0) or size), min(int(available), size)


def _file_size(path: Path) -> int:
    """Size of ``path`` in bytes, or ``0`` when it cannot be read yet."""
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _bitfield_bit(bitfield: bytes, index: int) -> bool:
    """True when piece ``index`` is present in aria2c's hex piece bitmap.

    aria2c documents the bitfield as "the highest bit corresponds to the piece
    at index 0", i.e. piece ``n`` lives in byte ``n // 8`` at bit
    ``0x80 >> (n % 8)``.
    """
    byte = index >> 3
    if byte < 0 or byte >= len(bitfield):
        return False
    return bool(bitfield[byte] & (0x80 >> (index & 7)))


def _aria2c_stream_window(status: dict[str, Any]) -> Optional[tuple[str, int, int]]:
    """Locate the streamable region of a live aria2c transfer.

    Returns ``(media_path, total_length, available_bytes)`` for the largest
    selected video file, or ``None`` when the transfer holds no such file yet.

    ``available_bytes`` is computed from the piece bitfield rather than the raw
    on-disk size. aria2c writes each verified piece at its offset, so with
    ``--file-allocation=none`` the file can contain unwritten holes; serving the
    file's current length would feed a video player zeros. Walking the bitfield
    from the file's first piece gives the exact contiguous prefix that is safe
    to serve. When there is no bitfield (e.g. a plain HTTP download) the file
    size is used, which is correct because ``--stream-piece-selector=inorder``
    fetches those segments from the beginning.
    """
    files = status.get("files")
    if not isinstance(files, list):
        return None

    chosen: Optional[tuple[str, int, int]] = None  # (path, length, file_offset)
    offset = 0
    for entry in files:
        if not isinstance(entry, dict):
            continue
        try:
            length = int(entry.get("length") or 0)
        except (TypeError, ValueError):
            length = 0
        path = str(entry.get("path") or "").strip()
        selected = str(entry.get("selected", "true")).lower() != "false"
        if (
            length > 0
            and path
            and selected
            and Path(path).suffix.lower() in _MEDIA_SUFFIXES
            and (chosen is None or length > chosen[1])
        ):
            chosen = (path, length, offset)
        offset += length

    if chosen is None:
        return None
    path, length, file_offset = chosen

    on_disk = _file_size(Path(path))
    available = on_disk

    try:
        piece_length = int(status.get("pieceLength") or 0)
    except (TypeError, ValueError):
        piece_length = 0
    raw_bitfield = str(status.get("bitfield") or "")
    bitfield = b""
    if raw_bitfield:
        try:
            bitfield = bytes.fromhex(raw_bitfield)
        except ValueError:
            bitfield = b""

    if piece_length > 0 and bitfield:
        first = file_offset // piece_length
        last = (file_offset + length - 1) // piece_length
        prefix = 0
        for piece in range(first, last + 1):
            if not _bitfield_bit(bitfield, piece):
                break
            prefix = min((piece + 1) * piece_length, file_offset + length) - file_offset
        available = min(prefix, on_disk)

    return path, length, max(0, min(available, length))


# --------------------------------------------------------------------------- #
# Flask application
# --------------------------------------------------------------------------- #

app = Flask(__name__)
app.json.ensure_ascii = False
manager = DownloadManager(DB_TARGET)
atexit.register(manager.shutdown)


def _queue_payload() -> dict[str, Any]:
    """Queue snapshot with the live in-memory metrics merged in.

    Shared by ``GET /api/queue`` and the Socket.IO ``snapshot`` event so the
    polling fallback and the real-time channel report identical numbers.
    """
    live = manager.live()
    tasks = db.get_queue(target=DB_TARGET)
    posters = {m["id"]: m.get("poster_url") for m in db.list_movies(target=DB_TARGET)}
    for task in tasks:
        task["movie_poster_url"] = posters.get(task["movie_id"])
        snapshot = live.get(task["movie_id"])
        if snapshot is not None:
            task["progress"] = snapshot["progress"]
            task["running"] = snapshot["running"]
            task["speed_kbps"] = snapshot["speed_kbps"]
            task["eta_s"] = snapshot["eta_s"]
            task["downloaded_bytes"] = snapshot["downloaded_bytes"]
            task["total_bytes"] = snapshot["total_bytes"]
            task["engine"] = snapshot.get("engine") or task.get("engine")
            task["magnet"] = snapshot.get("magnet") or task.get("magnet")
            task["streamable"] = bool(snapshot.get("stream_available"))
            task["stream_bytes"] = int(snapshot.get("stream_available") or 0)
            task["stream_total"] = int(snapshot.get("stream_total") or 0)
            task["offline_ready"] = bool(task.get("offline_ready"))
        else:
            task["running"] = False
            task["speed_kbps"] = float(task.get("speed_kbps") or 0.0)
            task["eta_s"] = float(task.get("eta_s") or 0.0)
            # Offline playback: with no live job the persisted window is what
            # the player can still reach, so surface it instead of a flat 0.
            persisted = task.get("stream_available")
            task["streamable"] = bool(persisted and int(persisted) > 0)
            task["stream_bytes"] = int(persisted or 0)
            task["stream_total"] = int(task.get("stream_total") or 0)
            task["offline_ready"] = bool(task.get("offline_ready"))
    return {
        "count": len(tasks),
        "queue": tasks,
        "stats": db.queue_stats(target=DB_TARGET),
        "speed": manager.speed(),
        "engine": ENGINE_NAME,
        "engines": list(SUPPORTED_ENGINES),
        "network": {
            "online": NETWORK.online,
            "known": NETWORK.known,
            "monitored": NETWORK.enabled,
        },
        "realtime": socketio is not None,
    }


# --------------------------------------------------------------------------- #
# Real-time transport (Flask-SocketIO)
# --------------------------------------------------------------------------- #
if SOCKETIO_ENABLED and SocketIO is not None:  # pragma: no branch - env driven
    socketio = SocketIO(
        app,
        async_mode="threading",          # no eventlet/gevent needed
        cors_allowed_origins=SOCKETIO_CORS or None,
        message_queue=SOCKETIO_MESSAGE_QUEUE,
        logger=False,
        engineio_logger=False,
    )

    def _snapshot_payload() -> dict[str, Any]:
        """Everything a freshly connected client needs to draw the UI."""
        return {
            "queue": _queue_payload(),
            "activity_latest": _activity_latest(),
        }

    @socketio.on("connect")
    def _socket_connect() -> None:
        emit("snapshot", _snapshot_payload())

    @socketio.on("request_snapshot")
    def _socket_request_snapshot() -> None:
        emit("snapshot", _snapshot_payload())


class ApiError(Exception):
    """Raised for client mistakes; rendered as JSON."""

    def __init__(self, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status = status


@app.errorhandler(ApiError)
def _handle_api_error(error: ApiError) -> tuple[Response, int]:
    return jsonify({"ok": False, "error": error.message}), error.status


@app.errorhandler(404)
def _handle_404(_: Any) -> tuple[Response, int]:
    return jsonify({"ok": False, "error": "not found"}), 404


@app.errorhandler(Exception)
def _handle_unexpected(error: Exception) -> tuple[Response, int]:
    if isinstance(error, HTTPException):
        return jsonify({"ok": False, "error": error.description}), error.code or 500
    LOGGER.exception("unhandled error: %s", error)
    return jsonify({"ok": False, "error": "internal server error"}), 500


def _movie_id_from_route(raw: str) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError) as exc:
        raise ApiError("movie id must be an integer", 400) from exc


# -- search ------------------------------------------------------------------ #


@app.get("/search")
def search() -> Response:
    """IMDb search -> structured JSON for the frontend."""
    query = (request.args.get("q") or request.args.get("query") or "").strip()
    try:
        limit = _int_arg(request.args.get("limit"), name="limit", default=SEARCH_LIMIT_DEFAULT)
    except ValueError as exc:
        raise ApiError(str(exc)) from exc
    limit = max(1, min(SEARCH_LIMIT_MAX, limit))

    try:
        payload = search_movies(query, limit)
    except ValueError as exc:
        raise ApiError(str(exc)) from exc
    return jsonify({"ok": True, **payload})


# -- download ---------------------------------------------------------------- #


@app.route("/download", methods=["GET", "POST"])
def download() -> tuple[Response, int]:
    """Enqueue a movie and start its real download job (yt-dlp or aria2c)."""
    data = _payload()
    title = _clip(data.get("title"), 500)
    if not title:
        raise ApiError("'title' is required")

    autostart = str(data.get("autostart", "1")).lower() not in {"0", "false", "no"}
    source_url = _clip(data.get("url"), 2000)
    magnet = _clip(data.get("magnet"), 4000)
    year = _clip(data.get("year"), 10)

    # A magnet pasted as ``url`` is honored by the aria2c engine.
    if source_url and source_url.lower().startswith("magnet:"):
        magnet = magnet or source_url
        source_url = None

    engine = (_clip(data.get("engine"), 40) or ENGINE_NAME).lower()
    if engine not in SUPPORTED_ENGINES:
        raise ApiError(
            f"unsupported engine {engine!r}; choose one of {', '.join(SUPPORTED_ENGINES)}"
        )
    if engine == ARIA2C_ENGINE_NAME and not ARIA2C_LOCATION:
        raise ApiError("aria2c engine requested but aria2c is not installed", 503)

    # Rule 1 -- keep player pages away from aria2c. It cannot extract a video
    # from one (it would fetch the page's HTML), so route the request to the
    # engine that can rather than queueing a transfer that is bound to fail.
    if engine == ARIA2C_ENGINE_NAME and source_url:
        player_host = _player_page_host(source_url)
        if player_host:
            engine = ENGINE_NAME
            log_activity(
                "download.reroute",
                f"{title}: aria2c cannot read {player_host} pages - "
                f"using {ENGINE_NAME} instead",
                level="warn",
            )

    # Rule 2 -- aria2c needs a magnet or a direct file URL, so resolve a YTS
    # torrent for the title here instead of falling back to a YouTube search.
    # Resolving before the queue row is written means a missing source reaches
    # the client immediately, and the magnet found is what the aria2c worker is
    # actually handed.
    if engine == ARIA2C_ENGINE_NAME and not (magnet or source_url):
        magnet, detail = _yts_magnet(title, year)
        if not magnet:
            raise ApiError(
                f"{detail}. Use the {ENGINE_NAME} engine or supply a magnet link",
                422,
            )
        log_activity("download.resolve", f"{title}: {detail}")

    # Safety: never start a transfer while the drive is below the free buffer.
    try:
        disk = db.ensure_disk_space(DOWNLOAD_DIR)
    except db.InsufficientDiskSpaceError as exc:
        log_activity("disk.warning", f"Download blocked - {exc}", level="warn")
        raise ApiError(str(exc), 507) from exc

    try:
        queued = db.add_movie_to_queue(
            title,
            rating=_as_float(data.get("rating")),
            plot=_clip(data.get("plot")),
            cast=_clip(data.get("cast")),
            runtime=_clip(data.get("runtime"), 100),
            poster_url=_clip(data.get("poster_url"), 1000),
            local_path=_clip(data.get("local_path"), 1000),
            year=year,
            engine=engine,
            magnet=magnet,
            target=DB_TARGET,
        )
    except db.DatabaseError as exc:
        raise ApiError(str(exc), 409) from exc
    except ValueError as exc:
        raise ApiError(str(exc)) from exc

    task = db.get_task_for_movie(queued["movie_id"], live_only=False, target=DB_TARGET)
    progress = float(task["progress"]) if task else 0.0

    job = manager.start(
        queued["movie_id"],
        queued["task_id"],
        progress=progress,
        autostart=autostart,
        url=source_url,
        title=title,
        year=year,
        engine=engine,
        magnet=magnet,
    )
    if queued["reused"]:
        log_activity(
            "download.reused",
            f"{title} is already in the download queue",
            level="warn",
            movie_id=queued["movie_id"],
        )
    else:
        log_activity(
            "download.queued",
            f"{title} added to the queue",
            movie_id=queued["movie_id"],
        )
    # Push the new row instantly; the activity event only carries a toast.
    manager.publish(queued["movie_id"])
    body = {
        "ok": True,
        "movie_id": queued["movie_id"],
        "task_id": queued["task_id"],
        "created": queued["created"],
        "reused": queued["reused"],
        "progress": round(job.progress, 2),
        "autostart": autostart,
        "free_gb": round(disk.free_gb, 2),
        "engine": job.engine,
    }
    return jsonify(body), 201 if queued["created"] else 200


# -- controls ---------------------------------------------------------------- #


@app.route("/pause/<movie_id>", methods=["GET", "POST"])
def pause(movie_id: str) -> Response:
    pk = _movie_id_from_route(movie_id)
    try:
        payload = manager.pause(pk)
    except LookupError as exc:
        raise ApiError(str(exc), 404) from exc
    except (ValueError, db.DatabaseError) as exc:
        raise ApiError(str(exc), 409) from exc
    movie = db.get_movie(pk, target=DB_TARGET) or {}
    log_activity(
        "download.paused",
        f"Paused {movie.get('title') or f'Movie #{pk}'}",
        level="warn",
        movie_id=pk,
    )
    return jsonify({"ok": True, "state": "paused", **payload})


@app.route("/resume/<movie_id>", methods=["GET", "POST"])
def resume(movie_id: str) -> Response:
    pk = _movie_id_from_route(movie_id)
    try:
        payload = manager.resume(pk)
    except LookupError as exc:
        raise ApiError(str(exc), 404) from exc
    except (ValueError, db.DatabaseError) as exc:
        raise ApiError(str(exc), 409) from exc
    movie = db.get_movie(pk, target=DB_TARGET) or {}
    log_activity(
        "download.resumed",
        f"Resumed {movie.get('title') or f'Movie #{pk}'}",
        movie_id=pk,
    )
    return jsonify({"ok": True, "state": "active", **payload})


def _remove_download_files(
    movie: Optional[dict[str, Any]], task: Optional[dict[str, Any]]
) -> None:
    """Delete a movie's local file(s), never touching anything outside the dir."""
    root = Path(DOWNLOAD_DIR).resolve()
    targets: set[Path] = set()
    local_path = (movie or {}).get("local_path")
    if local_path:
        targets.add(Path(local_path))
    if task is not None:
        # Escape the literal ``[cvN]`` token: glob treats ``[...]`` as a
        # character class, so an unescaped pattern never matches our own names
        # (nor the ``_web.mp4`` copy derived from them).
        targets.update(Path(DOWNLOAD_DIR).glob(f"*[[]cv{task['id']}]*"))
    for path in targets:
        try:
            resolved = path.resolve()
        except OSError:  # pragma: no cover - defensive
            continue
        if resolved == root or root not in resolved.parents:
            continue  # never delete files outside the download directory
        for candidate in (resolved, Path(str(resolved) + ".part")):
            try:
                if candidate.is_file():
                    candidate.unlink()
                    LOGGER.info("removed local file %s", candidate)
            except OSError as exc:  # pragma: no cover - defensive
                LOGGER.warning("could not remove %s: %s", candidate, exc)


@app.route("/delete/<movie_id>", methods=["DELETE", "GET", "POST"])
def delete(movie_id: str) -> Response:
    """Stop the job, cancel its task, delete the row and clean up local files."""
    pk = _movie_id_from_route(movie_id)
    manager.stop(pk)

    movie = db.get_movie(pk, target=DB_TARGET)
    task = db.get_task_for_movie(pk, live_only=False, target=DB_TARGET)
    if task is not None and task["state"] not in db.FINAL_TASK_STATES:
        try:
            db.cancel_task(task["id"], target=DB_TARGET)
        except db.DatabaseError:  # pragma: no cover - already final
            pass

    deleted = db.delete_movie(pk, target=DB_TARGET)
    if not deleted:
        raise ApiError(f"movie {pk} not found", 404)
    _remove_download_files(movie, task)
    log_activity("download.deleted", f"Removed movie #{pk} from the library", level="warn", movie_id=pk)
    # Real-time: drop the row everywhere and resync the vault.
    _broadcast("queue.remove", {"movie_id": pk})
    _broadcast("library.changed", {"reason": "deleted", "movie_id": pk})
    return jsonify({"ok": True, "deleted_movie_id": pk, "engine": ENGINE_NAME})


# -- read-only helpers for the frontend -------------------------------------- #


@app.get("/api/queue")
def api_queue() -> Response:
    """Polling fallback for the queue (the socket pushes the same payload)."""
    return jsonify({"ok": True, **_queue_payload()})


@app.get("/api/library")
def api_library() -> Response:
    """Vault rows, each carrying an explicit ``yt_trailer_code`` field.

    Rows arrive from SQLite without any trailer metadata, so every record is
    run through :func:`_attach_trailer_codes` before it reaches the SPA. A row
    whose download is still running also carries live ``streamable`` metrics so
    the UI can offer "Stream / Play Now" before the transfer finishes.
    """
    movies = _attach_trailer_codes(db.list_movies(target=DB_TARGET))
    live = manager.live()
    for movie in movies:
        mid = movie.get("id")
        snapshot = live.get(mid)
        available = int((snapshot or {}).get("stream_available") or 0)
        total = int((snapshot or {}).get("stream_total") or 0)
        offline_ready = False
        if available <= 0 and mid is not None:
            # No live job: a paused/interrupted/disconnected transfer may still
            # have a playable prefix, which is what makes offline streaming work.
            state = db.get_stream_state(mid, target=DB_TARGET)
            if state is not None:
                available = int(state.get("available") or 0)
                total = int(state.get("total") or 0)
                offline_ready = bool(state.get("offline_ready"))
        movie["streamable"] = available > 0
        movie["stream_bytes"] = available
        movie["stream_total"] = total
        movie["offline_ready"] = offline_ready
    return jsonify({"ok": True, "movies": movies})


@app.get("/api/trailer")
def api_trailer() -> Response:
    """Resolve a title's YouTube trailer code across the whole UI.

    Movie records returned by ``/search`` and ``/api/discover`` already carry
    ``yt_trailer_code``; this covers the remainder - the vault rows and any
    search result past the enrichment window - and is memoised server-side.
    """
    title = str(request.args.get("title", "")).strip()
    if not title:
        raise ApiError("a movie 'title' is required", 400)
    year = str(request.args.get("year", "")).strip() or None
    code = _trailer_code(title, year)
    return jsonify(
        {
            "ok": True,
            "title": title,
            "year": year,
            "yt_trailer_code": code,
            "embed_url": YT_EMBED_URL.format(code=code) if code else None,
        }
    )


@app.get("/api/activity")
def api_activity() -> Response:
    """Recent system events, oldest-first, for toasts and the activity panel.

    Pass ``?since=<id>`` to fetch only what happened after the given event.
    """
    with _activity_lock:
        all_events = list(ACTIVITY)
    latest = all_events[-1]["id"] if all_events else 0

    since_id = 0
    raw_since = request.args.get("since")
    if raw_since not in (None, ""):
        try:
            since_id = int(str(raw_since))
        except ValueError as exc:
            raise ApiError("'since' must be an integer") from exc
    events = [event for event in all_events if event["id"] > since_id]

    try:
        limit = _int_arg(request.args.get("limit"), name="limit", default=60)
    except ValueError as exc:
        raise ApiError(str(exc)) from exc
    limit = max(1, min(ACTIVITY_MAX, limit))
    return jsonify(
        {
            "ok": True,
            "events": events[-limit:],
            "latest": latest,
            "count": len(events),
        }
    )


@app.get("/api/speed")
def api_speed_get() -> Response:
    """Current transfer-rate cap applied to yt-dlp downloads."""
    return jsonify({"ok": True, "speed": manager.speed(), "presets": list(SPEED_PRESETS)})


@app.route("/api/speed", methods=["POST"])
def api_speed_set() -> Response:
    """Adjust the transfer-rate cap via ``preset`` and/or ``rate_limit``.

    The cap is read when a yt-dlp transfer starts (or resumes), so it applies to
    the next segment of every managed download.
    """
    data = _payload()
    try:
        speed = manager.set_speed(
            preset=_clip(data.get("preset"), 20),
            rate_limit=_clip(data.get("rate_limit"), 20),
        )
    except ValueError as exc:
        raise ApiError(str(exc)) from exc

    cap = speed.get("rate_limit")
    log_activity(
        "speed.changed",
        "Download speed cap set to " + (f"{cap}/s" if cap else "unlimited"),
    )
    _broadcast("speed.update", speed)
    return jsonify({"ok": True, "speed": speed, "presets": list(SPEED_PRESETS)})


@app.get("/api/health")
def api_health() -> Response:
    """Liveness, disk headroom and engine state for the dashboard header."""
    try:
        disk = db.check_disk_space(DOWNLOAD_DIR)
        disk_payload: dict[str, Any] = {
            "path": str(disk.path),
            "free_bytes": disk.free_bytes,
            "total_bytes": disk.total_bytes,
            "free_gb": round(disk.free_gb, 2),
            "total_gb": round(disk.total_gb, 2),
            "buffer_gb": round(disk.min_free_bytes / (1024**3), 2),
            "ok": disk.ok,
        }
    except OSError as exc:  # pragma: no cover - defensive
        disk_payload = {"ok": False, "error": str(exc)}

    return jsonify(
        {
            "ok": True,
            "app": APP_NAME,
            "version": APP_VERSION,
            "database": DB_TARGET,
            "backend": db.database_backend(DB_TARGET),
            "download_dir": DOWNLOAD_DIR,
            "disk": disk_payload,
            "engine": ENGINE_NAME,
            "engine_available": _ytdlp_available(),
            "engines": {
                "default": ENGINE_NAME,
                "supported": list(SUPPORTED_ENGINES),
                "aria2c": {
                    "available": bool(ARIA2C_LOCATION),
                    "path": ARIA2C_LOCATION,
                },
            },
            "speed": manager.speed(),
            "presets": list(SPEED_PRESETS),
            "queue": db.queue_stats(target=DB_TARGET),
            "network": {
                "online": NETWORK.online,
                "known": NETWORK.known,
                "monitored": NETWORK.enabled,
            },
        }
    )


@app.get("/api/discover")
def api_discover() -> Response:
    """Streaming-style homepage: a featured hero plus Trending / Latest rows.

    Titles are curated but every field (poster, rating, plot, cast, runtime) is
    resolved through the same provider chain as /search. Pass ``?refresh=1`` to
    rebuild the cached payload.
    """
    refresh = str(request.args.get("refresh", "")).lower() in {"1", "true", "yes"}
    try:
        payload = discover_home(force=refresh)
    except Exception as exc:  # pragma: no cover - defensive
        LOGGER.warning("discover build failed: %s", exc)
        raise ApiError("could not build the homepage", 502) from exc
    return jsonify({"ok": True, **payload})


@app.get("/progress/<movie_id>")
def progress(movie_id: str) -> Response:
    pk = _movie_id_from_route(movie_id)
    task = db.get_task_for_movie(pk, live_only=False, target=DB_TARGET)
    if task is None:
        raise ApiError(f"movie {pk} has no download task", 404)
    live = manager.live().get(pk)
    progress_value = live["progress"] if live else float(task["progress"])
    running = bool(live["running"]) if live else False
    return jsonify(
        {
            "ok": True,
            "movie_id": pk,
            "task_id": task["id"],
            "state": task["state"],
            "progress": round(progress_value, 2),
            "running": running,
            "movie_status": (db.get_movie(pk, target=DB_TARGET) or {}).get("status"),
        }
    )


# -- media streaming + single-page UI ---------------------------------------- #

#: Media content types pinned by suffix. ``mimetypes`` does not map every
#: container on every platform (and never maps an extension-less file), so the
#: player always receives a concrete ``video/*`` type where we recognise one.
_STREAM_CONTENT_TYPES = {
    ".mp4": "video/mp4",
    ".m4v": "video/mp4",
    ".webm": "video/webm",
    ".mkv": "video/x-matroska",
    ".mov": "video/quicktime",
    ".avi": "video/x-msvideo",
    ".flv": "video/x-flv",
    ".ts": "video/mp2t",
    ".m2ts": "video/mp2t",
    ".ogv": "video/ogg",
}

#: Bytes read per iteration while writing a response body.
STREAM_CHUNK_SIZE = 1024 * 1024  # 1 MiB


class _RangeNotSatisfiable(Exception):
    """A ``Range`` header that is well-formed but cannot be met (HTTP 416)."""


def _stream_content_type(path: Path) -> str:
    return _STREAM_CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream")


def _fmt_bytes(size: float) -> str:
    """Render a byte count for humans (``95.3 MB`` / ``1.4 GB``)."""
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024.0 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024.0
    return f"{value:.1f} TB"  # pragma: no cover - loop always returns


def _validate_video_file(
    path: Path,
    *,
    root: Optional[Path] = None,
    suffixes: Optional[AbstractSet[str]] = None,
    min_bytes: int = 0,
) -> Optional[str]:
    """Return the reason ``path`` is not an acceptable finished movie, else ``None``.

    Every "download succeeded" claim is funnelled through here before the
    database is touched, so a torrent's ``.torrent`` sidecar, a ``.rar`` part
    or a three-minute sample can never be written out as a ``COMPLETED`` row.
    ``root`` (when given) additionally requires the file to live inside the
    download directory, and ``min_bytes`` rejects suspiciously small payloads.
    """
    wanted = _MEDIA_SUFFIXES if suffixes is None else suffixes
    if not path.exists():
        return f"expected file does not exist: {path}"
    if path.is_dir():
        return f"{path.name} is a directory, not a video file"
    if root is not None:
        try:
            inside = path.resolve().is_relative_to(Path(root).resolve())
        except (OSError, ValueError):  # pragma: no cover - defensive
            inside = False
        if not inside:
            return f"{path.name} was written outside the download directory"
    suffix = path.suffix.lower()
    if suffix == ".torrent":
        return f"only a .torrent sidecar was saved ({path.name}), not the video itself"
    if suffix not in wanted:
        return (
            f"'{path.name}' is not a supported video "
            f"({' '.join(sorted(wanted))})"
        )
    try:
        size = path.stat().st_size
    except OSError as exc:  # pragma: no cover - unreadable file
        return f"could not read {path.name}: {exc}"
    if size <= min_bytes:
        return (
            f"'{path.name}' is only {_fmt_bytes(size)}, under the "
            f"{_fmt_bytes(min_bytes)} minimum - it looks like a sample or metadata"
        )
    return None


def _video_files_in(
    directory: Path,
    suffixes: Optional[AbstractSet[str]] = None,
) -> list[Path]:
    """Video files under ``directory`` (recursive), largest first.

    Torrent-style downloads land as a folder; the biggest file is almost always
    the feature itself rather than a bundled sample or extra. ``suffixes``
    narrows the accepted containers. A plain file is returned as a single
    candidate so aria2c's exact reported path is vetted the same way as a
    directory scan.
    """
    wanted = _MEDIA_SUFFIXES if suffixes is None else suffixes
    if directory.is_file():
        return [directory] if directory.suffix.lower() in wanted else []
    found: list[tuple[int, str, Path]] = []
    for path in directory.rglob("*"):
        try:
            if path.is_file() and path.suffix.lower() in wanted:
                found.append((path.stat().st_size, str(path).lower(), path))
        except OSError:  # a vanishing or unreadable entry must not break the walk
            continue
    found.sort(key=lambda item: (-item[0], item[1]))
    return [item[2] for item in found]


def _resolve_stream_path(candidate: Optional[str]) -> Optional[Path]:
    """Resolve a stored local path to a servable video file.

    Accepts either a file directly, or a directory (a download that produced a
    folder) in which case the largest video file inside it is returned. Returns
    ``None`` when nothing playable can be found.
    """
    if not candidate:
        return None
    path = Path(candidate)
    if path.is_file():
        return path
    if path.is_dir():
        videos = _video_files_in(path)
        if videos:
            return videos[0]
    return None


# -- web-playback conversion (post-download) --------------------------------- #

#: Containers an HTML5 ``<video>`` plays inline without conversion. Everything
#: else (``.mkv``/``.avi``/``.flv``/``.ts``/``.mov``/...) is converted so the
#: in-app player can decode and seek it smoothly.
WEB_PLAYABLE_SUFFIXES = frozenset({".mp4", ".m4v", ".webm", ".ogv", ".ogg"})

#: Suffix of the browser-compatible copy produced by :func:`convert_to_web_mp4`.
WEB_COPY_SUFFIX = "_web.mp4"

#: Automatic post-download conversion. Set ``CINEVAULT_TRANSCODE=0`` to disable.
TRANSCODE_ENABLED: bool = (
    os.environ.get("CINEVAULT_TRANSCODE", "1").strip().lower()
    not in {"0", "false", "no", "off"}
)


def _needs_web_conversion(path: Optional[str]) -> bool:
    """``True`` when ``path`` is a known video container a browser can't play.

    Extension-less or non-video files (e.g. a saved HTML error page) are left
    alone, so no ffmpeg process is spawned for something it cannot read.
    """
    if not path:
        return False
    suffix = Path(path).suffix.lower()
    return suffix in _MEDIA_SUFFIXES and suffix not in WEB_PLAYABLE_SUFFIXES


def _remove_quietly(path: Path) -> None:
    """Best-effort delete of a partial or superseded output file."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _nonempty_file(path: Path) -> bool:
    """``True`` when ``path`` exists and holds at least one byte."""
    try:
        return path.is_file() and path.stat().st_size > 0
    except OSError:
        return False


def _probe_media(path: str) -> tuple[Optional[str], Optional[str]]:
    """Return ``(video_codec, audio_codec)`` for ``path`` via ffprobe.

    Either element is ``None`` when ffprobe is unavailable, the file cannot be
    read, or that stream is absent. Used to decide whether a lossless remux is
    enough or a full re-encode is required.
    """
    if not FFPROBE_LOCATION:
        return None, None
    cmd = [
        FFPROBE_LOCATION,
        "-v", "error",
        "-show_entries", "stream=codec_type,codec_name",
        "-of", "json",
        path,
    ]
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60
        )
    except (OSError, subprocess.SubprocessError):
        return None, None
    if proc.returncode != 0:
        return None, None
    try:
        data = json.loads(proc.stdout.decode("utf-8", "replace") or "{}")
    except ValueError:
        return None, None
    video: Optional[str] = None
    audio: Optional[str] = None
    for stream in data.get("streams", []):
        kind = stream.get("codec_type")
        codec = (stream.get("codec_name") or "").lower() or None
        if kind == "video" and video is None:
            video = codec
        elif kind == "audio" and audio is None:
            audio = codec
    return video, audio


def _reencode_cmd(src: str, dst: str) -> list[str]:
    """ffmpeg command that transcodes ``src`` to a browser-safe H.264/AAC MP4."""
    return [
        FFMPEG_LOCATION or "ffmpeg",
        "-y", "-i", src,
        "-c:v", "libx264", "-crf", "23", "-preset", "veryfast",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        dst,
    ]


def _remux_cmd(src: str, dst: str) -> list[str]:
    """ffmpeg command that rewraps ``src`` into MP4 without re-encoding."""
    return [
        FFMPEG_LOCATION or "ffmpeg",
        "-y", "-i", src,
        "-c", "copy", "-movflags", "+faststart",
        dst,
    ]


def _web_conversion_cmds(src: str, dst: str) -> list[list[str]]:
    """Ordered ffmpeg attempts (cheapest first) that yield a playable MP4.

    A file already H.264 with AAC/MP3 audio is only *remuxed* (fast, lossless);
    one with compatible video but incompatible audio keeps the video and
    re-encodes just the audio; anything else is fully re-encoded. Every list
    ends with a full re-encode as a last-resort fallback.
    """
    video, audio = _probe_media(src)
    if video == "h264" and audio in (None, "aac", "mp3"):
        return [_remux_cmd(src, dst), _reencode_cmd(src, dst)]
    if video == "h264":
        return [
            [
                FFMPEG_LOCATION or "ffmpeg",
                "-y", "-i", src,
                "-c:v", "copy",
                "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart",
                dst,
            ],
            _reencode_cmd(src, dst),
        ]
    return [_reencode_cmd(src, dst)]


def _run_ffmpeg(cmd: list[str]) -> bool:
    """Run one ffmpeg attempt; return ``True`` only on a clean exit."""
    try:
        proc = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except (OSError, subprocess.SubprocessError) as exc:
        LOGGER.warning("ffmpeg could not start: %s", exc)
        return False
    if proc.returncode != 0:
        tail = (proc.stderr or b"").decode("utf-8", "replace").strip().splitlines()[-3:]
        LOGGER.warning("ffmpeg exited %s: %s", proc.returncode, " | ".join(tail))
        return False
    return True


def convert_to_web_mp4(input_path: str) -> str:
    """Return a browser-playable MP4 copy of ``input_path``.

    Mirrors the standalone helper but prefers the app's resolved ffmpeg,
    *remuxes* instead of re-encoding whenever the codecs already allow it, skips
    ``*_web.mp4`` inputs, and returns ``input_path`` unchanged when conversion is
    disabled, ffmpeg is missing, or every attempt fails - so callers can always
    fall back to the original file.
    """
    if not TRANSCODE_ENABLED or not FFMPEG_LOCATION:
        return input_path
    src = Path(input_path)
    if not src.is_file() or src.stem.lower().endswith("_web"):
        return input_path
    output = src.with_name(src.stem + WEB_COPY_SUFFIX)
    if _nonempty_file(output):
        return str(output)  # cached from an earlier run
    for cmd in _web_conversion_cmds(str(src), str(output)):
        if _run_ffmpeg(cmd) and _nonempty_file(output):
            LOGGER.info("web conversion produced %s", output)
            return str(output)
        _remove_quietly(output)  # clear any partial output before the next try
    LOGGER.warning("web conversion failed for %s; keeping original", src)
    return input_path


def _parse_range_header(header: str, size: int) -> Optional[tuple[int, int]]:
    """Resolve a ``Range`` header to an inclusive ``(start, end)`` byte window.

    Supports the single-range forms ``bytes=start-end``, ``bytes=start-`` and
    the suffix form ``bytes=-n`` (the final ``n`` bytes). Returns ``None`` when
    the header is absent or should simply be ignored (unknown unit or
    unparseable), so the caller can answer with a full ``200``. Raises
    :class:`_RangeNotSatisfiable` for a valid range that cannot be met.
    """
    value = (header or "").strip()
    if not value.lower().startswith("bytes="):
        return None
    # ``<video>`` only ever asks for a single range; coalesce by taking the first.
    spec = value[len("bytes="):].split(",", 1)[0].strip()
    if "-" not in spec:
        return None
    start_raw, _, end_raw = spec.partition("-")
    start_raw, end_raw = start_raw.strip(), end_raw.strip()

    if not start_raw:                       # suffix range: the final N bytes
        try:
            suffix = int(end_raw)
        except ValueError:
            return None
        if suffix <= 0 or size == 0:
            raise _RangeNotSatisfiable()
        return max(0, size - suffix), size - 1

    try:
        start = int(start_raw)
    except ValueError:
        return None
    if not end_raw:                         # open-ended: start .. EOF
        end = size - 1
    else:
        try:
            end = int(end_raw)
        except ValueError:
            return None
    end = min(end, size - 1)                # a request past EOF is clamped
    if start < 0 or start > end or start >= size:
        raise _RangeNotSatisfiable()
    return start, end


def _file_chunks(path: Path, start: int, length: int) -> Iterator[bytes]:
    """Yield ``length`` bytes from ``path`` beginning at ``start`` (1 MiB chunks)."""
    remaining = length
    with path.open("rb") as handle:
        handle.seek(start)
        while remaining > 0:
            chunk = handle.read(min(STREAM_CHUNK_SIZE, remaining))
            if not chunk:
                break
            remaining -= len(chunk)
            yield chunk


def _range_start(header: str) -> Optional[int]:
    """Byte index a ``Range`` header starts at, or ``None`` when unknowable."""
    value = (header or "").strip()
    if not value.lower().startswith("bytes="):
        return None
    spec = value[len("bytes="):].split(",", 1)[0].strip()
    start_raw, separator, _ = spec.partition("-")
    start_raw = start_raw.strip()
    if not separator or not start_raw:
        return None
    try:
        return max(0, int(start_raw))
    except ValueError:
        return None


def _range_response_416(total: int, content_type: str) -> Response:
    """An unsatisfiable-range reply that still advertises the full size."""
    response = Response(status=416, mimetype=content_type)
    response.headers["Content-Range"] = f"bytes */{total}"
    response.headers["Accept-Ranges"] = "bytes"
    response.headers["Cache-Control"] = "no-store"
    return response


def _stream_file(
    path: Path,
    *,
    expected_total: Optional[int] = None,
    available: Optional[int] = None,
) -> Response:
    """Serve ``path`` with byte-range support so the HTML5 player can seek.

    No ``Range`` header -> ``200 OK`` with the whole file. A satisfiable range
    -> ``206 Partial Content`` with ``Content-Range``/``Content-Length``. An
    unmatchable range -> ``416`` with ``Content-Range: bytes */<size>``.

    ``available`` marks a file a live download is still fetching (the "stream
    while downloading" case): only that many bytes from the start of the file
    are safe to send, and ``expected_total`` is the transfer's eventual size.
    This matters because aria2c writes verified pieces out of order -- the file
    can already be its full length on disk while its middle is still unwritten
    holes, so trusting the on-disk size would splice zeros into playback.
    Responses advertise ``expected_total`` (or the full length when it is known)
    so the player treats the resource as growable and keeps asking for more.
    """
    content_type = _stream_content_type(path)
    head_only = request.method == "HEAD"
    range_header = request.headers.get("Range")

    growing = available is not None
    size = max(0, int(available)) if growing else _file_size(path)
    total = max(size, int(expected_total or 0))

    window: Optional[tuple[int, int]] = None
    if range_header:
        try:
            window = _parse_range_header(range_header, size)
        except _RangeNotSatisfiable:
            return _range_response_416(total if growing else size, content_type)

    if window is not None:
        start, end = window
    elif size > 0:
        start, end = 0, size - 1
    elif growing:
        # No verified piece yet: report the eventual size so the player retries.
        return _range_response_416(total, content_type)
    else:
        response = Response(b"", status=200, mimetype=content_type)
        response.headers["Content-Length"] = "0"
        response.headers["Accept-Ranges"] = "bytes"
        response.headers["Cache-Control"] = "no-store"
        return response

    length = end - start + 1
    body = b"" if head_only else _file_chunks(path, start, length)
    status = 206 if (window is not None or growing) else 200
    response = Response(body, status=status, mimetype=content_type)
    if status == 206:
        response.headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        if growing:
            response.headers["X-Stream-Partial"] = "1"
    response.headers["Content-Length"] = str(length)
    response.headers["Accept-Ranges"] = "bytes"
    response.headers["Cache-Control"] = "no-store"
    return response


@app.get("/stream/<movie_id>")
def stream(movie_id: str) -> Response:
    """Stream a movie's file, with HTTP byte-range support.

    ``local_path`` may be a file or a folder (torrent-style download); either
    way a playable video is served. When the movie is still downloading, the
    partial file the engine is writing is served instead, so the feature is
    watchable before the transfer finishes. ``PIK_SAMPLE_VIDEO`` is only a
    last-resort fallback when there is neither.
    """
    pk = _movie_id_from_route(movie_id)
    movie = db.get_movie(pk, target=DB_TARGET)
    if movie is None:
        raise ApiError(f"movie {pk} not found", 404)

    resolved = _resolve_stream_path(movie.get("local_path"))
    if resolved is not None:
        return _stream_file(resolved)

    # Still downloading: stream the contiguous, verified head of the file as
    # aria2c writes the rest of it.
    partial = manager.stream_source(pk)
    if partial is not None:
        path, total, available = partial
        # A player can ask for a byte the transfer has not verified yet. Rather
        # than fail immediately, wait briefly for the download to catch up --
        # that is what keeps playback from stuttering every time it outruns
        # aria2c by a few hundred kilobytes.
        start = _range_start(request.headers.get("Range") or "")
        deadline = time.monotonic() + max(0.0, STREAM_WAIT_TIMEOUT)
        # Only a live, actively-fetching job can "catch up": when the transfer is
        # paused, parked offline, or gone (restarted), the verified prefix is
        # final, so answer immediately with what is on disk.
        snapshot = manager.live().get(pk)
        can_grow = bool(snapshot and snapshot.get("running")) and NETWORK.online
        if can_grow:
            while start is not None and available <= start and time.monotonic() < deadline:
                time.sleep(STREAM_WAIT_INTERVAL)
                again = manager.stream_source(pk)
                if again is None:
                    break
                path, total, available = again
        return _stream_file(path, expected_total=total, available=available)

    sample = _resolve_stream_path(SAMPLE_VIDEO)
    if sample is not None:
        return _stream_file(sample)

    raise ApiError(
        "no local file for this movie yet - finish a download first",
        404,
    )


@app.get("/")
def index() -> str:
    """Sections 1-3: search, download manager and library vault."""
    return render_template("index.html")


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #


def create_app() -> Flask:
    """Idempotent initialisation so WSGI servers (gunicorn/uwsgi) can import."""
    db.init_db(DB_TARGET)
    # Make sure the download root exists before the first transfer (yt-dlp would
    # otherwise be handed a template inside a missing directory).
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    NETWORK.start()
    LOGGER.info(
        "%s ready - database=%s backend=%s download_dir=%s engine=%s%s",
        APP_NAME,
        DB_TARGET,
        db.database_backend(DB_TARGET),
        DOWNLOAD_DIR,
        ENGINE_NAME,
        f" ffmpeg={FFMPEG_LOCATION}"
        if FFMPEG_LOCATION
        else " (ffmpeg not found; separate-stream merges may fail)",
    )
    return app


def _auto_resume_pending() -> int:
    """Reattach the manager to active tasks so a restart resumes each transfer.

    Only tasks already marked ``active`` are restarted: they pick up the partial
    file and aria2c's persisted state. Paused/queued tasks are left to the user
    (or to the network monitor), so a reboot never silently overrides a pause.
    """
    resumed = 0
    for task in db.get_queue(states=["active"], target=DB_TARGET):
        try:
            manager.start(
                int(task["movie_id"]),
                int(task["id"]),
                progress=float(task["progress"]),
                title=task.get("movie_title") or "",
                engine=task.get("engine") or ENGINE_NAME,
                magnet=task.get("magnet"),
                autostart=True,
            )
            resumed += 1
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.warning(
                "auto-resume failed (movie=%s): %s", task.get("movie_id"), exc
            )
    if resumed:
        LOGGER.info("auto-resumed %d download(s) after restart", resumed)
        log_activity(
            "download.resumed",
            f"Restored {resumed} running download(s) after restart",
        )
    return resumed


def main() -> None:
    host = os.environ.get("PIK_HOST", "127.0.0.1")
    port = int(os.environ.get("PIK_PORT", "5000"))
    debug = os.environ.get("PIK_DEBUG", "0").lower() in {"1", "true", "yes"}
    create_app()
    transport = "websocket (Flask-SocketIO)" if socketio is not None else "http polling"
    print(
        f"{APP_NAME} running on http://{host}:{port}  "
        f"(database: {DB_TARGET}, transport: {transport})"
    )
    # Warm the homepage cache in the background so the first visitor is served
    # from memory instead of waiting on the provider lookups.
    if os.environ.get("CINEVAULT_WARM_DISCOVER", "1").lower() not in {"0", "false", "no"}:
        threading.Thread(
            target=discover_home, name="discover-warm", daemon=True
        ).start()
    # use_reloader=False keeps the background thread registry in one process.
    NETWORK.start()
    _auto_resume_pending()
    if socketio is not None:
        socketio.run(
            app,
            host=host,
            port=port,
            debug=debug,
            use_reloader=False,
            allow_unsafe_werkzeug=True,
        )
    else:
        app.run(host=host, port=port, debug=debug, threaded=True, use_reloader=False)


create_app()

if __name__ == "__main__":
    main()
