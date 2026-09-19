"""
The bot's own control panel.

Everything here runs in the web process, which is a *different* process from
the bot: gunicorn serves this while the bot runs beside it. The two never call
each other directly. They meet in MongoDB, in the same `tgleech` collection the
bundled bridge plugin already watches — so a link added here is claimed and run
by the bot within seconds, and what the bot is doing comes back the same way.

That means the panel needs two things from the environment, and nothing else:
the bot token (to find the right collection) and the database URL.

The `tgleech` plugin must be loaded in the bot for any of this to do anything.
Without it the panel still opens and still reports honestly — it simply says
the bot is not answering.
"""

from asyncio import get_running_loop
from hashlib import sha256
from importlib import import_module
from os import getenv
from pathlib import Path
from time import time
from weakref import WeakKeyDictionary

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="web/templates")

# What the panel is locked with. A private bot on a public address is still on
# a public address, and this page can start downloads and restart the bot.
DEFAULT_PASSWORD = "kasun123"

HEARTBEAT_ID = "__bridge__"
CONTROL_ID = "__settings__"
SKIPPED_ID = "__skipped__"
# What the page itself remembers, and the accounts it has been told about.
# Both live beside the queue so they survive a refresh, a new browser and a
# redeploy — the browser keeps nothing but the password.
PREFS_ID = "__panel__"
PEOPLE_ID = "__people__"
NOT_TASKS = {"$nin": [HEARTBEAT_ID, CONTROL_ID, SKIPPED_ID, PREFS_ID, PEOPLE_ID]}

# The bot goes quiet for a few seconds between rounds; longer than this and it
# is not there.
STALE_SECONDS = 60

ENGINES = ("direct", "qb", "jd", "nzb", "ytdl")
MODES = ("leech", "mirror")


def _setting(name, default=""):
    """A setting from the environment, or from config.py beside it."""
    value = getenv(name, "")
    if value:
        return value.strip()
    try:
        settings = import_module("config")
    except ModuleNotFoundError:
        return default
    value = getattr(settings, name, default) or default
    return value.strip() if isinstance(value, str) else value


def panel_password():
    return (_setting("PANEL_PASSWORD", "") or DEFAULT_PASSWORD).strip()


def _guard(given):
    wanted = panel_password()
    if wanted and (given or "").strip() != wanted:
        raise HTTPException(status_code=401, detail="Wrong password.")


def _partition():
    token = str(_setting("BOT_TOKEN", ""))
    bot_id = token.split(":", 1)[0]
    raw = sha256(b"wzmlx_v3_db_partition_salt" + bot_id.encode("utf-8")).hexdigest()
    return f"p_{raw[:24]}"


# A Mongo client belongs to the event loop it was made on, so one is kept per
# loop rather than one for the process. In the worker there is only ever one
# loop; this only matters when something else runs the app, and it is cheaper
# than a client that quietly stops working.
_clients = WeakKeyDictionary()


def _database():
    url = str(_setting("DATABASE_URL", ""))
    if not url:
        return None
    try:
        loop = get_running_loop()
    except RuntimeError:
        loop = None
    client = _clients.get(loop)
    if client is None:
        from pymongo import AsyncMongoClient

        client = AsyncMongoClient(url, serverSelectionTimeoutMS=8000)
        if loop is not None:
            _clients[loop] = client
    return client.wzmlx


def _queue():
    """The bot's queue collection, or None when there is no database to reach."""
    database = _database()
    return None if database is None else database.tgleech[_partition()]


def _settings_store():
    database = _database()
    return None if database is None else database.settings


def _people_store():
    """Where the bot keeps each account's own settings, by Telegram id."""
    database = _database()
    return None if database is None else database.users[_partition()]


async def _control():
    queue = _queue()
    if queue is None:
        return {}
    return await queue.find_one({"_id": CONTROL_ID}) or {}


def _links_in(text):
    """One link per line, ignoring blanks and anything commented out."""
    out = []
    for line in str(text or "").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.append(line)
    return out


def _link_problem(url):
    lowered = url.lower()
    if lowered.startswith(("http://", "https://", "magnet:")):
        return None
    if lowered.startswith("ftp://") or ":" not in lowered:
        return f'"{url[:60]}" is not a link the bot can fetch.'
    return f'"{url[:60]}" is not a link the bot can fetch.'


# ---------------------------------------------------------------------------
# The page
# ---------------------------------------------------------------------------


@router.get("/panel", response_class=HTMLResponse)
async def panel_page(request: Request):
    return templates.TemplateResponse(request, "panel.html", {"request": request})


@router.post("/panel/api/login")
async def login(payload: dict):
    _guard(payload.get("password"))
    return {"ok": True}


@router.get("/panel/api/state")
async def state(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        return JSONResponse(
            {
                "reachable": False,
                "why": "This bot has no DATABASE_URL, so the panel has nothing to talk to.",
            },
            status_code=200,
        )

    try:
        beat = await queue.find_one({"_id": HEARTBEAT_ID})
        control = await queue.find_one({"_id": CONTROL_ID}) or {}
        rows = (
            await queue.find({"_id": NOT_TASKS})
            .sort("createdAt", -1)
            .limit(200)
            .to_list(length=200)
        )
        live = await queue.find({"_id": NOT_TASKS, "status": "running"}).to_list(length=50)
        # aggregate() hands back the cursor itself, so it is awaited before
        # anything is read from it.
        tally_cursor = await queue.aggregate(
            [
                {"$match": {"_id": NOT_TASKS}},
                {"$group": {"_id": "$status", "n": {"$sum": 1}}},
            ]
        )
        tally = await tally_cursor.to_list(length=20)
    except Exception as err:
        return JSONResponse(
            {"reachable": False, "why": f"The bot's database did not answer: {err}"},
            status_code=200,
        )

    seen = {row["_id"] for row in live}
    tasks = live + [row for row in rows if row["_id"] not in seen]

    at = (beat or {}).get("at")
    ago = None if at is None else max(0, round(time() - float(at)))
    counts = {kind: 0 for kind in ("queued", "running", "done", "failed", "cancelled")}
    for row in tally:
        if row["_id"] in counts:
            counts[row["_id"]] = row["n"]
    counts["total"] = sum(counts.values())

    return {
        "reachable": True,
        "bridge": {
            "present": beat is not None,
            "online": ago is not None and ago <= STALE_SECONDS,
            "secondsAgo": ago,
            "bot": (beat or {}).get("bot", ""),
            "version": (beat or {}).get("version", ""),
            "running": (beat or {}).get("running", 0),
            "maxRunning": (beat or {}).get("maxRunning", 0),
            "atOnce": control.get("maxRunning", 0),
            "leechDisabled": bool((beat or {}).get("leechDisabled")),
            "defaultUpload": (beat or {}).get("defaultUpload", ""),
            "upSeconds": (
                None
                if not (beat or {}).get("startedAt")
                else max(0, round(time() - float(beat["startedAt"])))
            ),
        },
        # Everything the bot has in hand, including tasks started in Telegram.
        "live": (beat or {}).get("tasks", []),
        "counts": counts,
        "tasks": [
            {
                "id": str(row["_id"]),
                "url": row.get("url", ""),
                "name": row.get("name") or "",
                "mode": "mirror" if row.get("mode") == "mirror" else "leech",
                "engine": row.get("engine") or "direct",
                "status": row.get("status", "queued"),
                "error": row.get("error") or "",
                "createdAt": row.get("createdAt", 0),
                "cancelRequested": bool(row.get("cancelRequested")),
                "progress": row.get("progress") or None,
                "result": row.get("result") or None,
            }
            for row in tasks[:200]
        ],
    }


@router.post("/panel/api/tasks")
async def add_tasks(payload: dict, x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")

    links = _links_in(payload.get("links"))
    if not links:
        raise HTTPException(status_code=400, detail="Paste at least one link.")
    for url in links:
        problem = _link_problem(url)
        if problem:
            raise HTTPException(status_code=400, detail=problem)

    name = str(payload.get("name") or "").strip()
    if name and len(links) > 1:
        raise HTTPException(
            status_code=400, detail="A name can only be given when there is one link."
        )
    mode = payload.get("mode") if payload.get("mode") in MODES else "leech"
    engine = payload.get("engine") if payload.get("engine") in ENGINES else "direct"

    now = time()
    batch = f"panel-{int(now * 1000)}"
    docs = []
    for index, url in enumerate(links):
        docs.append(
            {
                "_id": f"task-{batch}-{index}",
                "url": url,
                "name": name or None,
                "flags": str(payload.get("flags") or "").strip(),
                "engine": engine,
                "mode": mode,
                "size": 0,
                "chatId": str(payload.get("chatId") or "").strip() or None,
                "userId": str(payload.get("userId") or "").strip() or None,
                "status": "queued",
                "batch": batch,
                # Fractions of a second keep the order they were pasted in.
                "createdAt": now + index / 1000,
                "startedAt": None,
                "finishedAt": None,
                "cancelRequested": False,
                "progress": None,
                "result": None,
                "error": None,
                "mid": None,
                "command": None,
            }
        )
    await queue.insert_many(docs)
    word = "link" if len(docs) == 1 else "links"
    return {"queued": len(docs), "message": f"{len(docs)} {word} handed to the bot."}


@router.post("/panel/api/tasks/cancel")
async def cancel_task(payload: dict, x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    task_id = str(payload.get("id") or "").strip()
    if not task_id:
        raise HTTPException(status_code=400, detail="Which task?")
    await queue.update_one({"_id": task_id}, {"$set": {"cancelRequested": True}})
    return {"message": "Stopping it."}


@router.post("/panel/api/tasks/cancel-all")
async def cancel_all(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    result = await queue.update_many(
        {"_id": NOT_TASKS, "status": {"$in": ["queued", "running"]}},
        {"$set": {"cancelRequested": True}},
    )
    return {"message": f"Stopping {result.modified_count} task(s)."}


@router.post("/panel/api/live/cancel")
async def cancel_live(payload: dict, x_panel_password: str = Header(default="")):
    """Stops one of the bot's own tasks, wherever it was started from."""
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    try:
        mid = int(payload.get("mid"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="Which task?")
    await queue.update_one(
        {"_id": CONTROL_ID}, {"$addToSet": {"cancelMids": mid}}, upsert=True
    )
    return {"message": "Asked the bot to stop it."}


@router.post("/panel/api/tasks/clear")
async def clear_finished(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    result = await queue.delete_many(
        {"_id": NOT_TASKS, "status": {"$in": ["done", "failed", "cancelled"]}}
    )
    return {"message": f"Cleared {result.deleted_count} finished task(s)."}


@router.post("/panel/api/at-once")
async def set_at_once(payload: dict, x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    try:
        wanted = int(payload.get("value"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="A number, please.")
    wanted = max(1, min(wanted, 20))
    await queue.update_one(
        {"_id": CONTROL_ID}, {"$set": {"maxRunning": wanted, "at": time()}}, upsert=True
    )
    return {"value": wanted, "message": f"The bot will run {wanted} at a time."}


@router.post("/panel/api/restart")
async def restart(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    await queue.update_one({"_id": CONTROL_ID}, {"$set": {"restartAt": time()}}, upsert=True)
    return {"message": "The bot was asked to restart. It is back in a minute or so."}


# ---------------------------------------------------------------------------
# What the page remembers, and the accounts it knows
# ---------------------------------------------------------------------------

# The fields the page fills in again after a refresh. Anything else typed is
# for that one job only.
REMEMBERED = ("userId", "chatId", "mode", "engine", "flags")


@router.get("/panel/api/prefs")
async def read_prefs(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        return {"values": {}}
    doc = await queue.find_one({"_id": PREFS_ID}) or {}
    return {"values": {key: doc.get(key, "") for key in REMEMBERED}}


@router.put("/panel/api/prefs")
async def write_prefs(payload: dict, x_panel_password: str = Header(default="")):
    """
    Keeps what was typed, so a refresh does not empty the page.

    It is saved in the bot's own database rather than in the browser, so the
    same values are there from any machine — and can be cleared from here.
    """
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    values = payload.get("values")
    if not isinstance(values, dict):
        raise HTTPException(status_code=400, detail="Nothing to save.")
    keep = {}
    for key in REMEMBERED:
        if key in values:
            value = values[key]
            keep[key] = "" if value is None else str(value).strip()
    if "mode" in keep and keep["mode"] not in MODES:
        keep["mode"] = "leech"
    if "engine" in keep and keep["engine"] not in ENGINES:
        keep["engine"] = "direct"
    keep["updatedAt"] = time()
    await queue.update_one({"_id": PREFS_ID}, {"$set": keep}, upsert=True)
    return {"saved": True}


@router.delete("/panel/api/prefs")
async def forget_prefs(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    await queue.delete_one({"_id": PREFS_ID})
    return {"message": "The page starts empty next time."}


def _user_id_problem(value):
    raw = str(value or "").strip()
    if not raw:
        return "Give the account's Telegram id."
    if not raw.lstrip("-").isdigit():
        return f'"{raw[:30]}" is not a Telegram id — it is a number, not a name.'
    return None


def _shown(value):
    """A setting as a person would write it — a chat id is not "-100123.0"."""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)[:60]


async def _own_settings(user_id):
    """
    A short account of what that account has told the bot to do.

    This is the same `user_data` the bot applies to a task it runs as that
    account, so it is also the answer to "will it follow my settings?".
    """
    store = _people_store()
    if store is None:
        return None
    try:
        doc = await store.find_one({"_id": int(user_id)})
    except (TypeError, ValueError):
        return None
    if not doc:
        return {"known": False, "summary": []}
    summary = []
    for key, label in (
        ("LEECH_SPLIT_SIZE", "split size"),
        ("USER_DUMP", "dump chat"),
        ("LEECH_DUMP_CHAT", "dump chat"),
        ("LEECH_PREFIX", "prefix"),
        ("LEECH_SUFFIX", "suffix"),
        ("LEECH_FILENAME_CAPTION", "caption"),
        ("AS_DOCUMENT", "as document"),
        ("DEFAULT_UPLOAD", "uploads to"),
        ("RCLONE_PATH", "rclone path"),
        ("GDRIVE_ID", "drive folder"),
        ("EQUAL_SPLITS", "equal splits"),
    ):
        value = doc.get(key)
        if value in (None, "", 0, False):
            continue
        summary.append({"label": label, "value": _shown(value)})
    if doc.get("THUMBNAIL"):
        summary.append({"label": "thumbnail", "value": "saved"})
    return {"known": True, "summary": summary}


@router.get("/panel/api/people")
async def list_people(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        return {"people": []}
    doc = await queue.find_one({"_id": PEOPLE_ID}) or {}
    people = []
    for entry in doc.get("people") or []:
        person = {
            "id": str(entry.get("id", "")),
            "label": str(entry.get("label", "")),
            "chatId": str(entry.get("chatId", "")),
        }
        person["own"] = await _own_settings(person["id"])
        people.append(person)
    people.sort(key=lambda row: row["label"].lower() or row["id"])
    return {"people": people}


@router.post("/panel/api/people")
async def save_person(payload: dict, x_panel_password: str = Header(default="")):
    """Adds an account, or changes one already on the list."""
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    problem = _user_id_problem(payload.get("id"))
    if problem:
        raise HTTPException(status_code=400, detail=problem)
    person = {
        "id": str(payload.get("id")).strip(),
        "label": str(payload.get("label") or "").strip(),
        "chatId": str(payload.get("chatId") or "").strip(),
    }
    doc = await queue.find_one({"_id": PEOPLE_ID}) or {}
    people = [row for row in (doc.get("people") or []) if str(row.get("id")) != person["id"]]
    people.append(person)
    await queue.update_one({"_id": PEOPLE_ID}, {"$set": {"people": people}}, upsert=True)
    return {"message": f"Saved {person['label'] or person['id']}.", "people": len(people)}


@router.delete("/panel/api/people/{person_id}")
async def remove_person(person_id: str, x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    queue = _queue()
    if queue is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    doc = await queue.find_one({"_id": PEOPLE_ID}) or {}
    people = [row for row in (doc.get("people") or []) if str(row.get("id")) != str(person_id)]
    await queue.update_one({"_id": PEOPLE_ID}, {"$set": {"people": people}}, upsert=True)
    return {"message": "Taken off the list. Nothing about the account itself is changed."}


@router.get("/panel/api/people/{person_id}")
async def person_settings(person_id: str, x_panel_password: str = Header(default="")):
    """What the bot will do when it runs a task as this account."""
    _guard(x_panel_password)
    own = await _own_settings(person_id)
    if own is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    return own


# ---------------------------------------------------------------------------
# The bot's own settings, as it keeps them
# ---------------------------------------------------------------------------

# Never shown once saved: anything here would hand over the bot or its data.
SECRETS = {
    "BOT_TOKEN",
    "DATABASE_URL",
    "TELEGRAM_HASH",
    "USER_SESSION_STRING",
    "RCLONE_CONFIG",
    "TOKEN_PICKLE",
    "GDRIVE_SECRET",
    "WEB_ACCESS_PASSWORD",
    "MEGA_PASSWORD",
    "JD_PASS",
    "USENET_SERVERS",
}


@router.get("/panel/api/settings")
async def read_settings(x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    store = _settings_store()
    if store is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    doc = await store.find_one({"_id": str(_setting("BOT_TOKEN", "")).split(":", 1)[0]})
    if doc is None:
        # Older bots keep one document without an id of their own.
        doc = await store.find_one({}) or {}
    values = {}
    for key, value in doc.items():
        if key == "_id":
            continue
        if key in SECRETS:
            values[key] = "" if not value else "********"
        elif isinstance(value, (str, int, float, bool)):
            values[key] = value
    return {"values": dict(sorted(values.items()))}


@router.post("/panel/api/settings")
async def write_settings(payload: dict, x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    store = _settings_store()
    if store is None:
        raise HTTPException(status_code=400, detail="This bot has no DATABASE_URL.")
    values = payload.get("values")
    if not isinstance(values, dict) or not values:
        raise HTTPException(status_code=400, detail="Nothing to save.")
    # A hidden value left untouched must not be written back as asterisks.
    clean = {
        key: value
        for key, value in values.items()
        if not (key in SECRETS and str(value).strip("*") == "")
    }
    if not clean:
        raise HTTPException(status_code=400, detail="Nothing to save.")
    bot_id = str(_setting("BOT_TOKEN", "")).split(":", 1)[0]
    await store.update_one({"_id": bot_id}, {"$set": clean}, upsert=True)
    return {
        "message": "Saved. The bot reads these when it starts, so restart it to apply them.",
    }


# ---------------------------------------------------------------------------
# The log, so a failure can be read without opening the dyno
# ---------------------------------------------------------------------------


@router.get("/panel/api/log")
async def read_log(lines: int = 200, x_panel_password: str = Header(default="")):
    _guard(x_panel_password)
    wanted = max(20, min(int(lines or 200), 2000))
    path = Path("log.txt")
    if not path.exists():
        return {"lines": [], "why": "There is no log file yet."}
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError as err:
        return {"lines": [], "why": f"The log could not be read: {err}"}
    return {"lines": text.splitlines()[-wanted:]}
