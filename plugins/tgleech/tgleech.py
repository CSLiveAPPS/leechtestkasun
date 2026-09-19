"""
TG Leech bridge.

The R2-Transfer Hub's "TG Leech" page writes one document per link into this
bot's own database, in `tgleech.<partition>`. This plugin claims those
documents, starts the very same task the /leech command would have started,
and writes the progress and the outcome back to the document it came from.

Nothing here replaces the Telegram commands — they keep working untouched.
The bridge only gives the Hub a second way in, over the database both sides
already share, so no port has to be opened on the bot.
"""

from asyncio import CancelledError, sleep
from inspect import isawaitable
from time import time

from pymongo import ReturnDocument

from bot import LOGGER, bot_loop, task_dict, task_dict_lock
from bot.core.config_manager import Config
from bot.core.plugin_manager import PluginBase
from bot.core.tg_client import TgClient, db_partition_id
from bot.helper.ext_utils.db_handler import database
from bot.modules.mirror_leech import Mirror
from bot.modules.ytdlp import YtDlp
from bot.version import get_version

# The heartbeat lives in the same collection as the queue so the bridge needs
# no second collection; the Hub skips this id when it lists tasks.
HEARTBEAT_ID = "__bridge__"

# How many the Hub wants running at once. It lives beside the queue so the
# number can be changed from the page and take effect on the next round,
# without restarting the bot.
CONTROL_ID = "__settings__"

# How many of the bot's tasks the heartbeat carries. Enough for any real
# screen, and it keeps one document from growing without limit.
MAX_SNAPSHOT = 40

DEFAULT_POLL_SECONDS = 5
DEFAULT_MAX_RUNNING = 3

# A task that never reaches the status list is one the bot refused — a
# disabled command, a failed pre-task check. Nothing will call back for it.
START_GRACE_SECONDS = 180
# Once a task has been seen and then leaves the status list, its result
# callback is usually already on its way.
FINISH_GRACE_SECONDS = 90

# Which class runs a link, and the command word the trigger message carries —
# one word for leeching into Telegram, another for mirroring to wherever
# DEFAULT_UPLOAD points.
ENGINES = {
    "direct": ("mirror", {}, {"leech": "leech", "mirror": "mirror"}),
    "qb": ("mirror", {"is_qbit": True}, {"leech": "qbleech", "mirror": "qbmirror"}),
    "jd": ("mirror", {"is_jd": True}, {"leech": "jdleech", "mirror": "jdmirror"}),
    "nzb": ("mirror", {"is_nzb": True}, {"leech": "nzbleech", "mirror": "nzbmirror"}),
    "ytdl": ("ytdl", {}, {"leech": "ytdlleech", "mirror": "ytdl"}),
}


def _mode(doc):
    """Leech unless the task plainly says mirror. Older tasks carry neither."""
    return "mirror" if str(doc.get("mode") or "").strip() == "mirror" else "leech"


def _partition():
    if not TgClient.PARTITION:
        bot_id = TgClient.ID or (Config.BOT_TOKEN or "").split(":", 1)[0]
        TgClient.PARTITION = db_partition_id(bot_id)
    return TgClient.PARTITION


def _queue():
    if database.db is None:
        return None
    return database.db.tgleech[_partition()]


def _command_text(doc):
    engine = ENGINES.get(doc.get("engine") or "direct") or ENGINES["direct"]
    word = engine[2][_mode(doc)]
    if Config.CMD_SUFFIX:
        word = f"{word}{Config.CMD_SUFFIX}"
    parts = [f"/{word}", str(doc.get("url") or "").strip()]
    flags = str(doc.get("flags") or "").strip()
    if flags:
        parts.append(flags)
    name = str(doc.get("name") or "").strip()
    if name:
        parts.append(f"-n {name}")
    return " ".join(part for part in parts if part)


def _reporting(base):
    """Wraps a task class so its outcome lands back on the queued document."""

    class Reporting(base):
        def __init__(self, client, message, doc_id, bridge, **kwargs):
            self._doc_id = doc_id
            self._bridge = bridge
            super().__init__(client, message, **kwargs)

        async def on_upload_complete(
            self, link, files, folders, mime_type, rclone_path="", dir_id=""
        ):
            await super().on_upload_complete(
                link, files, folders, mime_type, rclone_path, dir_id
            )
            await self._bridge.finish(
                self._doc_id,
                "done",
                result={
                    "name": str(getattr(self, "name", "") or ""),
                    "size": int(getattr(self, "size", 0) or 0),
                    "link": link or "",
                    "files": len(files or {}),
                    "folders": folders or 0,
                },
            )

        async def on_download_error(self, error, button=None, is_limit=False):
            await super().on_download_error(error, button, is_limit)
            await self._bridge.finish(self._doc_id, "failed", error=str(error))

        async def on_upload_error(self, error):
            await super().on_upload_error(error)
            await self._bridge.finish(self._doc_id, "failed", error=str(error))

    return Reporting


ReportingMirror = _reporting(Mirror)
ReportingYtDlp = _reporting(YtDlp)


async def _read_progress(status):
    """
    Whatever this status object will tell us, without trusting any of it.

    "state" is read first on purpose: for aria2, qBittorrent and SABnzbd
    `status()` is what refreshes the task from the download client, so
    everything after it is fresh.
    """
    out = {}
    for key, call in (
        ("state", "status"),
        ("name", "name"),
        ("size", "size"),
        ("processed", "processed_bytes"),
        ("percent", "progress"),
        ("speed", "speed"),
        ("eta", "eta"),
        ("engine", "engine"),
    ):
        try:
            value = getattr(status, call, None)
            if callable(value):
                value = value()
            if isawaitable(value):
                value = await value
            out[key] = "" if value is None else str(value)
        except Exception:
            out[key] = ""
    return out


class TgLeechBridge(PluginBase):
    def __init__(self):
        self._task = None
        self._stop = False
        self._started_at = time()
        # doc id -> what we know about the task we started for it
        self._running = {}
        # How many at once, as last asked for by the Hub; None = use the
        # plugin's own setting.
        self._limit = None

    # -- lifecycle ---------------------------------------------------------

    async def on_load(self):
        if not Config.DATABASE_URL or database.db is None:
            LOGGER.warning("tgleech: no database, the Hub bridge stays idle")
            return True
        self._stop = False
        await self._recover()
        self._task = bot_loop.create_task(self._loop())
        LOGGER.info(f"tgleech: watching queue tgleech.{_partition()}")
        return True

    async def _recover(self):
        """
        Anything left marked running belongs to a bot that is no longer here —
        the dyno restarted mid-task. Nothing will ever report on those, so they
        are closed off rather than left looking alive. What is still queued is
        untouched and gets picked up as usual.
        """
        queue = _queue()
        if queue is None:
            return
        try:
            left = await queue.update_many(
                {"status": "running", "_id": {"$nin": [HEARTBEAT_ID, CONTROL_ID]}},
                {
                    "$set": {
                        "status": "failed",
                        "finishedAt": time(),
                        "error": "The bot restarted while this was running. Send the link again.",
                    }
                },
            )
            if left.modified_count:
                LOGGER.info(f"tgleech: {left.modified_count} task(s) were lost to a restart")
        except Exception as err:
            LOGGER.error(f"tgleech: could not tidy up after a restart: {err}")

    async def on_unload(self):
        self._stop = True
        if self._task is not None:
            self._task.cancel()
            self._task = None
        return True

    async def on_disable(self):
        return await self.on_unload()

    async def on_enable(self):
        return await self.on_load()

    # -- the loop ----------------------------------------------------------

    def _max_running(self):
        """What the Hub asked for, or the plugin's own setting if it never has."""
        if self._limit is not None:
            return self._limit
        try:
            value = int(self.get_config("max_running", DEFAULT_MAX_RUNNING))
        except (TypeError, ValueError):
            value = DEFAULT_MAX_RUNNING
        return max(1, min(value, 20))

    async def _read_limit(self):
        queue = _queue()
        if queue is None:
            return
        try:
            doc = await queue.find_one({"_id": CONTROL_ID})
        except Exception:
            return
        if not doc:
            self._limit = None
            return
        try:
            self._limit = max(1, min(int(doc.get("maxRunning")), 20))
        except (TypeError, ValueError):
            self._limit = None

    def _poll_seconds(self):
        try:
            value = int(self.get_config("poll_seconds", DEFAULT_POLL_SECONDS))
        except (TypeError, ValueError):
            value = DEFAULT_POLL_SECONDS
        return max(2, min(value, 120))

    async def _loop(self):
        while not self._stop:
            try:
                await self._read_limit()
                await self._beat()
                await self._watch()
                await self._cancels()
                await self._foreign_cancels()
                await self._restarts()
                await self._claim()
            except CancelledError:
                raise
            except Exception as err:
                LOGGER.error(f"tgleech: {err}", exc_info=True)
            await sleep(self._poll_seconds())

    async def _snapshot(self):
        """
        Everything the bot has in hand, not only what this bridge started.

        The panel shows the bot as a whole, so a download someone started from
        Telegram appears beside the ones sent from the page, and can be
        stopped from either side.
        """
        mine = {state["mid"] for state in self._running.values()}
        out = []
        async with task_dict_lock:
            holding = list(task_dict.items())[:MAX_SNAPSHOT]
        for mid, status in holding:
            try:
                row = await _read_progress(status)
            except Exception:
                continue
            row["mid"] = mid
            row["mine"] = mid in mine
            try:
                row["by"] = str(getattr(getattr(status, "listener", None), "tag", "") or "")
            except Exception:
                row["by"] = ""
            out.append(row)
        return out

    async def _beat(self):
        queue = _queue()
        if queue is None:
            return
        await queue.update_one(
            {"_id": HEARTBEAT_ID},
            {
                "$set": {
                    "at": time(),
                    "version": get_version(),
                    "bot": TgClient.BNAME or "",
                    "botId": str(TgClient.ID or ""),
                    "running": len(self._running),
                    "maxRunning": self._max_running(),
                    "leechDisabled": bool(Config.DISABLE_LEECH),
                    "defaultUpload": str(Config.DEFAULT_UPLOAD or ""),
                    "startedAt": self._started_at,
                    "tasks": await self._snapshot(),
                }
            },
            upsert=True,
        )

    async def _foreign_cancels(self):
        """
        Stops tasks the panel asked about by their message id.

        These are the bot's own tasks — started from Telegram, or by someone
        else — so there is no queue document behind them. The ids are taken
        off the list as they are dealt with, so one order stops one task.
        """
        queue = _queue()
        if queue is None:
            return
        try:
            doc = await queue.find_one_and_update(
                {"_id": CONTROL_ID, "cancelMids": {"$nin": [None, []]}},
                {"$set": {"cancelMids": []}},
            )
        except Exception:
            return
        wanted = (doc or {}).get("cancelMids") or []
        for raw in wanted:
            try:
                mid = int(raw)
            except (TypeError, ValueError):
                continue
            async with task_dict_lock:
                status = task_dict.get(mid)
            if status is None:
                continue
            try:
                await status.cancel_task()
                LOGGER.info(f"tgleech: stopped task {mid}, as the panel asked")
            except Exception as err:
                LOGGER.error(f"tgleech: could not stop {mid}: {err}")

    async def _restarts(self):
        """
        Restarts the bot when the page asks for it.

        The page cannot press the bot's own confirm button, so this takes the
        same path that button takes — the bot's own restart, with its own
        tidying up: tasks stopped, helpers killed, update.py run, then the
        process replaced. Anything running is lost, exactly as it is when the
        owner restarts from Telegram.

        The order is marked as carried out *before* anything is stopped. A
        restart that came from this must never be seen as a fresh order by the
        bot that comes back, or it would restart for ever.
        """
        queue = _queue()
        if queue is None:
            return
        try:
            doc = await queue.find_one({"_id": CONTROL_ID})
        except Exception:
            return
        if not doc:
            return
        try:
            asked = float(doc.get("restartAt") or 0)
        except (TypeError, ValueError):
            return
        if asked <= 0:
            return
        try:
            done = float(doc.get("restartDoneAt") or 0)
        except (TypeError, ValueError):
            done = 0.0
        if asked <= done:
            return

        try:
            await queue.update_one(
                {"_id": CONTROL_ID},
                {"$set": {"restartDoneAt": asked, "restartRanAt": time()}},
            )
        except Exception as err:
            LOGGER.error(f"tgleech: could not mark the restart as taken: {err}")
            return

        LOGGER.info("tgleech: restarting, as the page asked")
        try:
            await self._restart_now()
        except Exception as err:
            LOGGER.error(f"tgleech: the restart failed: {err}", exc_info=True)

    async def _restart_now(self):
        """The bot's own restart, driven from here instead of from a button."""
        from bot.modules.restart import confirm_restart

        chat_id = Config.OWNER_ID
        try:
            note = await TgClient.bot.send_message(
                chat_id=chat_id,
                text="Restart asked for from the R2-Transfer page.",
                disable_notification=True,
            )
            holder = await TgClient.bot.send_message(
                chat_id=chat_id,
                text="/restart",
                disable_notification=True,
            )
        except Exception as err:
            LOGGER.error(f"tgleech: could not post the restart note: {err}")
            return

        # confirm_restart works off the button's own message: it deletes that
        # message and answers in the one it replies to.
        holder.reply_to_message = note

        class _AsIfPressed:
            def __init__(self, message):
                self.data = "botrestart confirm hard"
                self.message = message

            async def answer(self, *args, **kwargs):
                return None

        # Its decorator hands back the task it started rather than its result,
        # so the work is waited on here — the process is replaced part-way
        # through, and anything that goes wrong before that gets logged.
        started = await confirm_restart(TgClient.bot, _AsIfPressed(holder))
        if started is not None:
            await started

    async def _claim(self):
        queue = _queue()
        if queue is None:
            return
        room = self._max_running() - len(self._running)
        while room > 0 and not self._stop:
            doc = await queue.find_one_and_update(
                {"status": "queued", "_id": {"$nin": [HEARTBEAT_ID, CONTROL_ID]}},
                {"$set": {"status": "running", "startedAt": time(), "error": None}},
                sort=[("createdAt", 1)],
                return_document=ReturnDocument.AFTER,
            )
            if not doc:
                return
            await self._start(doc)
            room -= 1

    async def _start(self, doc):
        doc_id = doc["_id"]
        queue = _queue()
        url = str(doc.get("url") or "").strip()
        if not url:
            await self.finish(doc_id, "failed", error="This task has no link.")
            return

        # Whose task this is. The bot keeps settings per account — thumbnail,
        # dump chat, split size, prefix — and applies whichever account asked,
        # so this is what decides which settings the task runs with.
        wanted_user = doc.get("userId") or Config.OWNER_ID
        try:
            user_id = int(wanted_user)
        except (TypeError, ValueError):
            await self.finish(
                doc_id, "failed", error=f"{wanted_user!r} is not a Telegram user id."
            )
            return

        # A task posted in that account's own chat with the bot, unless it was
        # told otherwise.
        chat_id = doc.get("chatId") or user_id or Config.RSS_CHAT
        try:
            chat_id = int(chat_id)
        except (TypeError, ValueError):
            await self.finish(
                doc_id,
                "failed",
                error=f"{chat_id!r} is not a chat id the bot can post to.",
            )
            return

        command = _command_text(doc)
        try:
            message = await TgClient.bot.send_message(
                chat_id=chat_id,
                text=command,
                disable_web_page_preview=True,
                disable_notification=True,
            )
        except Exception as err:
            await self.finish(
                doc_id,
                "failed",
                error=f"Could not post the task in chat {chat_id}: {err}",
            )
            return

        try:
            user = await TgClient.bot.get_users(user_id)
        except Exception as err:
            await self.finish(
                doc_id,
                "failed",
                error=(
                    f"The bot cannot see user {user_id}: {err}. "
                    "That account has to press Start in the bot's chat once before the bot can work as it."
                ),
            )
            return

        # The same shape the RSS feeds use: a real message the bot sent, with
        # the command text and the chosen account as its sender.
        message.text = command
        message.from_user = user
        message._rss_trigger = True

        kind, kwargs, _words = (
            ENGINES.get(doc.get("engine") or "direct") or ENGINES["direct"]
        )
        runner = ReportingYtDlp if kind == "ytdl" else ReportingMirror
        # A mirror is the same task with the uploading turned the other way:
        # the bot's own DEFAULT_UPLOAD decides where it lands.
        task = runner(
            TgClient.bot,
            message,
            doc_id,
            self,
            is_leech=_mode(doc) == "leech",
            **kwargs,
        )

        self._running[doc_id] = {
            "mid": message.id,
            "at": time(),
            "seen": False,
            "left": 0.0,
        }
        if queue is not None:
            await queue.update_one(
                {"_id": doc_id},
                {"$set": {"mid": message.id, "command": command, "chatId": chat_id}},
            )
        bot_loop.create_task(task.new_event())
        LOGGER.info(f"tgleech: started {doc_id} — {command}")

    async def _watch(self):
        """Writes progress, and gives up on tasks the bot never really took."""
        queue = _queue()
        if queue is None:
            return
        now = time()
        for doc_id, state in list(self._running.items()):
            async with task_dict_lock:
                status = task_dict.get(state["mid"])
            if status is not None:
                state["seen"] = True
                state["left"] = 0.0
                await queue.update_one(
                    {"_id": doc_id}, {"$set": {"progress": await _read_progress(status)}}
                )
                continue
            if not state["seen"]:
                if now - state["at"] > START_GRACE_SECONDS:
                    await self.finish(
                        doc_id,
                        "failed",
                        error="The bot never started this task — check that leech is enabled and the link is allowed.",
                    )
                continue
            # Seen and now gone: the result callback normally arrives within
            # moments, so only a long silence counts as a lost task.
            if state["left"] == 0.0:
                state["left"] = now
            elif now - state["left"] > FINISH_GRACE_SECONDS:
                await self.finish(
                    doc_id, "failed", error="The task ended without reporting a result."
                )

    async def _cancels(self):
        queue = _queue()
        if queue is None:
            return

        # Stopping a whole batch can mean hundreds that never started. Those
        # need no talking to — they are closed off in one write rather than
        # one round trip each.
        try:
            waiting = await queue.update_many(
                {
                    "cancelRequested": True,
                    "status": "queued",
                    "_id": {"$nin": [HEARTBEAT_ID, CONTROL_ID]},
                },
                {
                    "$set": {
                        "status": "cancelled",
                        "finishedAt": time(),
                        "error": "Cancelled before it started.",
                    }
                },
            )
            if waiting.modified_count:
                LOGGER.info(f"tgleech: {waiting.modified_count} waiting task(s) cancelled")
        except Exception as err:
            LOGGER.error(f"tgleech: could not cancel the waiting tasks: {err}")

        # What is actually running has to be stopped one at a time, because
        # each one is a download the bot is holding.
        async for doc in queue.find(
            {"cancelRequested": True, "status": "running"}
        ):
            doc_id = doc["_id"]
            state = self._running.get(doc_id)
            mid = state["mid"] if state else doc.get("mid")
            status = None
            if mid:
                async with task_dict_lock:
                    status = task_dict.get(mid)
            if status is not None:
                try:
                    await status.cancel_task()
                except Exception as err:
                    LOGGER.error(f"tgleech: could not cancel {doc_id}: {err}")
            else:
                await self.finish(doc_id, "cancelled", error="Cancelled.")

    # -- writing the outcome back -----------------------------------------

    async def finish(self, doc_id, status, result=None, error=None):
        self._running.pop(doc_id, None)
        queue = _queue()
        if queue is None:
            return
        # A stopped task comes back through the error path, but it was asked
        # for, so it is reported as stopped rather than as a failure.
        if status == "failed":
            try:
                asked = await queue.find_one({"_id": doc_id}, {"cancelRequested": 1})
                if asked and asked.get("cancelRequested"):
                    status = "cancelled"
            except Exception:
                pass
        changes = {"status": status, "finishedAt": time()}
        if result is not None:
            changes["result"] = result
        if error is not None:
            changes["error"] = str(error)
        try:
            await queue.update_one({"_id": doc_id}, {"$set": changes})
        except Exception as err:
            LOGGER.error(f"tgleech: could not record {doc_id} as {status}: {err}")
