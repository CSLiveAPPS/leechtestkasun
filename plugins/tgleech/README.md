# tgleech — the bridge behind the bot's control panel

Lets a web page hand this bot work: the bot's **own control panel** at `/panel`, and the
R2-Transfer Hub's **TG Leech** page.

The bot has no endpoint that takes a task, but it already keeps its settings and its user data in
MongoDB. This plugin uses the same database as a queue: the Hub writes one document per link, the
plugin claims it, starts exactly the task `/leech` would have started, and writes the progress and
the outcome back to that document.

Nothing here replaces or changes the Telegram commands — they keep working untouched.

**This plugin is what makes the control panel work.** Without it the panel still opens and reports
honestly — it says the bot is not answering — but nothing it is asked to do will happen.

## Installing

The bot rebuilds itself from `UPSTREAM_REPO` every time it starts, so a plugin dropped onto the
running dyno is gone at the next restart. Put this folder in the repo the bot updates from:

1. Copy `plugins/tgleech/` into your `UPSTREAM_REPO` at the same path, on the `UPSTREAM_BRANCH` the
   bot uses.
2. Restart the bot.

It loads by itself (`plugin_manager.boot()` finds every folder in `plugins/`), registers no
commands, and starts watching the queue. The bot's log says:

```
tgleech: watching queue tgleech.p_xxxxxxxxxxxxxxxxxxxxxxxx
```

If `DISABLE_PLUGINS` is on, or this plugin is switched off with `/plugins`, the bridge simply does
not run and the TG Leech page reports "no bridge".

## Settings

Set with `/plugins` → tgleech → configure:

| Name | Default | What it does |
| --- | --- | --- |
| `max_running` | 3 | How many queued links run at once |
| `poll_seconds` | 5 | How often the queue is checked |

The Hub's "At one time" box overrides `max_running`. It is kept in the same collection under
`_id: "__settings__"` as `{maxRunning: n}`, and the bridge re-reads it every round, so changing it
takes effect within seconds and needs no restart.

## Where a link is posted

Every task needs a real message for the bot to work from, exactly as the RSS feeds do.

**Whose task it is** comes from `userId` on the document, falling back to `OWNER_ID`. That account
becomes the message's sender, which is what makes WZML-X apply its settings — `user_data[user_id]`
holds the thumbnail, dump chat, split size, prefix and the rest. That account must have pressed
Start in the bot once, or the bot cannot see it and the task fails with a message saying so.

**Where it is posted** is `chatId` if the document has one, otherwise that account's own chat with
the bot, otherwise `RSS_CHAT`.

## What is stored

Collection `tgleech.<partition>` in the bot's own database, where `<partition>` is the same value the
bot uses everywhere else — `p_` plus the first 24 characters of `sha256("wzmlx_v3_db_partition_salt"
+ bot id)`. One document per link:

```
_id, url, name, flags, engine, chatId, userId, batch
status: queued | running | done | failed | cancelled
createdAt, startedAt, finishedAt, mid, command
cancelRequested
progress: { state, name, size, processed, percent, speed, eta, engine }
result:   { name, size, link, files, folders }
error
```

Stopping is asked for by setting `cancelRequested` on a task. The bridge closes off everything still
waiting in one write, and cancels what is actually running one at a time, because each of those is a
download the bot is holding.

The document `_id: "__settings__"` holds what the page asks of the bot, and `_id: "__bridge__"` is
the heartbeat — how a page knows the bot is alive. Neither is ever treated as a task.

```
__settings__: { maxRunning, restartAt, restartDoneAt, cancelMids: [ ... ] }
__bridge__:   { at, version, bot, botId, running, maxRunning, leechDisabled,
                defaultUpload, startedAt, tasks: [ ... ] }
```

- `mode` on a task is `leech` (into Telegram) or `mirror` (uploaded to wherever `DEFAULT_UPLOAD`
  points). A task with no `mode` is a leech, which is what every task was before.
- `restartAt` asks the bot to restart itself. The bridge takes the bot's own restart path — the same
  one the `/restart` button takes — and writes `restartDoneAt` **before** stopping anything, so a
  restart it caused can never look like a fresh order and loop.
- `cancelMids` stops tasks the bridge never started, by their message id. Ids are taken off the list
  as they are dealt with.
- `tasks` on the heartbeat is everything the bot has in hand, including work started from Telegram,
  each marked `mine` when this bridge started it. That is what the panel's "Running now" shows.

## The control panel

`web/panel.py` and `web/templates/panel.html` serve a page at **`/panel`** on the bot's own web
address, linked from its landing page. It is locked with a password — `kasun123` unless
`PANEL_PASSWORD` says otherwise — and from it you can:

- paste links in bulk and send them as a **leech** or a **mirror**, with any engine (direct,
  qBittorrent, JDownloader, NZB, yt-dlp), optional rename, flags, user id and chat id
- watch **everything the bot is doing**, including tasks started in Telegram, and stop any of them
- see what was sent from the page with its progress, stop one or all, clear what is finished
- set how many run at once
- read and edit the bot's saved settings (tokens and passwords are never shown)
- read the tail of the log
- restart the bot

The panel runs in the **web** process, not the bot process — gunicorn serves it beside the bot — so
it never calls the bot directly. Everything above travels through the documents described here, and
this plugin is the half that acts on them.
