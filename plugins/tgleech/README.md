# tgleech — the TG Leech bridge

Lets the R2-Transfer Hub's **TG Leech** page hand this bot direct download links.

The bot has no endpoint that takes a task, but it already keeps its settings and its user data in
MongoDB. This plugin uses the same database as a queue: the Hub writes one document per link, the
plugin claims it, starts exactly the task `/leech` would have started, and writes the progress and
the outcome back to that document.

Nothing here replaces or changes the Telegram commands — they keep working untouched.

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

## Where a link is posted

Every task needs a real message for the bot to work from, exactly as the RSS feeds do. The chat is
taken from the task itself (the Hub's "Chat id" field), then `RSS_CHAT`, then `OWNER_ID`. The owner
must have started the bot at least once for the last of those to work.

## What is stored

Collection `tgleech.<partition>` in the bot's own database, where `<partition>` is the same value the
bot uses everywhere else — `p_` plus the first 24 characters of `sha256("wzmlx_v3_db_partition_salt"
+ bot id)`. One document per link:

```
_id, url, name, flags, engine, chatId, batch
status: queued | running | done | failed | cancelled
createdAt, startedAt, finishedAt, mid, command
cancelRequested
progress: { state, name, size, processed, percent, speed, eta, engine }
result:   { name, size, link, files, folders }
error
```

The document `_id: "__bridge__"` is the heartbeat: it is how the Hub knows the bot is alive, and it
is never treated as a task.
