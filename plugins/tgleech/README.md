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

The document `_id: "__settings__"` holds `maxRunning`, and `_id: "__bridge__"` is the heartbeat: it is how the Hub knows the bot is alive, and it
is never treated as a task.
