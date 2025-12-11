# Telegram Sender Service

## A unified service for sending / editing / deleting Telegram messages without flooding or API bans

If you need a reliable way to send, edit, or delete Telegram messages from a single place—while avoiding flood limits and blocking—this service should be useful.

The system consists of a Redis-backed queue and a set of consumers that send Telegram messages using a built-in rate-limiter.


### Services

1) `tg_sender`

Main application that contains: consumers, producers (optional), per-bot rate limiters, queue handling logic

2) `redis`

Redis instance for queues (Streams are used).

3) `tg_send_api`

A testing API used to demonstrate the service functionality.
Normally, you should push messages directly to Redis Streams, but this container makes testing easier.
You can disable it if not needed

### Message Queue and Main Logic

This service uses Redis Streams.
If you use your own Redis, make sure that persistence is properly configured.

By default the service listens to the **control stream**: `stream:tg_bot:control`

Most stream names (prefixes, etc.) can be changed via .env—see src/configs/config.py.

#### How it works

1) tg_sender starts.
2) You push a message (schema defined in src/schemas/message.py or via API) into the control queue.
3) This message describes a Telegram Bot Task.
4) The service starts a handler for that bot, with its own queues and rate limits.

#### Queues created per bot

Each Telegram bot task creates two queues: 
- regular messages - `stream:tg_bot:{bot_id}`
- broadcast messages - `stream:tg_bot:broadcast:{bot_id}`

Why two queues?

Broadcasts (announcements, bulk messages, etc.) should not block normal user-targeted messages.
The consumer processes messages using a round-robin strategy so that broadcasts cannot starve the main queue.

Logs Queue

If you want to receive logs about processed messages (delivered, failed, retries, etc.), you can enable it when registering a bot: `ServiceMessage(is_sent_logs=True)`

The messages logs queue default name is `stream:tg_bot:logs:{bot_id}`.


### Simple HowTo (Local)

Let's suppose you want to try service locally
1) Start containers `docker compose -f docker/docker-compose-local.yml up --build`
2) Navigate to test api `http://127.0.0.1:8000/docs`
3) Add bot using endpoint `/add` (`api_token` is taken from `AppSettings.DUMMY_TOKEN` (unless overridden in .env), `token` you should take from Bot Father in Telegram)
4) Now you can send\edit\delete messages using other endpoints!

### Some Examples

1) Message to add new `Telegram bot task` to queue (default stream name: `stream:tg_bot:control`)
```
{
    "type": "add_bot",
    "data": {
        "bot_id": 1, // bot id in your main app
        "token": "blablabla", // bot token
        "is_sent_logs": False // flag whether to create and send logs to specific queue
    }
}
```

2) Message to send Telegram message to queue (default stream name: `stream:tg_bot:{bot_id}`)
```
{
    "type": "send_msg",
    "data": {
        "bot_id": 1, // bot id in your main app
        "chat_id": 123, // user_id or group_id from Telegram
        "external_id": 123, // message id in your main app
        "text": "Hello World!", // text message
        "message_id": 123, // Telegram message id in case you want to delete or edit message
        // In case you want to send markup, see for more API or src.schemas.message.py::TaskMessage:
        "reply_markup": { 
            "inline_keyboard": [
                [
                    {
                        "text": "Yes",
                        "callback_data": "yes"
                    },
                    {
                        "text": "No",
                        "callback_data": "no"
                    },
                ]
            ]
        }
    }
}
```


### Some Limitations
1) Only one Telegram Bot Task per bot is supported
In practice, Telegram rate limits are too small to benefit from running multiple parallel handlers.
This might be extended in the future.

2) Text + InlineKeyboard is supported
Other types can be added later if needed.

### Future Plans

Rewrite core logic in Go to utilize goroutines and fully saturate available CPU cores.
