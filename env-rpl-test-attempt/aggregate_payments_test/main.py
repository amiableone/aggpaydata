import argparse
import asyncio

from pymongo import MongoClient

from data import MongoCollectionPopulator
from tgbot import Bot
from aggregation import Aggregator


parser = argparse.ArgumentParser(
    description="Run PaymentDataAggregator telegram bot.",
)
parser.add_argument(
    "token",
    help="bot token for PaymentDataAggregator provided by BotFather",
)
parser.add_argument(
    "--host",
    default="localhost",
    help="host to connect to MongoDB with, provided to MongoClient",
)
parser.add_argument(
    "--port",
    default=27017,
    type=int,
    help="port to connect to MongoDB with, provided to MongoClient"
)
parser.add_argument(
    "--db",
    default="sample_db",
    help="name of the backend database",
)
parser.add_argument(
    "--collection",
    default="sample_collection",
    help="name of the Mongo collection",
)
parser.add_argument(
    "--refill",
    action="store_true",
    help="if True, clear the Mongo collection and fill it with data from bson",
)


async def _start_cb(bot: Bot, chat_id, *args):
    text = (
        "Hi.\n"
        "I'm a payment aggregator bot.\n"
        "To check what I can do please use /help command."
    )
    data = {"chat_id": chat_id, "text": text}
    await bot.post("sendMessage", data)


async def _help_cb(bot: Bot, chat_id, *args):
    text = (
        "To make a query, send me params of the query in the following form:\n"
        "{\n"
        "\"dt_from\":\"2022-09-01T00:00:00\",\n"
        "\"dt_upto\":\"2022-12-31T23:59:00\",\n"
        "\"group_type\":\"month\"\n"
        "}\n"
        "Available values for the 'group_type' param are 'month', 'week', 'day', "
        "'hour'.\n"
        "Be sure to use double quotes, not single ones."
    )
    data = {"chat_id": chat_id, "text": text}
    await bot.post("sendMessage", data)


async def handle_cmds(
        bot: Bot,
        polling_task: asyncio.Task,
):
    handlers = set()
    while not polling_task.done():
        chat, cmd, params = await cmds.get()
        # For the sake of this program, it's assumed that only coro funcs are
        # provided as callbacks to bot commands.
        handler = bot.commands[cmd](chat, params)
        handlers.add(handler)
        handler.add_done_callback(handlers.discard)
    await asyncio.gather(*handlers)


async def handle_query(bot: Bot, agg: Aggregator):
    while bot.is_running:
        chat, params = await bot.queries.get()
        result = agg.aggregate(**params)
        item = chat, result
        bot.query_results: asyncio.Queue
        bot.query_results.put_nowait(item)


async def send_messages(
        bot: Bot,
        polling_task: asyncio.Task,
):
    senders = set()
    while not polling_task.done():
        chat, msg = await bot.query_results.get()
        data = {"chat_id": chat, "text": msg}
        sender = asyncio.create_task(bot.post("sendMessage", data))
        senders.add(sender)
        sender.add_done_callback(senders.discard)
    await asyncio.gather(*senders)


async def main(
        bot: Bot,
        agg: Aggregator,
):
    try:
        run = asyncio.create_task(bot.run())
        poll = asyncio.create_task(bot.run_polling())
        bot.query_results = asyncio.Queue()
        query_handler = asyncio.create_task(handle_query(bot, agg))
        sender = asyncio.create_task(send_messages(bot, poll))
        cmd_handler = asyncio.create_task(handle_cmds(bot, poll))
        bot.add_tasks(
            poll,
            query_handler,
            sender,
            cmd_handler,
        )
        gather = asyncio.gather(
            run,
            *bot._tasks,
            return_exceptions=True,
        )
        await asyncio.shield(gather)
    except asyncio.CancelledError:
        bot.stop_session()
        # cancel query_handler to cease getting new updates and allow other tasks
        # finish working on updates retrieved before stop_session() was called.
        query_handler.cancel()
        await gather


if __name__ == "__main__":
    args = parser.parse_args()
    # Connect to mongodb.
    client = MongoClient(args.host, args.port)
    # Set up collection in database.
    if args.refill:
        # Use --refill flag when using db or collection name for the first time.
        MongoCollectionPopulator(client, args.db, args.collection).populate()
    db = client[args.db]
    coll = db[args.collection]
