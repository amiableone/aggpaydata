import argparse
import asyncio
import logging

from functools import partial
from pymongo import MongoClient

from data import MongoCollectionPopulator
from tgbot import Bot
from aggregation import Aggregator


logger = logging.getLogger(__name__)

parser = argparse.ArgumentParser(
    description="Run PaymentDataAggregator telegram bot.",
)
parser.add_argument(
    "token",
    help="bot token for PaymentDataAggregator provided by BotFather",
)
parser.add_argument(
    "--set-commands",
    help="use this flag when adding a new command to the bot.",
    action="store_true",
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
parser.add_argument(
    "--debug", "-d",
    action="store_true",
    help="run in debug mode to see more detailed logs",
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
        "Be sure to use double quotes, not single ones.\n"
        "If you made a mistake in your query, just edit it."
    )
    data = {"chat_id": chat_id, "text": text}
    await bot.post("sendMessage", data)


async def handle_cmds(bot: Bot):
    handlers = set()
    while bot.is_running:
        chat, cmd, params = await bot.cmds_pending.get()
        # For the sake of this program, it's assumed that only coro funcs are
        # provided as callbacks to bot commands.
        handler = bot.commands[cmd](chat, params)
        handlers.add(handler)
        handler.add_done_callback(handlers.discard)
        logger.debug("Handling command /%s from chat %s.", cmd, chat)
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
        poller: asyncio.Task,
):
    senders = set()
    while bot.is_running:
        getter = asyncio.create_task(bot.query_results.get())
        done, pending = await asyncio.wait(
            [getter, poller],
            return_when=asyncio.FIRST_COMPLETED,
        )
        first = done.pop()
        if getter is not first:
            await asyncio.gather(*senders)
            getter.cancel()
            return
        chat, msg = getter.result()
        data = {"chat_id": chat, "text": msg}
        sender = asyncio.create_task(bot.post("sendMessage", data))
        senders.add(sender)
        sender.add_done_callback(senders.discard)
        logger.debug("Sending query results to chat %s", chat)
    await asyncio.gather(*senders)


async def log_state(bot: Bot):
    while True:
        await asyncio.sleep(60)
        logger.debug(
            "\n"
            "    Bot is running: %s.\n"
            "    Tasks pending: %s.\n"
            "    Session is open: %s.",
            bot.is_running,
            [task._coro.__name__ for task in bot._tasks],
            not bot.session.closed,
        )
        if bot._work_complete.done():
            break


async def main(
        bot: Bot,
        agg: Aggregator,
        set_commands: bool,
        debug: bool,
):

    logging.basicConfig(
        format="%(name)s:%(asctime)s:%(funcName)s::%(message)s",
        level=logging.DEBUG if debug else logging.INFO,
    )
    logger.debug("Running in debug mode.")
    try:
        if set_commands:
            await bot.set_commands()
        run = asyncio.create_task(bot.run())
        poll = asyncio.create_task(bot.run_polling())
        bot.query_results = asyncio.Queue()
        query_handler = asyncio.create_task(handle_query(bot, agg))
        sender = asyncio.create_task(send_messages(bot, poll))
        cmd_handler = asyncio.create_task(handle_cmds(bot))
        bot.add_tasks(
            poll,
            query_handler,
            sender,
            cmd_handler,
        )
        tasks = [run]
        if debug:
            tasks.append(asyncio.create_task(log_state(bot)))
        gather = asyncio.gather(
            *tasks,
            *bot._tasks,
            return_exceptions=True,
        )
        await asyncio.shield(gather)
    except asyncio.CancelledError:
        bot.stop_session()
        # cancel query_handler and cmd_handler to cease getting new updates and
        # allow other tasks finish working on updates retrieved before stop_session()
        # was called.
        query_handler.cancel()
        cmd_handler.cancel()
        await gather


if __name__ == "__main__":
    args = parser.parse_args()
    client = MongoClient(args.host, args.port)
    logger.info("Connected to MongoDB.")
    # Set up collection in database.
    if args.refill:
        # Use --refill flag when using db or collection name for the first time.
        MongoCollectionPopulator(client, args.db, args.collection).populate()
        logger.info("Mongo collection populated.")
    coll = client[args.db][args.collection]
    bot = Bot(args.token)
    start_cb = partial(_start_cb, bot)
    help_cb = partial(_help_cb, bot)
    bot.add_commands(start=start_cb, help=help_cb)
    bot.commands["start"].description = "Let me introduce myself."
    bot.commands["help"].description = "Let me assist you."
    agg = Aggregator(coll)
    asyncio.run(
        main(
            bot,
            agg,
            args.set_commands,
            args.debug,
        )
    )
