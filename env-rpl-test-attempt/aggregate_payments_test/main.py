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


async def handle_query(bot: Bot, agg: Aggregator):
    while bot.is_running:
        chat, params = await bot.queries.get()
        result = agg.aggregate(**params)
        agg.add_aggregation(chat, result)


async def send_messages(
        bot: Bot,
        agg: Aggregator,
        polling_task: asyncio.Task,
):
    senders = set()
    while not polling_task.done():
        aggregations = agg.aggregations.copy()
        for chat, msg_queue in aggregations.items():
            messages = [msg_queue.get_nowait() for _ in range(msg_queue.qsize())]
            for msg in messages:
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
        query_handler = asyncio.create_task(handle_query(bot, agg))
        sender = asyncio.create_task(send_messages(bot, agg, poll))
        bot.add_tasks(
            poll,
            query_handler,
            sender,
        )
        gather = asyncio.gather(
            run,
            *bot._tasks,
            return_exceptions=True,
        )
        await asyncio.shield(gather)
    except asyncio.CancelledError:
        bot.stop_session()
        query_handler.cancel()
        # poll is not cancelled because it may continue processing updates.
        # sender is not cancelled for similar reason.
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
