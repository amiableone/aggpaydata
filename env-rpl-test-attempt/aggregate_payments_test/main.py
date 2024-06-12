import argparse

from pymongo import MongoClient

from data import MongoCollectionPopulator


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
