from datetime import datetime
from pprint import pprint
from pymongo import MongoClient

client = MongoClient()
sample_db = client.sample_db
sample_coll = sample_db.sample_collection

input_data = {
    "dt_from": "2022-09-01T00:00:00",
    "dt_upto": "2022-12-31T23:59:59",
    "group_type": "month",
}

pipeline = [
    {
        # Filter out documents with dates outside the provided range
        "$match": {
            "dt": {
                "$gte": datetime.fromisoformat(input_data["dt_from"]),
                "$lte": datetime.fromisoformat(input_data["dt_upto"]),
            },
        },
    },
    {
        # Group by the beginning of the period that each document relates to
        # based on provided group_type.
        "$group": {
            "_id": {
                "trunc_date": {
                    "$dateTrunc": {
                        "date": "$dt",
                        "unit": input_data["group_type"],
                    },
                },
            },
        },
    },
]
