import json

from datetime import datetime
from asyncio import Queue
from pymongo import MongoClient
from pymongo.collection import Collection


class Aggregator:
    """
    Single task class performing aggregation of payment data
    within specified time period by specified time intervals.
    """

    def __init__(self, coll: Collection):
        self.coll = coll

    def get_pipeline(self, dt_from, dt_upto, group_type):
        return [
            {
                # Filter out documents with dates outside the provided range
                "$match": {
                    "dt": {
                        "$gte": dt_from,
                        "$lte": dt_upto,
                    },
                },
            },
            {
                # Group by the beginning of the period that each document relates to
                # based on provided group_type.
                "$group": {
                    "_id": {
                        "label": {
                            "$dateTrunc": {
                                "date": "$dt",
                                "unit": group_type,
                            },
                        },
                    },
                    "total_payments": {"$sum": "$value"},
                },
            },
            {
                # Sort by label value computed in the previous stage.
                "$sort": {"_id.label": 1},
            },
            {
                # Pivot total_payments and _id.label data.
                "$group": {
                    "_id": None,
                    "dataset": {"$push": "$total_payments"},
                    "labels": {
                        "$push": {
                            "$dateToString": {
                                "date": "$_id.label",
                                "format": "%Y-%m-%dT%H:%M:%S",
                            },
                        },
                    },
                },
            },
            # Exclude _id field from output.
            {"$project": {"_id": 0}},
        ]

    def aggregate(self, **params):
        dt_from = params.get("dt_from")
        dt_upto = params.get("dt_upto")
        group_type = params.get("group_type")
        if not dt_from:
            return "Param dt_from is missing."
        if not dt_upto:
            return "Param dt_upto is missing."
        if not group_type:
            return "Params group_type is missing"
        try:
            dt_upto = datetime.fromisoformat(dt_upto)
            dt_from = datetime.fromisoformat(dt_from)
            res = self.coll.aggregate(
                self.get_pipeline(dt_from, dt_upto, group_type)
            ).next()
            return json.dumps(res)
        except Exception:
            return "Looks like you've provided invalid params."
