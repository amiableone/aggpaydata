import json

from datetime import datetime
from pprint import pprint
from pymongo import MongoClient
from pymongo.collection import Collection


class Aggregator:
    """
    Single task class performing aggregation of payment data
    within specified time period by specified time intervals.
    """

    def __init__(self, coll: Collection, **params):
        self.coll = coll
        self._missing = []
        self.dt_from = params.get("dt_from")
        self.dt_upto = params.get("dt_upto")
        self.group_type = params.get("group_type")
        if self.dt_from:
            self.dt_from = datetime.fromisoformat(self.dt_from)
        else:
            self._missing.append("dt_from")
        if self.dt_upto:
            self.dt_upto = datetime.fromisoformat(self.dt_upto)
        else:
            self._missing.append("dt_upto")
        if not self.group_type:
            self._missing.append("group_type")

    def get_pipeline(self):
        return [
            {
                # Filter out documents with dates outside the provided range
                "$match": {
                    "dt": {
                        "$gte": self.dt_from,
                        "$lte": self.dt_upto,
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
                                "unit": self.group_type,
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

    def aggregate(self):
        if not self._missing:
            res = self.coll.aggregate(self.get_pipeline()).next()
            return json.dumps(res)
        missing = ", ".join(self._missing)
        pluralize = "s" if len(self._missing) > 1 else ""
        return f"Provide param{pluralize} {missing}."
