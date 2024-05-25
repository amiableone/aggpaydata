import os

from bson import decode_all
from bson.raw_bson import RawBSONDocument
from pathlib import Path

data = Path(os.path.dirname(__file__)) / "data"


class BSONReader:
    path = data
    file_name = "sample_collection.bson"

    def __init__(self, file_name=""):
        """
        Provide a name of bson file.
        Default is "sample_collection.bson"

        The file must be located in directory 'data'.
        """
        self.file = self.path / (file_name or self.file_name)

    def read(self):
        with open(self.file, "rb") as f:
            self.docs = decode_all(f.read())
        return self.docs
