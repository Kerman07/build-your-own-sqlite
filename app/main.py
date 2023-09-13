import struct
import sys

from collections import defaultdict
from dataclasses import dataclass
from typing import List


class Database:
    def __init__(self, file: bytes) -> None:
        self.file = file
        self.header = DatabaseHeader(file)


class DatabaseHeader:
    def __init__(self, file: bytes) -> None:
        self.page_size = self.get_page_size(file)

    def get_page_size(self, file: bytes) -> int:
        file.seek(16)
        return int.from_bytes(file.read(2), byteorder="big")


class BTreePageHeader:
    def __init__(self, file: bytes) -> None:
        self.init_header(file)

    def init_header(self, file: bytes):
        self.type = file.read(1)
        self.free_block = int.from_bytes(file.read(2), byteorder="big")
        self.number_of_cells = int.from_bytes(file.read(2), byteorder="big")
        self.start_cell_content = int.from_bytes(file.read(2), byteorder="big")
        self.fragmented_bytes = int.from_bytes(file.read(1), byteorder="big")
        if self.type in [b"\X02", b"\X05"]:
            self.right_most_pointer = int.from_bytes(file.read(4), byteorder="big")


class SchemaRow:
    def __init__(self, file: bytes, sizes: List[int]) -> None:
        self.typ = file.read(sizes[0]).decode("utf-8")
        self.name = file.read(sizes[1]).decode("utf-8")
        self.table_name = file.read(sizes[2]).decode("utf-8")
        self.rootpage = int.from_bytes(file.read(sizes[3]), byteorder="big")
        self.sql = file.read(sizes[4]).decode("utf-8")


class SqliteSchema:
    def __init__(self, file: bytes, locations: List[int]) -> None:
        self.objects = {}
        self.get_schema(file, locations)

    def get_schema(self, file: bytes, locations: List[int]):
        for location in locations:
            file.seek(location)
            self.payload_size = Varint.parse(file)
            self.row_id = Varint.parse(file)
            self.header_size = Varint.parse(file)
            sizes = [SerialType.get_size(Varint.parse(file)) for _ in range(5)]
            obj = SchemaRow(file, sizes)
            self.objects[obj.name] = obj


class SerialType:
    @staticmethod
    def get_size(size: int) -> int:
        mapping = {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 6, 6: 8, 7: 8, 8: 0, 9: 0}
        if size in mapping:
            return mapping[size]
        if size & 1:
            return (size - 13) // 2
        return (size - 12) // 2


class Varint:
    @staticmethod
    def parse(file: bytes) -> int:
        var = int.from_bytes(file.read(1), byteorder="big")
        ans = var & 0x7F
        while (var >> 7) & 1:
            var = int.from_bytes(file.read(1), byteorder="big")
            ans = (ans << 7) + (var & 0x7F)
        return ans


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2]

    with open(database_file_path, "rb") as database_file:
        db = Database(database_file)
        db_header = db.header
        database_file.seek(100)
        first_page_btree_header = BTreePageHeader(database_file)
        database_file.seek(108)
        locations = [
            int.from_bytes(database_file.read(2), byteorder="big")
            for _ in range(first_page_btree_header.number_of_cells)
        ]
        schema = SqliteSchema(database_file, locations)
        if command == ".dbinfo":
            print(f"database page size: {db_header.page_size}")
            print(f"number of tables: {first_page_btree_header.number_of_cells}")
        elif command == ".tables":
            print(
                " ".join(
                    object.name
                    for object in schema.objects.values()
                    if object.typ == "table"
                )
            )
        elif command.startswith("select count(*)"):
            table = command.split(" ")[-1]
            table_page = schema.objects[table].rootpage
            database_file.seek((table_page - 1) * db_header.page_size)
            btree_page_header = BTreePageHeader(database_file)
            print(btree_page_header.number_of_cells)
        else:
            print(f"Invalid command: {command}")


if __name__ == "__main__":
    main()
