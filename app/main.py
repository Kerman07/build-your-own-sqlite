import re
import sys

from typing import List, Union


def read_record_value_from_file(
    file: bytes, size_type: List[Union[int, str]]
) -> Union[int, str]:
    if size_type[1] == "int":
        return int.from_bytes(file.read(size_type[0]), byteorder="big")
    return file.read(size_type[0]).decode("utf-8")


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

    def init_header(self, file: bytes) -> None:
        self.type = file.read(1)
        self.free_block = int.from_bytes(file.read(2), byteorder="big")
        self.number_of_cells = int.from_bytes(file.read(2), byteorder="big")
        self.start_cell_content = int.from_bytes(file.read(2), byteorder="big")
        self.fragmented_bytes = int.from_bytes(file.read(1), byteorder="big")
        if self.type in [b"\x02", b"\x05"]:
            self.right_most_pointer = int.from_bytes(file.read(4), byteorder="big")


class SchemaRow:
    def __init__(self, file: bytes, sizes: List[Union[int, str]]) -> None:
        self.typ = read_record_value_from_file(file, sizes[0])
        self.name = read_record_value_from_file(file, sizes[1])
        self.table_name = read_record_value_from_file(file, sizes[2])
        self.rootpage = read_record_value_from_file(file, sizes[3])
        self.sql = read_record_value_from_file(file, sizes[4])
        columns = self.sql.split("(")[1][:-1]
        columns = [col.split()[0] for col in columns.split(",")]
        self.columns = {}
        for i, val in enumerate(columns):
            self.columns[val] = i


class SqliteSchema:
    def __init__(self, file: bytes, locations: List[int]) -> None:
        self.objects = {}
        self.get_schema(file, locations)

    def get_schema(self, file: bytes, locations: List[int]) -> None:
        for location in locations:
            file.seek(location)
            self.payload_size = Varint.parse(file)
            self.row_id = Varint.parse(file)
            self.header_size = Varint.parse(file)
            sizes = [SerialType.get_size(Varint.parse(file)) for _ in range(5)]
            obj = SchemaRow(file, sizes)
            self.objects[obj.name] = obj

    def get_ind_of_column(self, table, column) -> int:
        return self.objects[table].columns[column.strip()]


class SerialType:
    @staticmethod
    def get_size(size: int) -> int:
        mapping = {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 6, 6: 8, 7: 8, 8: 0, 9: 0}
        if size in mapping:
            return (mapping[size], "int")
        if size & 1:
            return ((size - 13) // 2, "text")
        return ((size - 12) // 2, "blob")


class Varint:
    @staticmethod
    def parse(file: bytes) -> int:
        var = int.from_bytes(file.read(1), byteorder="big")
        ans = var & 0x7F
        while (var >> 7) & 1:
            var = int.from_bytes(file.read(1), byteorder="big")
            ans = (ans << 7) + (var & 0x7F)
        return ans


class Record:
    def __init__(self, file: bytes) -> None:
        self.init_record(file)

    def init_record(self, file: bytes) -> None:
        self.payload_size = Varint.parse(file)
        self.row_id = Varint.parse(file)
        self.header_size = Varint.parse(file)
        file.read(1)
        self.column_sizes = [
            SerialType.get_size(Varint.parse(file)) for _ in range(self.header_size - 2)
        ]
        self.values = [self.row_id] + [
            read_record_value_from_file(file, size_type)
            for size_type in self.column_sizes
        ]


class Table:
    @staticmethod
    def get_records(file: bytes, page_offset: int, page_size: int) -> List[Record]:
        total_offset = page_offset * page_size
        file.seek(total_offset)
        btree_page_header = BTreePageHeader(file)
        records = []
        cell_pointers = [
            int.from_bytes(file.read(2), byteorder="big")
            for _ in range(btree_page_header.number_of_cells)
        ]
        for cell in cell_pointers:
            file.seek(cell + total_offset)
            if btree_page_header.type == b"\x05":
                tab_page = int.from_bytes(file.read(4), byteorder="big")
                records += Table.get_records(file, tab_page - 1, page_size)
            elif btree_page_header.type == b"\x0d":
                records.append(Record(file))
        return records


def main():
    database_file_path = sys.argv[1]
    command = sys.argv[2]
    command = (
        command.replace("SELECT", "select")
        .replace("FROM", "from")
        .replace("WHERE", "where")
    )

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
                    obj.name for obj in schema.objects.values() if obj.typ == "table"
                )
            )
        elif command.startswith("select count(*)"):
            table = command.split(" ")[-1]
            table_page = schema.objects[table].rootpage
            database_file.seek((table_page - 1) * db_header.page_size)
            btree_page_header = BTreePageHeader(database_file)
            print(btree_page_header.number_of_cells)
        elif command.startswith("select "):
            pattern = r"select\s*([\w\s,]*)\s*from\s*(\w+)"
            matched = re.search(pattern, command)
            slt_columns, slt_table = matched.group(1).split(", "), matched.group(2)
            table_page = schema.objects[slt_table].rootpage
            records = Table.get_records(
                database_file, table_page - 1, db_header.page_size
            )
            if "where" in command:
                _, condition = command.replace("'", "").split("where ", 1)
                column, operator, *value = condition.split(" ")
                value = " ".join(value)
                index = schema.get_ind_of_column(slt_table, column)
                if operator == "=":
                    records = [rec for rec in records if rec.values[index] == value]
                elif operator == "!=":
                    records = [rec for rec in records if rec.values[index] != value]
            for record in records:
                print(
                    "|".join(
                        str(record.values[schema.get_ind_of_column(slt_table, column)])
                        for column in slt_columns
                    )
                )
        else:
            print(f"Invalid command: {command}")


if __name__ == "__main__":
    main()
