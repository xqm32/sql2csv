import os
import sqlite3
from typing import Dict, List

import pandas


# 将 row 转换为 dict
def dict_from_row(row: sqlite3.Row) -> Dict:
    return dict(zip(row.keys(), row))


# 将 row 列表转换为 dict 列表
def dlist_from_rlist(rlist: List[sqlite3.Row]) -> List:
    return [dict_from_row(i) for i in rlist]


class SQL:
    def __init__(self, sql_name: str) -> None:
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")

        with open(sql_name, "r", encoding="utf-8") as f:
            self.db.executescript(f.read())

        self.sqlite_schema = self.db.execute("SELECT * FROM sqlite_schema").fetchall()

    # 获取表信息
    def table_info(self, table_name: str) -> List[sqlite3.Row]:
        return self.db.execute(f"PRAGMA table_info({table_name})").fetchall()

    # 获取表所有信息
    def table_xinfo(self, table_name: str) -> List[sqlite3.Row]:
        return self.db.execute(f"PRAGMA table_xinfo({table_name})").fetchall()

    # 获取表的外键
    def foreign_key_list(self, table_name: str) -> List[sqlite3.Row]:
        return self.db.execute(f"PRAGMA foreign_key_list({table_name})").fetchall()

    # 将 table 处理为 DataFrame
    def DataFrame_from_table(self, table_name: str) -> pandas.DataFrame:
        records = dlist_from_rlist(self.table_info(table_name))
        records = pandas.DataFrame.from_records(records)
        records["fk"] = ""

        foregin_keys = self.foreign_key_list(table_name)
        for j in foregin_keys:
            records.loc[records["name"] == j["from"], "fk"] = f"{j['table']}({j['to']})"

        return records

    # 原地本地化表格
    def localize_DataFrame(self, df: pandas.DataFrame) -> None:
        df.loc[df["pk"] != 1, "pk"] = ""
        df.loc[df["pk"] == 1, "pk"] = "主键"
        df.loc[df["notnull"] != 1, "notnull"] = ""
        df.loc[df["notnull"] == 1, "notnull"] = "不为空"
        df.rename(
            {
                "cid": "编号",
                "name": "名称",
                "type": "数据类型",
                "notnull": "不为空",
                "dflt_value": "默认值",
                "pk": "主键约束",
                "fk": "外键约束",
                "table": "表名",
            },
            axis="columns",
            inplace=True,
        )

    def save_DataFrame(self, df: pandas.DataFrame, path: str, **args) -> None:
        if args:
            df.to_csv(path, **args)
        else:
            df.to_csv(path, index_label="编号")

    def tables_to_csv(self) -> None:
        tables = pandas.DataFrame(columns=["name", "props"])

        for i in self.sqlite_schema:
            if i["type"] == "table" and not i["name"].startswith("sqlite"):
                print(f"{i['name']}", end="\t")

                records = dlist_from_rlist(self.table_info(i["name"]))
                records = pandas.DataFrame.from_records(records)

                tab = pandas.DataFrame(
                    [[i["name"], ", ".join(records["name"].to_list())]],
                    columns=["name", "props"],
                )
                tables = pandas.concat(
                    [tables, tab],
                    ignore_index=True,
                )

                print(" √")

        tables.rename(
            {"name": "表名", "props": "属性"},
            axis="columns",
            inplace=True,
        )
        self.save_DataFrame(tables, "tables.csv")

    def props_to_csv(self) -> None:
        props = pandas.DataFrame(
            columns=[
                "cid",
                "name",
                "type",
                "notnull",
                "dflt_value",
                "pk",
                "fk",
                "table",
            ]
        )

        for i in self.sqlite_schema:
            if i["type"] == "table" and not i["name"].startswith("sqlite"):
                print(f"{i['name']}", end="\t")

                records = self.DataFrame_from_table(i["name"])
                records["table"] = i["name"]

                props = pandas.concat([props, records], ignore_index=True)

                print(" √")

        props.drop("cid", axis="columns", inplace=True)
        self.localize_DataFrame(props)
        self.save_DataFrame(props, "props.csv")

    def to_csv(self) -> None:
        for i in self.sqlite_schema:
            if i["type"] == "table" and not i["name"].startswith("sqlite"):
                print(f"{i['name']}", end="\t")

                records = self.DataFrame_from_table(i["name"])

                records.drop("cid", axis="columns", inplace=True)
                self.localize_DataFrame(records)
                self.save_DataFrame(records, f"{i['name']}.csv")

                print(" √")


if __name__ == "__main__":
    # 请务必保证当前目录下 .sql 文件有且仅有一个
    sql_name = list(filter(lambda i: i.endswith(".sql"), os.listdir()))[0]
    sql = SQL(sql_name)
    sql.to_csv()
    sql.props_to_csv()
    sql.tables_to_csv()
