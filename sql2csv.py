import sqlite3
import os
from typing import List

import pandas

# 将 row 转换为 dict
def dict_from_row(row: sqlite3.Row):
    return dict(zip(row.keys(), row))


# 将 row 列表转换为 dict 列表
def dlist_from_rlist(rlist: List[sqlite3.Row]):
    return [dict_from_row(i) for i in rlist]


# 获取表信息
def table_info(table_name: str):
    return db.execute(f"PRAGMA table_info({table_name})").fetchall()


# 获取表所有信息
def table_xinfo(table_name: str):
    return db.execute(f"PRAGMA table_xinfo({table_name})").fetchall()


# 获取表的外键
def foreign_key_list(table_name: str):
    return db.execute(f"PRAGMA foreign_key_list({table_name})").fetchall()


if __name__ == "__main__":
    # 请务必保证当前目录下 .sql 文件有且仅有一个
    sql = list(filter(lambda i: i.endswith(".sql"), os.listdir()))[0]

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON")

    with open(sql, "r", encoding="utf-8") as f:
        db.executescript(f.read())

    sqlite_schema = db.execute("SELECT * FROM sqlite_schema").fetchall()

    for i in sqlite_schema:
        if i["type"] == "table":
            print(f"{i['name']}")

            records = dlist_from_rlist(table_info(i["name"]))
            records = pandas.DataFrame.from_records(records)
            records["fk"] = ""

            foregin_keys = foreign_key_list(i["name"])
            for j in foregin_keys:
                records.loc[
                    records["name"] == j["from"], "fk"
                ] = f"{j['table']}({j['to']})"

            records.rename(
                {
                    "cid": "编号",
                    "name": "名称",
                    "type": "数据类型",
                    "notnull": "不为空",
                    "dflt_value": "默认值",
                    "pk": "主键约束",
                    "fk": "外键约束",
                },
                axis="columns",
                inplace=True,
            )

            records.to_csv(f"{i['name']}.csv", index=False)
