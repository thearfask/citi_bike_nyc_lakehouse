from pathlib import Path

def execute_ddl(sql_file: str):
    sql_script = Path(sql_file).read_text(encoding="utf-8")

    statements = [
        stmt.strip()
        for stmt in sql_script.split(";")
        if stmt.strip()
    ]

    for statement in statements:
        spark.sql(statement)

    print("DDL execution completed.")