# -- coding: utf-8 --
"""
Ingestão incremental das tabelas db_loja (a partir do Script-DDL-dbloja.sql)
Cria novas pastas a cada execução: data_ingestao=YYYY-MM-DD_NN
"""

from minio import Minio
from io import BytesIO
import pandas as pd, re, os
from datetime import datetime
import re, json

# === CONFIGURAÇÕES ===
SQL_FILE = "qry/Script-DDL-dbloja.sql"
BUCKET_NAME = "data-ingest"
BRONZE_PREFIX = "bronze/dbloja"
SCHEMA = "db_loja"

MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False

def parse_sql_file(sql_path):
    """Extrai tabelas e dados do SQL PostgreSQL."""
    with open(sql_path, "r", encoding="utf-8") as f:
        content = f.read()
    content = re.sub(r'--.*', '', content)
    content = re.sub(r'\s+', ' ', content)
    data = {}
    create_pattern = rf"CREATE TABLE\s+(?:{SCHEMA}\.)?(\w+)\s*\((.*?)\);"
    for match in re.finditer(create_pattern, content, re.IGNORECASE):
        table = match.group(1)
        cols = []
        for col_def in re.split(r",(?![^()]*\))", match.group(2)):
            col_def = col_def.strip()
            if not col_def or col_def.upper().startswith(("PRIMARY", "FOREIGN", "CONSTRAINT")):
                continue
            col_match = re.match(r'"?(\w+)"?\s+([\w\s\(\),]+)', col_def)
            if col_match:
                cols.append(col_match.group(1))
        data[table] = {"columns": cols, "rows": []}
    insert_pattern = rf"INSERT INTO\s+(?:{SCHEMA}\.)?(\w+)\s*\((.+?)\)\s*VALUES\s(.*?);"
    for match in re.finditer(insert_pattern, content, re.IGNORECASE):
        table = match.group(1)
        cols = [c.strip().strip('"') for c in match.group(2).split(",")]
        tuples = re.findall(r"\((.*?)\)", match.group(3))
        for t in tuples:
            vals = [v.strip().strip("'") for v in re.split(r",(?![^']*')", t)]
            if table not in data:
                data[table] = {"columns": cols, "rows": []}
            data[table]["rows"].append(dict(zip(cols, vals)))
    return data

def main():
    print("🧩 Extraindo tabelas do arquivo SQL...")
    data = parse_sql_file(SQL_FILE)
    if not data:
        print("⚠ Nenhuma tabela encontrada.")
        return

    client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    today = datetime.now().strftime("%Y-%m-%d")
    existing = [o.object_name for o in client.list_objects(BUCKET_NAME, prefix=f"{BRONZE_PREFIX}/data_ingestao={today}", recursive=False)]
    ids = [int(m.group(1)) for n in existing if (m := re.search(r"data_ingestao=\d{4}-\d{2}-\d{2}_(\d+)", n))]
    next_id = max(ids, default=0) + 1
    folder = f"data_ingestao={today}_{next_id:02d}"
    base_path = f"{BRONZE_PREFIX}/{folder}/"

    print(f"📁 Nova pasta: {base_path}")
    for table, tbl_data in data.items():
        df = pd.DataFrame(tbl_data["rows"]).convert_dtypes()
        parquet = BytesIO()
        df.to_parquet(parquet, index=False)
        parquet.seek(0)
        object_name = f"{base_path}{table}.parquet"
        client.put_object(BUCKET_NAME, object_name, parquet, length=len(parquet.getvalue()), content_type="application/octet-stream")
        print(f"✅ {table} enviada -> {object_name}")

    print("🏁 Ingestão incremental concluída com sucesso!")

if __name__ == "__main__":
    main()
