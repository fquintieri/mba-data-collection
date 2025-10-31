# -- coding: utf-8 --
"""
Upload incremental de arquivos JSON locais para a camada Bronze.
Cria novas pastas data_ingestao=YYYY-MM-DD_NN a cada execução.
"""

from minio import Minio
from datetime import datetime
from io import BytesIO
import os, re

# === CONFIG ===
BUCKET_NAME = "data-ingest"
BRONZE_PREFIX = "bronze/json"
LOCAL_FOLDER = "/workspace/json"
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False

# === CONEXÃO ===
client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

# === PASTA INCREMENTAL ===
today = datetime.now().strftime("%Y-%m-%d")
existing = [o.object_name for o in client.list_objects(BUCKET_NAME, prefix=f"{BRONZE_PREFIX}/data_ingestao={today}", recursive=False)]
ids = [int(m.group(1)) for n in existing if (m := re.search(r"data_ingestao=\d{4}-\d{2}-\d{2}_(\d+)", n))]
next_id = max(ids, default=0) + 1
folder = f"data_ingestao={today}_{next_id:02d}"
base_path = f"{BRONZE_PREFIX}/{folder}/"

print(f"🚀 Upload incremental dos JSONs para {base_path}...")
for filename in os.listdir(LOCAL_FOLDER):
    if filename.lower().endswith(".json"):
        path = os.path.join(LOCAL_FOLDER, filename)
        with open(path, "rb") as f:
            data = f.read()
        object_name = f"{base_path}{filename}"
        client.put_object(BUCKET_NAME, object_name, BytesIO(data), length=len(data), content_type="application/json")
        print(f"✅ {filename} -> {object_name}")

print("🏁 Upload incremental concluído com sucesso!")
