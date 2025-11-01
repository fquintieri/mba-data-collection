# -*- coding: utf-8 -*-
"""
Upload incremental de arquivos JSON locais para a camada Bronze.
---------------------------------------------------------------
- Cria pasta: data=YYYYMMDD
- Adiciona timestamp (HHMMSS) aos arquivos dentro da pasta.
- Garante que nenhum arquivo é sobrescrito.
"""

from minio import Minio
from datetime import datetime
from io import BytesIO
import os

# === CONFIG ===
BUCKET_NAME = "data-ingest"
BRONZE_PREFIX = "bronze/json"
LOCAL_FOLDER = "/workspace/json"
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False

# === CONEXÃO ===
client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=SECURE
)
if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

# === DATA E HORA ===
data_folder = datetime.now().strftime("%Y%m%d")       # Ex: 20251101
hora_stamp = datetime.now().strftime("%H%M%S")        # Ex: 095530

folder = f"data={data_folder}"
base_path = f"{BRONZE_PREFIX}/{folder}/"

print(f"🚀 Upload incremental dos JSONs para {base_path}...")

# === UPLOAD INCREMENTAL ===
for filename in os.listdir(LOCAL_FOLDER):
    if filename.lower().endswith(".json"):
        # adiciona timestamp de hora ao nome do arquivo
        name, ext = os.path.splitext(filename)
        new_filename = f"{name}_{data_folder}_{hora_stamp}{ext}"

        path = os.path.join(LOCAL_FOLDER, filename)
        with open(path, "rb") as f:
            data = f.read()

        object_name = f"{base_path}{new_filename}"

        client.put_object(
            BUCKET_NAME,
            object_name,
            BytesIO(data),
            length=len(data),
            content_type="application/json"
        )

        print(f"✅ {filename} -> {object_name}")

print("🏁 Upload incremental concluído com sucesso!")
