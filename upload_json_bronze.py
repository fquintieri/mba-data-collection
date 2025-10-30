# -- coding: utf-8 --
"""
Envia arquivos JSON locais da pasta /workspace/json
para a camada Bronze no MinIO (bucket 'data-ingest').
"""

from minio import Minio
from datetime import datetime
import os
from io import BytesIO

# === CONFIGURAÇÕES ===
BUCKET_NAME = "data-ingest"
BRONZE_PREFIX = "bronze/json"
LOCAL_FOLDER = "/workspace/json"  # caminho real no teu Codespace
MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False

# === CONEXÃO COM MINIO ===
client = Minio(
    MINIO_ENDPOINT,
    access_key=ACCESS_KEY,
    secret_key=SECRET_KEY,
    secure=SECURE
)

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)
    print(f"🪣 Bucket '{BUCKET_NAME}' criado.")
else:
    print(f"🪣 Bucket '{BUCKET_NAME}' já existe.")

# === DIRETÓRIO DE DESTINO ===
date_str = datetime.now().strftime("%Y%m%d")
timestamp_str = datetime.now().strftime("%H%M%S")
base_path = f"{BRONZE_PREFIX}/data={date_str}/"

# === ENVIO DOS ARQUIVOS JSON ===
print(f"🚀 Iniciando upload dos arquivos JSON da pasta '{LOCAL_FOLDER}'...")

for filename in os.listdir(LOCAL_FOLDER):
    if not filename.lower().endswith(".json"):
        continue

    local_path = os.path.join(LOCAL_FOLDER, filename)
    with open(local_path, "rb") as f:
        data = f.read()

    # Monta o nome do arquivo no MinIO
    object_name = f"{base_path}{filename.replace('.json', f'_{timestamp_str}.json')}"

    client.put_object(
        BUCKET_NAME,
        object_name,
        BytesIO(data),
        length=len(data),
        content_type="application/json"
    )

    print(f"✅ {filename} -> {object_name}")

print("\n🏁 Todos os arquivos JSON foram enviados para a camada Bronze com sucesso!")
