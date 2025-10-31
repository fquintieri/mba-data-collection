# -- coding: utf-8 --
"""
Ingestão incremental da API BrasilAPI (IBGE UF) para a camada Bronze no MinIO.
Cria pastas incrementais (data_ingestao=YYYY-MM-DD_NN).
"""

import requests
from datetime import datetime
from minio import Minio
from io import BytesIO
import json, re

# === CONFIGURAÇÕES ===
API_URL = "https://brasilapi.com.br/api/ibge/uf/v1"
BUCKET_NAME = "data-ingest"
BRONZE_PREFIX = "bronze/api/ibge_uf"

MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False

# === CONEXÃO ===
client = Minio(MINIO_ENDPOINT, access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=SECURE)

if not client.bucket_exists(BUCKET_NAME):
    client.make_bucket(BUCKET_NAME)

# === REQUISIÇÃO HTTP ===
print(f"🌐 Requisitando dados da API: {API_URL}")
response = requests.get(API_URL)
response.raise_for_status()
data = response.json()
print(f"✅ {len(data)} registros recebidos.")

# === CALCULA PASTA INCREMENTAL ===
today = datetime.now().strftime("%Y-%m-%d")
existing = [o.object_name for o in client.list_objects(BUCKET_NAME, prefix=f"{BRONZE_PREFIX}/data_ingestao={today}", recursive=False)]
existing_ids = []
for n in existing:
    m = re.search(r"data_ingestao=\d{4}-\d{2}-\d{2}_(\d+)", n)
    if m: existing_ids.append(int(m.group(1)))
next_id = max(existing_ids, default=0) + 1
folder = f"data_ingestao={today}_{next_id:02d}"
object_name = f"{BRONZE_PREFIX}/{folder}/uf.json"

# === UPLOAD ===
json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
client.put_object(BUCKET_NAME, object_name, BytesIO(json_bytes), length=len(json_bytes), content_type="application/json")
print(f"✅ Enviado para o MinIO -> {object_name}")
print(f"🏁 Nova pasta criada: {folder}")
