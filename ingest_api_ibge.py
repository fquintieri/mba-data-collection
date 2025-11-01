# -- coding: utf-8 -- 
"""
Ingestão incremental da API BrasilAPI (IBGE UF) para a camada Bronze no MinIO.
Nova estrutura:
bronze/api/ibge_uf/data=YYYYMMDD/ibge-uf_YYYYMMDD_HHMMSS.json
"""

import requests
from datetime import datetime
from minio import Minio
from io import BytesIO
import json

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

# === DATA E HORA PARA NOMES ===
today = datetime.now().strftime("%Y%m%d")
hour = datetime.now().strftime("%H%M%S")

# Estrutura da pasta e nome do arquivo final
base_path = f"{BRONZE_PREFIX}/data={today}/"
file_name = f"ibge-uf_{today}_{hour}.json"
object_name = f"{base_path}{file_name}"

# === UPLOAD PARA MINIO ===
json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
client.put_object(
    BUCKET_NAME,
    object_name,
    BytesIO(json_bytes),
    length=len(json_bytes),
    content_type="application/json"
)

print(f"✅ Enviado para o MinIO -> {object_name}")
print("🏁 Ingestão concluída com sucesso!")
