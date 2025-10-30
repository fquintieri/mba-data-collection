#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline de Processamento - Camada Silver (via SDK MinIO)
---------------------------------------------------------
Substitui o acesso s3a:// pelo uso do SDK MinIO para:
- Baixar os arquivos Bronze (JSON/CSV) do bucket;
- Processar com PySpark;
- Subir os Parquets Silver de volta ao MinIO;
- Testar o acesso via navegador (endpoint HTTP).

Compatível com ambiente local, Docker e Codespaces.
"""

import os
import io
import re
import tempfile
import shutil
import requests
from typing import Iterable
from minio import Minio
from minio.deleteobjects import DeleteObject
from pyspark.sql import SparkSession, functions as F, types as T

# =======================================================
# CONFIGURAÇÕES GERAIS
# =======================================================
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET           = os.getenv("DL_BUCKET", "datalake")

BRONZE_PREFIX    = "bronze/dbloja/"
SILVER_PREFIX    = "silver/dbloja/"

# =======================================================
# CONECTAR AO MINIO
# =======================================================
def connect_minio() -> Minio:
    print("Conectando ao MinIO…")
    cli = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    cli.list_buckets()
    print("✅ Conectado com sucesso.")
    return cli

# =======================================================
# FUNÇÕES AUXILIARES
# =======================================================
def remove_prefix(minio_client: Minio, prefix: str):
    """Remove arquivos antigos do prefixo no bucket."""
    print(f"🧹 Limpando prefixo existente: s3://{BUCKET}/{prefix}")
    to_delete: Iterable[DeleteObject] = (
        DeleteObject(obj.object_name)
        for obj in minio_client.list_objects(BUCKET, prefix=prefix, recursive=True)
    )
    errors = list(minio_client.remove_objects(BUCKET, to_delete))
    if errors:
        print("⚠️ Erros ao remover alguns objetos:")
        for e in errors:
            print(e)
    else:
        print("✅ Prefixo limpo.")

def upload_directory(minio_client: Minio, local_dir: str, dest_prefix: str):
    """Sobe todos os arquivos de local_dir para o MinIO preservando nomes."""
    print(f"📤 Upload {local_dir} → s3://{BUCKET}/{dest_prefix}")
    for root, _, files in os.walk(local_dir):
        for fname in files:
            full = os.path.join(root, fname)
            rel = os.path.relpath(full, start=local_dir).replace("\\", "/")
            key = f"{dest_prefix}{rel}"
            minio_client.fput_object(BUCKET, key, full)
            print(f"✅ PUT {key}")
    print("✅ Upload completo.")

def test_browser_access(prefix: str):
    """Verifica se o prefixo Silver está acessível via navegador HTTP."""
    protocol = "https" if MINIO_SECURE else "http"
    url = f"{protocol}://{MINIO_ENDPOINT}/{BUCKET}/{prefix}"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            print(f"🌐 Verificação OK: {url}")
        else:
            print(f"⚠️ HTTP {resp.status_code} ao acessar {url}")
    except Exception as e:
        print(f"⚠️ Falha ao testar acesso via browser: {e}")

# =======================================================
# PIPELINE PRINCIPAL
# =======================================================
def main():
    print("\n🚀 Iniciando pipeline da camada Silver...\n")
    spark = SparkSession.builder.appName("ProcessamentoSilverMinIO").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    minio = connect_minio()

    workdir = tempfile.mkdtemp(prefix="silver_process_")
    try:
        # Exemplo: processar pedidos CSV (Bronze)
        print("📥 Baixando dados Bronze (pedido_cabecalho)...")
        bronze_path = f"{BRONZE_PREFIX}pedido_cabecalho/"
        local_csv = os.path.join(workdir, "pedido_cabecalho.csv")

        # Baixa o primeiro CSV encontrado
        for obj in minio.list_objects(BUCKET, prefix=bronze_path, recursive=True):
            if obj.object_name.endswith(".csv"):
                print(f"Encontrado: {obj.object_name}")
                minio.fget_object(BUCKET, obj.object_name, local_csv)
                break
        else:
            raise RuntimeError("Nenhum arquivo CSV encontrado no Bronze.")

        print("💾 Lendo CSV com Spark...")
        df = (
            spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(f"file://{local_csv}")
        )

        # Exemplo de transformação
        df_transf = df.withColumn("valor_total_num", F.col("valor_total").cast(T.DoubleType()))

        print("🧱 Gravando parquet local...")
        local_out = os.path.join(workdir, "silver_pedidos")
        df_transf.write.mode("overwrite").parquet(local_out)

        # Remover marcadores de sucesso
        for fname in ["_SUCCESS", "._SUCCESS"]:
            fpath = os.path.join(local_out, fname)
            if os.path.exists(fpath):
                os.remove(fpath)

        print("🚀 Publicando Silver no MinIO...")
        dest_prefix = f"{SILVER_PREFIX}pedido_cabecalho/"
        remove_prefix(minio, dest_prefix)
        upload_directory(minio, local_out, dest_prefix)

        print("\n✅ Silver publicado em s3://{}/{}\n".format(BUCKET, dest_prefix))

        # Teste HTTP (browser)
        test_browser_access(dest_prefix)

    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        spark.stop()
        print("🧹 Limpeza concluída. Pipeline finalizado.\n")


if __name__ == "__main__":
    main()
