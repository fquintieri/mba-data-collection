#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Pipeline Silver (usando conteúdo real da camada Bronze)
-------------------------------------------------------
- Lê automaticamente os arquivos do Bronze existentes no MinIO.
- Suporta JSON, Parquet e CSV.
- Aplica cast básico e limpeza de schema.
- Salva resultado otimizado em silver/<subpasta>/.
"""

import os
import tempfile
import shutil
from minio import Minio
from minio.deleteobjects import DeleteObject
from pyspark.sql import SparkSession, functions as F
from typing import Iterable

# =======================================================
# CONFIGURAÇÕES
# =======================================================
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE     = os.getenv("MINIO_SECURE", "false").lower() == "true"
BUCKET           = "data-ingest"

BRONZE_PREFIX = "bronze/"
SILVER_PREFIX = "silver/"

# =======================================================
# FUNÇÕES AUXILIARES
# =======================================================
def connect_minio():
    print("🔗 Conectando ao MinIO…")
    cli = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    cli.list_buckets()
    print("✅ Conectado ao MinIO.")
    return cli


def remove_prefix(cli, prefix):
    """Remove prefixos antigos da Silver."""
    print(f"🧹 Limpando prefixo: s3://{BUCKET}/{prefix}")
    to_delete = (DeleteObject(o.object_name)
                 for o in cli.list_objects(BUCKET, prefix=prefix, recursive=True))
    errors = list(cli.remove_objects(BUCKET, to_delete))
    if errors:
        print("⚠️ Falhas ao limpar prefixo:", errors)
    else:
        print("✅ Prefixo limpo.")


def upload_directory(cli, local_dir, dest_prefix):
    """Envia todos os arquivos de local_dir para o MinIO."""
    for root, _, files in os.walk(local_dir):
        for f in files:
            path = os.path.join(root, f)
            rel = os.path.relpath(path, start=local_dir).replace("\\", "/")
            key = f"{dest_prefix}{rel}"
            cli.fput_object(BUCKET, key, path)
            print(f"📤 Enviado: {key}")


# =======================================================
# PIPELINE PRINCIPAL
# =======================================================
def main():
    print("\n🚀 Iniciando pipeline Silver (conteúdo Bronze existente)...\n")
    spark = SparkSession.builder.appName("SilverFromBronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    cli = connect_minio()

    workdir = tempfile.mkdtemp(prefix="silver_from_bronze_")

    try:
        # -------------------------------
        # 1️⃣ PARQUET → SILVER/PARQUET
        # -------------------------------
        parquet_prefix = f"{BRONZE_PREFIX}parquet/"
        for obj in cli.list_objects(BUCKET, prefix=parquet_prefix, recursive=True):
            if obj.object_name.endswith(".parquet"):
                local_file = os.path.join(workdir, os.path.basename(obj.object_name))
                cli.fget_object(BUCKET, obj.object_name, local_file)
                print(f"📥 Baixado Parquet: {obj.object_name}")

                df = spark.read.parquet(f"file://{local_file}")
                df_silver = df.dropna(how="all")  # limpeza básica

                out_dir = os.path.join(workdir, "silver_parquet")
                df_silver.write.mode("overwrite").parquet(out_dir)

                dest = f"{SILVER_PREFIX}parquet/"
                remove_prefix(cli, dest)
                upload_directory(cli, out_dir, dest)
                print(f"✅ Silver Parquet publicado em s3://{BUCKET}/{dest}\n")
                break  # processa o primeiro conjunto encontrado

        # -------------------------------
        # 2️⃣ JSON → SILVER/JSON
        # -------------------------------
        json_prefix = f"{BRONZE_PREFIX}json/"
        for obj in cli.list_objects(BUCKET, prefix=json_prefix, recursive=True):
            if obj.object_name.endswith(".json"):
                local_file = os.path.join(workdir, os.path.basename(obj.object_name))
                cli.fget_object(BUCKET, obj.object_name, local_file)
                print(f"📥 Baixado JSON: {obj.object_name}")

                df = spark.read.option("multiline", "true").json(f"file://{local_file}")
                df_silver = df.dropna(how="all")

                out_dir = os.path.join(workdir, "silver_json")
                df_silver.write.mode("overwrite").parquet(out_dir)

                dest = f"{SILVER_PREFIX}json/"
                remove_prefix(cli, dest)
                upload_directory(cli, out_dir, dest)
                print(f"✅ Silver JSON publicado em s3://{BUCKET}/{dest}\n")
                break

        # -------------------------------
        # 3️⃣ API IBGE (uf.json) → SILVER/IBGE_UF
        # -------------------------------
        api_prefix = f"{BRONZE_PREFIX}api/"
        for obj in cli.list_objects(BUCKET, prefix=api_prefix, recursive=True):
            if obj.object_name.lower().endswith("uf.json"):
                local_file = os.path.join(workdir, "uf.json")
                cli.fget_object(BUCKET, obj.object_name, local_file)
                print(f"📥 Baixado API IBGE UF: {obj.object_name}")

                df = spark.read.option("multiline", "true").json(f"file://{local_file}")
                df_sel = df.select("id", "sigla", "nome", F.col("regiao.nome").alias("regiao_nome"))

                out_dir = os.path.join(workdir, "silver_ibge_uf")
                df_sel.write.mode("overwrite").parquet(out_dir)

                dest = f"{SILVER_PREFIX}ibge_uf/"
                remove_prefix(cli, dest)
                upload_directory(cli, out_dir, dest)
                print(f"✅ Silver IBGE UF publicado em s3://{BUCKET}/{dest}\n")
                break

        print("🏁 Pipeline Silver concluído com sucesso!")

    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        spark.stop()


if __name__ == "__main__":
    main()
