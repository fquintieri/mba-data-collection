# -- coding: utf-8 --
"""
Pipeline Silver incremental:
- Detecta automaticamente a última pasta da camada Bronze (mais recente)
- Processa e grava o resultado na camada Silver com versionamento incremental.
"""

import os
import re
import tempfile
import shutil
from datetime import datetime
from minio import Minio
from pyspark.sql import SparkSession, functions as F
from typing import Iterable

# =======================================================
# CONFIGURAÇÕES GERAIS
# =======================================================
BUCKET = "data-ingest"
BRONZE_PREFIX = "bronze/"
SILVER_PREFIX = "silver/"

MINIO_ENDPOINT = "minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
SECURE = False

# =======================================================
# FUNÇÕES AUXILIARES
# =======================================================
def connect_minio():
    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=SECURE,
    )
    client.list_buckets()
    return client


def find_latest_prefix(client, base_prefix: str) -> str:
    """Encontra o último lote incremental (maior data_ingestao) dentro de uma pasta Bronze."""
    all_folders = set()
    for obj in client.list_objects(BUCKET, prefix=base_prefix, recursive=True):
        m = re.search(r"(data_ingestao=\d{4}-\d{2}-\d{2}_\d{2})", obj.object_name)
        if m:
            all_folders.add(m.group(1))
    if not all_folders:
        return None
    return sorted(all_folders)[-1]  # retorna o último


def find_next_silver_folder(client, silver_prefix: str) -> str:
    """Determina o próximo número incremental para a Silver."""
    today = datetime.now().strftime("%Y-%m-%d")
    existing = [o.object_name for o in client.list_objects(BUCKET, prefix=f"{silver_prefix}data_processamento={today}", recursive=False)]
    ids = [int(m.group(1)) for n in existing if (m := re.search(r"data_processamento=\d{4}-\d{2}-\d{2}_(\d+)", n))]
    next_id = max(ids, default=0) + 1
    return f"data_processamento={today}_{next_id:02d}"


def upload_directory(client, local_dir, dest_prefix):
    """Faz upload recursivo de todos os arquivos de uma pasta local para o MinIO."""
    for root, _, files in os.walk(local_dir):
        for f in files:
            full = os.path.join(root, f)
            rel = os.path.relpath(full, start=local_dir).replace("\\", "/")
            key = f"{dest_prefix}{rel}"
            client.fput_object(BUCKET, key, full)
            print(f"📤 {key}")


# =======================================================
# PIPELINE PRINCIPAL
# =======================================================
def main():
    print("\n🚀 Iniciando pipeline Silver incremental...\n")
    spark = SparkSession.builder.appName("SilverIncremental").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    client = connect_minio()

    workdir = tempfile.mkdtemp(prefix="silver_incremental_")

    try:
        # -------------------------------------------
        # 1️⃣ API IBGE UF
        # -------------------------------------------
        ibge_bronze = "bronze/api/ibge_uf/"
        latest_ibge = find_latest_prefix(client, ibge_bronze)
        if latest_ibge:
            print(f"🌎 Processando IBGE UF -> {latest_ibge}")
            local_json = os.path.join(workdir, "uf.json")
            client.fget_object(BUCKET, f"{ibge_bronze}{latest_ibge}/uf.json", local_json)
            df_ibge = spark.read.option("multiline", "true").json(f"file://{local_json}")
            df_ibge_sel = df_ibge.select(
                "id",
                "sigla",
                "nome",
                F.col("regiao.nome").alias("regiao_nome")
            )

            silver_ibge_prefix = f"{SILVER_PREFIX}api/ibge_uf/"
            new_folder = find_next_silver_folder(client, silver_ibge_prefix)
            local_out = os.path.join(workdir, "silver_ibge_uf")
            df_ibge_sel.write.mode("overwrite").parquet(local_out)
            upload_directory(client, local_out, f"{silver_ibge_prefix}{new_folder}/")
            print(f"✅ IBGE UF salvo em {silver_ibge_prefix}{new_folder}/\n")
        else:
            print("⚠️ Nenhum lote IBGE encontrado na Bronze.\n")

        # -------------------------------------------
        # 2️⃣ DB_LOJA (Parquet)
        # -------------------------------------------
        dbloja_bronze = "bronze/dbloja/"
        latest_dbloja = find_latest_prefix(client, dbloja_bronze)
        if latest_dbloja:
            print(f"🏪 Processando DB_LOJA -> {latest_dbloja}")
            objects = [o for o in client.list_objects(BUCKET, prefix=f"{dbloja_bronze}{latest_dbloja}/", recursive=True) if o.object_name.endswith(".parquet")]

            for obj in objects:
                table = os.path.basename(obj.object_name).replace(".parquet", "")
                local_parquet = os.path.join(workdir, os.path.basename(obj.object_name))
                client.fget_object(BUCKET, obj.object_name, local_parquet)

                df = spark.read.parquet(f"file://{local_parquet}")
                df_silver = df.dropna(how="all")

                silver_tbl_prefix = f"{SILVER_PREFIX}dbloja/{table}/"
                new_folder = find_next_silver_folder(client, silver_tbl_prefix)
                local_out = os.path.join(workdir, f"silver_{table}")
                df_silver.write.mode("overwrite").parquet(local_out)
                upload_directory(client, local_out, f"{silver_tbl_prefix}{new_folder}/")
                print(f"✅ {table} salvo em {silver_tbl_prefix}{new_folder}/")
        else:
            print("⚠️ Nenhum lote db_loja encontrado na Bronze.\n")

        # -------------------------------------------
        # 3️⃣ JSONS LIVRES
        # -------------------------------------------
        json_bronze = "bronze/json/"
        latest_json = find_latest_prefix(client, json_bronze)
        if latest_json:
            print(f"🧾 Processando JSONs -> {latest_json}")
            json_objects = [o for o in client.list_objects(BUCKET, prefix=f"{json_bronze}{latest_json}/", recursive=True) if o.object_name.endswith(".json")]
            silver_json_prefix = f"{SILVER_PREFIX}json/"
            new_folder = find_next_silver_folder(client, silver_json_prefix)
            local_out = os.path.join(workdir, "silver_json")

            for obj in json_objects:
                local_json = os.path.join(workdir, os.path.basename(obj.object_name))
                client.fget_object(BUCKET, obj.object_name, local_json)
                df_json = spark.read.option("multiline", "true").json(f"file://{local_json}")
                df_json = df_json.withColumn("arquivo_origem", F.lit(os.path.basename(obj.object_name)))
                df_json.write.mode("append").parquet(local_out)

            upload_directory(client, local_out, f"{silver_json_prefix}{new_folder}/")
            print(f"✅ JSONs salvos em {silver_json_prefix}{new_folder}/")
        else:
            print("⚠️ Nenhum lote JSON encontrado na Bronze.\n")

        print("🏁 Pipeline Silver incremental finalizado com sucesso!\n")

    finally:
        shutil.rmtree(workdir, ignore_errors=True)
        spark.stop()


if __name__ == "__main__":
    main()
