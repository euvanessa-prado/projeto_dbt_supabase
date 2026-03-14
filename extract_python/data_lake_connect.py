import boto3
import io
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
AWS_REGION_NAME = os.getenv("AWS_REGION_NAME")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")
POSTGRES_URL = os.getenv("POSTGRES_URL")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,
    region_name=AWS_REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
)

engine = create_engine(POSTGRES_URL)

response = s3.list_objects_v2(Bucket=BUCKET_NAME)
arquivos = [obj["Key"] for obj in response.get("Contents", [])]
print(f"Arquivos encontrados: {arquivos}")

parquets = [f for f in arquivos if f.endswith(".parquet")]

for arquivo in parquets:
    table_name = os.path.splitext(os.path.basename(arquivo))[0]
    print(f"Carregando {arquivo} → tabela '{table_name}'...")

    response = s3.get_object(Bucket=BUCKET_NAME, Key=arquivo)
    df = pd.read_parquet(io.BytesIO(response["Body"].read()))

    df.to_sql(table_name, con=engine, if_exists="replace", index=False)
    print(f"  {len(df)} linhas inseridas.")

print("Concluído.")
