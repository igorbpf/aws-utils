from io import StringIO
import boto3
import pandas as pd

s3_resource = boto3.resource('s3')


def save_df_s3(df: pd.DataFrame, bucket_name: str, path: str, file_name: str) -> None:
    csv_buffer = StringIO()

    df.to_csv(csv_buffer, index=False)

    s3_resource.Object(
        bucket_name, f'{path}/{file_name}.csv').put(Body=csv_buffer.getvalue())


def send_file_s3(file_path: str, bucket_name: str, s3_key: str) -> None:
    client = boto3.client("s3")
    client.upload_file(file_path, bucket_name, s3_key)


if __name__ == "__main__":
    boto3.setup_default_session(profile_name='spinyleaks-dev')

    send_file_s3("./data/inf_diario_fi_202304.csv", "dev-leaksspiny-lake",
                 "raw/cvm/cotas-fundos-parquet2/cotas_parquet.parquet")
