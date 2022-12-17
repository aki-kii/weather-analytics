import sys
import io
from datetime import datetime, timedelta
import boto3
import pandas as pd

from awsglue.utils import getResolvedOption

def logic():
    '''
    メインロジック
    '''

    # S3クライアント
    s3 = boto3.resource('s3')
    src_obj = s3.Object(s3_raw_path, f'in/{s3_datalake_path}.csv')
    body = src_obj.get()['Body'].read().decode('shift-jis')
    buffer_str = io.StringIO(body)

    # DataFrameの成形
    df = pd.read_csv(buffer_str)
    df = df.loc[:, high_tempereture_column]
    df = df[df['観測所番号'].isin(observation_point)]

    # CSV → Parquet
    df.to_parquet(
        f'{s3_raw_path}/tmp/{high_tempereture_file}.parquet',
        compression='snappy'
    )

    # DataLakeへファイルアップロード
    s3.meta.client.upload_file(
        f'{s3_raw_path}/tmp/{high_tempereture_file}.parquet',
        s3_datalake_path,
        f'{high_tempereture_file}.parquet'
    )


if __name__ == "__main__":
    # 引数取得
    args = getResolvedOption(
        sys.argv,
        [
            "S3_BUCKET_RAW_PATH",
            "S3_BUCKET_DATALAKE_PATH"
        ]
    )

    # 定数
    # 観測地点
    SAPPORO = 14163
    TOKYO = 44132
    YOKOHAMA = 46106
    NAGOYA = 51106
    OSAKA = 62078
    FUKUOKA = 82182

    high_tempereture_column = [
        "観測所番号",
        "都道府県",
        "地点",
        "現在時刻(年)",
        "現在時刻(月)",
        "現在時刻(日)",
        "今日の最高気温(℃)"]

    # グローバル変数
    # rawデータ配置用S3バケット
    s3_raw_path = args["S3_BUCKET_RAW_PATH"]
    # datalake用S3バケット
    s3_datalake_path = args["S3_BUCKET_DATALAKE_PATH"]
    # 観測日付
    filedate = datetime.now() - timedelta(days=1)
    # 最高気温取得用ファイル
    high_tempereture_file = f'high_temperature_{filedate:%Y%m%d}'
    # 観測地点のリスト
    observation_point = [SAPPORO, TOKYO, YOKOHAMA, NAGOYA, OSAKA, FUKUOKA]

    logic()