from pyspark.sql import SparkSession
import argparse


def main(input_s3_path, output_s3_path):
    spark = SparkSession.builder.appName("Transform Region Data").getOrCreate()

    # S3 경로 설정
    data_path = f"{input_s3_path}*/region_data/*.parquet"
    output_path = f"{output_s3_path}/region_data/"

    # 데이터 로드
    df = spark.read.parquet(data_path)

    # 데이터 일부 출력 (상위 5개)
    df.show(5)

    # 데이터 필터링 및 중복 제거 (예시)
    transformed_df = df

    # 변환된 데이터 S3에 저장
    transformed_df.write.mode("overwrite").parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_s3_path", required=True)
    parser.add_argument("--output_s3_path", required=True)
    
    args = parser.parse_args()
    main(args.input_s3_path, args.output_s3_path)