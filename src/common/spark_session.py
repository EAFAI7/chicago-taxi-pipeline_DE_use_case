"""
Fabrique une SparkSession en mode local, configurée avec le connecteur S3A
pour lire/écrire directement sur MinIO (stockage objet compatible S3, socle
du datalake pour ce pipeline).

On tourne en local[*] plutôt que sur un cluster Spark dédié : choix assumé
pour que `docker compose up` reste simple et auto-suffisant (voir README).
"""
import os

from pyspark.sql import SparkSession

_JARS = ",".join([
    "/opt/spark-jars/hadoop-aws-3.3.4.jar",
    "/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar",
])


def get_spark(app_name: str) -> SparkSession:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars", _JARS)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
