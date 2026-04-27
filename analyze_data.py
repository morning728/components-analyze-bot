#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Full Spark report for the car market dataset.
Loads the CSV into HDFS if it is missing there.
"""

import io
import locale
import os
import sys
import time

try:
    locale.setlocale(locale.LC_ALL, "en_US.UTF-8")
except Exception:
    try:
        locale.setlocale(locale.LC_ALL, "C.UTF-8")
    except Exception:
        pass

os.environ["PYTHONIOENCODING"] = "utf-8"

try:
    if hasattr(sys.stdout, "buffer") and sys.stdout.encoding != "utf-8":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "buffer") and sys.stderr.encoding != "utf-8":
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
except Exception:
    pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, max as spark_max, min as spark_min, round as spark_round, stddev
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


HDFS_PATH = "hdfs://namenode:9000/data/all_cars_prices.csv"
LOCAL_FILE = "all_cars_prices.csv"


def create_spark_session(app_name="CarMarketAnalysis", master="spark://spark-master:7077"):
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def get_schema():
    return StructType(
        [
            StructField("Vehicle_Type", StringType(), True),
            StructField("Brand", StringType(), True),
            StructField("Model", StringType(), True),
            StructField("Fuel_Type", StringType(), True),
            StructField("Transmission", StringType(), True),
            StructField("Drive_Type", StringType(), True),
            StructField("Year", IntegerType(), True),
            StructField("Month", IntegerType(), True),
            StructField("Day", IntegerType(), True),
            StructField("Week", IntegerType(), True),
            StructField("Dealer", StringType(), True),
            StructField("Region_Code", StringType(), True),
            StructField("Currency", StringType(), True),
            StructField("Price_USD", DoubleType(), True),
            StructField("Mileage_KM", IntegerType(), True),
        ]
    )


def check_hdfs_file_exists(hdfs_path):
    helper = create_spark_session(app_name="CheckHDFSFile", master="local[1]")
    try:
        sc = helper.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        conf.set("fs.defaultFS", "hdfs://namenode:9000")
        uri = sc._jvm.java.net.URI(hdfs_path)
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
        return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(hdfs_path))
    finally:
        helper.stop()


def upload_to_hdfs(local_path, hdfs_path):
    if not os.path.exists(local_path):
        print("Локальный файл не найден: {}".format(local_path))
        return False

    loader = create_spark_session(app_name="UploadCarsToHDFS", master="local[1]")
    try:
        sc = loader.sparkContext
        conf = sc._jsc.hadoopConfiguration()
        conf.set("fs.defaultFS", "hdfs://namenode:9000")
        uri = sc._jvm.java.net.URI("hdfs://namenode:9000")
        fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)

        hdfs_dir = os.path.dirname(hdfs_path)
        dir_path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_dir)
        if not fs.exists(dir_path):
            fs.mkdirs(dir_path)

        df = loader.read.option("header", "true").csv("file://{}".format(local_path))
        temp_dir = hdfs_path.replace(".csv", "_temp")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        temp_path = sc._jvm.org.apache.hadoop.fs.Path(temp_dir)
        final_path = sc._jvm.org.apache.hadoop.fs.Path(hdfs_path)
        part_file = None
        for status in fs.listStatus(temp_path):
            name = status.getPath().getName()
            if name.startswith("part-") and not name.endswith(".crc"):
                part_file = status.getPath()
                break

        if not part_file:
            print("Не найден part-файл в {}".format(temp_dir))
            return False

        if fs.exists(final_path):
            fs.delete(final_path, False)
        fs.rename(part_file, final_path)
        fs.delete(temp_path, True)
        print("Данные загружены в HDFS: {}".format(hdfs_path))
        return True
    finally:
        loader.stop()


def ensure_data_in_hdfs(local_path, hdfs_path):
    print("Проверка наличия данных в HDFS...")
    time.sleep(2)

    if check_hdfs_file_exists(hdfs_path):
        print("Данные уже есть в HDFS: {}".format(hdfs_path))
        return True

    for candidate in [
        local_path,
        "/data/{}".format(os.path.basename(local_path)),
        "/opt/spark/work-dir/{}".format(os.path.basename(local_path)),
        os.path.basename(local_path),
    ]:
        if os.path.exists(candidate):
            print("Найден локальный файл: {}".format(candidate))
            return upload_to_hdfs(candidate, hdfs_path)

    print("Локальный CSV не найден, загрузка в HDFS пропущена.")
    return False


def load_data(spark, path):
    return spark.read.option("header", "true").schema(get_schema()).csv(path)


def print_section_title(title, index=None):
    prefix = "{}. ".format(index) if index is not None else ""
    print("\n{}{}".format(prefix, title))
    print("-" * 80)


def print_metrics(df):
    print("=" * 80)
    print("АНАЛИЗ ТЕСТОВОГО ДАТАСЕТА АВТОМОБИЛЕЙ")
    print("=" * 80)

    print_section_title("Общая статистика", 1)
    total_records = df.count()
    stats = df.agg(
        spark_round(avg("Price_USD"), 2).alias("avg_price"),
        spark_round(spark_min("Price_USD"), 2).alias("min_price"),
        spark_round(spark_max("Price_USD"), 2).alias("max_price"),
        spark_round(stddev("Price_USD"), 2).alias("std_price"),
        spark_round(avg("Mileage_KM"), 0).alias("avg_mileage"),
    ).collect()[0]
    print("Всего записей: {}".format(total_records))
    print("Средняя цена: ${}".format(stats["avg_price"]))
    print("Минимальная цена: ${}".format(stats["min_price"]))
    print("Максимальная цена: ${}".format(stats["max_price"]))
    print("Стандартное отклонение: ${}".format(stats["std_price"]))
    print("Средний пробег: {:,} км".format(int(stats["avg_mileage"] or 0)))

    print_section_title("Статистика по типам автомобилей", 2)
    vehicle_stats = (
        df.groupBy("Vehicle_Type")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
            spark_round(spark_min("Price_USD"), 2).alias("min_price"),
            spark_round(spark_max("Price_USD"), 2).alias("max_price"),
        )
        .orderBy("Vehicle_Type")
    )
    print("Vehicle_Type | Records | Avg Price | Min Price | Max Price")
    print("-" * 80)
    for row in vehicle_stats.collect():
        print(
            "{} | {} | ${} | ${} | ${}".format(
                row["Vehicle_Type"],
                row["records"],
                row["avg_price"],
                row["min_price"],
                row["max_price"],
            )
        )

    print_section_title("Топ-10 брендов", 3)
    brand_stats = (
        df.groupBy("Brand")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy(col("records").desc(), col("avg_price").desc())
    )
    print("Brand | Records | Avg Price")
    print("-" * 80)
    for row in brand_stats.limit(10).collect():
        print("{} | {} | ${}".format(row["Brand"], row["records"], row["avg_price"]))

    print_section_title("Статистика по годам", 4)
    year_stats = (
        df.groupBy("Year")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy("Year")
    )
    print("Year | Records | Avg Price")
    print("-" * 80)
    for row in year_stats.collect():
        print("{} | {} | ${}".format(row["Year"], row["records"], row["avg_price"]))

    print_section_title("Топ-10 дилеров", 5)
    dealer_stats = (
        df.groupBy("Dealer")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy(col("records").desc(), col("avg_price").desc())
    )
    print("Dealer | Records | Avg Price")
    print("-" * 80)
    for row in dealer_stats.limit(10).collect():
        print("{} | {} | ${}".format(row["Dealer"], row["records"], row["avg_price"]))

    print_section_title("Топ-10 моделей", 6)
    model_stats = (
        df.groupBy("Model")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy(col("records").desc(), col("avg_price").desc())
    )
    print("Model | Records | Avg Price")
    print("-" * 80)
    for row in model_stats.limit(10).collect():
        print("{} | {} | ${}".format(row["Model"], row["records"], row["avg_price"]))

    print_section_title("Статистика по регионам", 7)
    region_stats = (
        df.groupBy("Region_Code")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy(col("records").desc(), col("avg_price").desc())
    )
    print("Region | Records | Avg Price")
    print("-" * 80)
    for row in region_stats.collect():
        print("{} | {} | ${}".format(row["Region_Code"], row["records"], row["avg_price"]))

    print_section_title("Динамика цен по месяцам за 2024 год", 8)
    monthly_stats = (
        df.filter(col("Year") == 2024)
        .groupBy("Month")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy("Month")
    )
    print("Month | Records | Avg Price")
    print("-" * 80)
    for row in monthly_stats.collect():
        print("{} | {} | ${}".format(row["Month"], row["records"], row["avg_price"]))

    print_section_title("Корреляция между годом и ценой", 9)
    correlation = df.stat.corr("Year", "Price_USD")
    print("Корреляция Year-Price_USD: {:.4f}".format(correlation))

    print("\n" + "=" * 80)
    print("АНАЛИЗ ЗАВЕРШЕН")
    print("=" * 80)


def main():
    print("=" * 80)
    print("ПОДГОТОВКА ДАННЫХ")
    print("=" * 80)

    if not ensure_data_in_hdfs(LOCAL_FILE, HDFS_PATH):
        print("Продолжаю с попыткой чтения данных напрямую из локального файла.")

    print("\n" + "=" * 80)
    print("СОЗДАНИЕ SPARK СЕССИИ")
    print("=" * 80)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        try:
            print("\nЗагрузка данных из HDFS...")
            df = load_data(spark, HDFS_PATH)
        except Exception:
            local_candidates = [
                LOCAL_FILE,
                f"/data/{LOCAL_FILE}",
                f"/opt/spark/work-dir/{LOCAL_FILE}",
            ]
            for path in local_candidates:
                if os.path.exists(path):
                    print("\nЗагрузка данных из локального файла: {}".format(path))
                    df = load_data(spark, "file://{}".format(path))
                    break
            else:
                raise FileNotFoundError("Локальный файл {} не найден".format(LOCAL_FILE))

        df.cache()
        print_metrics(df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
