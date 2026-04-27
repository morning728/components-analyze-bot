#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compact Spark report for Telegram: car market test dataset.
"""

import io
import locale
import os
import sys
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

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


def create_spark_session():
    return (
        SparkSession.builder.appName("CarMarketTelegramAnalysis")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def load_data(spark, path):
    schema = StructType(
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
    return spark.read.option("header", "true").schema(schema).csv(path)


def find_local_data_path():
    candidates = [
        LOCAL_FILE,
        "/data/{}".format(LOCAL_FILE),
        "/opt/spark/work-dir/{}".format(LOCAL_FILE),
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return "file://{}".format(candidate)
    raise FileNotFoundError("Не найден файл данных {}".format(LOCAL_FILE))


def get_vehicle_emoji(vehicle_type):
    return {
        "Sedan": "🚘",
        "SUV": "🚙",
        "Pickup": "🛻",
        "EV": "🔋",
    }.get(vehicle_type, "🚗")


def build_type_section(df_vehicle, vehicle_type):
    lines = ["{} *{}*".format(get_vehicle_emoji(vehicle_type), vehicle_type), ""]

    total_records = df_vehicle.count()
    stats = df_vehicle.agg(
        spark_round(avg("Price_USD"), 2).alias("avg_price"),
        spark_round(spark_min("Price_USD"), 2).alias("min_price"),
        spark_round(spark_max("Price_USD"), 2).alias("max_price"),
        spark_round(stddev("Price_USD"), 2).alias("std_price"),
        spark_round(avg("Mileage_KM"), 0).alias("avg_mileage"),
    ).collect()[0]

    lines.append("Записей: *{}*".format(total_records))
    lines.append("Средняя цена: *${}*".format(stats["avg_price"]))
    lines.append("Диапазон: *${} - ${}*".format(stats["min_price"], stats["max_price"]))
    lines.append("Средний пробег: *{:,} км*".format(int(stats["avg_mileage"] or 0)))
    lines.append("")

    lines.append("*Топ-3 бренда*")
    brand_rows = (
        df_vehicle.groupBy("Brand")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy(col("records").desc(), col("avg_price").desc())
        .limit(3)
        .collect()
    )
    for idx, row in enumerate(brand_rows, start=1):
        lines.append(
            "{}. *{}* - {} записей, средняя цена ${}".format(
                idx, row["Brand"], row["records"], row["avg_price"]
            )
        )
    lines.append("")

    lines.append("*По годам выпуска*")
    year_rows = (
        df_vehicle.groupBy("Year")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy("Year")
        .collect()
    )
    for row in year_rows:
        lines.append(
            "• {}: {} записей, средняя цена ${}".format(
                row["Year"], row["records"], row["avg_price"]
            )
        )
    lines.append("")
    return "\n".join(lines)


def build_report(df):
    lines = ["📊 *Анализ тестового датасета автомобилей*", ""]

    total_records = df.count()
    overview = df.agg(
        spark_round(avg("Price_USD"), 2).alias("avg_price"),
        spark_round(spark_min("Price_USD"), 2).alias("min_price"),
        spark_round(spark_max("Price_USD"), 2).alias("max_price"),
        spark_round(avg("Mileage_KM"), 0).alias("avg_mileage"),
    ).collect()[0]

    lines.append("*Общая сводка*")
    lines.append("Всего записей: *{}*".format(total_records))
    lines.append("Средняя цена по рынку: *${}*".format(overview["avg_price"]))
    lines.append("Минимальная цена: *${}*".format(overview["min_price"]))
    lines.append("Максимальная цена: *${}*".format(overview["max_price"]))
    lines.append("Средний пробег: *{:,} км*".format(int(overview["avg_mileage"] or 0)))
    lines.append("")

    lines.append("*Распределение по типам*")
    type_rows = (
        df.groupBy("Vehicle_Type")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy("Vehicle_Type")
        .collect()
    )
    for row in type_rows:
        lines.append(
            "• *{}*: {} записей, средняя цена ${}".format(
                row["Vehicle_Type"], row["records"], row["avg_price"]
            )
        )
    lines.append("")

    lines.append("*Топ-5 дилеров*")
    dealer_rows = (
        df.groupBy("Dealer")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy(col("records").desc(), col("avg_price").desc())
        .limit(5)
        .collect()
    )
    for idx, row in enumerate(dealer_rows, start=1):
        lines.append(
            "{}. *{}* - {} записей, средняя цена ${}".format(
                idx, row["Dealer"], row["records"], row["avg_price"]
            )
        )
    lines.append("")

    lines.append("*Региональная сводка*")
    region_rows = (
        df.groupBy("Region_Code")
        .agg(
            count("*").alias("records"),
            spark_round(avg("Price_USD"), 2).alias("avg_price"),
        )
        .orderBy(col("records").desc(), col("avg_price").desc())
        .collect()
    )
    for row in region_rows:
        lines.append(
            "• {}: {} записей, средняя цена ${}".format(
                row["Region_Code"], row["records"], row["avg_price"]
            )
        )
    lines.append("")

    for vehicle_type in [row["Vehicle_Type"] for row in type_rows]:
        lines.append(build_type_section(df.filter(col("Vehicle_Type") == vehicle_type), vehicle_type))
        lines.append("-" * 30)

    corr = df.stat.corr("Year", "Price_USD")
    if corr is not None:
        lines.append("")
        lines.append("*Корреляция год-цена*: `{:.4f}`".format(corr))

    lines.append("")
    lines.append("✅ *Анализ завершен*")
    return "\n".join(lines)


def main():
    import logging

    logging.getLogger("pyspark").setLevel(logging.ERROR)
    logging.getLogger("py4j").setLevel(logging.ERROR)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        try:
            df = load_data(spark, HDFS_PATH)
        except Exception:
            df = load_data(spark, find_local_data_path())

        df.cache()
        print(build_report(df))
    except Exception as exc:
        print("Ошибка при выполнении анализа: {}".format(exc))
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
