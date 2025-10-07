import os
import json
from pyspark.sql import SparkSession, functions as F


def main():
    spark = (
        SparkSession.builder.appName("CurateYahooFinance")
        .getOrCreate()
    )

    datasets_base = os.environ.get("DATASETS_BASE")
    if datasets_base:
        raw_path = f"{datasets_base}/raw/*.csv"
        curated_path = f"{datasets_base}/curated/prices"
        categories_path = os.environ.get("CATEGORIES_PATH", "/opt/config/categories.json")
    else:
        raw_path = os.path.join("data", "raw", "*.csv")
        curated_path = os.path.join("data", "exports", "curated", "prices")
        categories_path = os.environ.get("CATEGORIES_PATH", os.path.join("config", "categories.json"))

    df = spark.read.csv(raw_path, header=True, inferSchema=True)

    # Seleção de colunas e limpeza simples
    # Espera colunas: date, Open, High, Low, Close, Adj Close, Volume, symbol
    # Algumas instalações usam 'Adj Close' com espaço; normalizamos para 'AdjClose'
    for col_old, col_new in [("Adj Close", "AdjClose")]:
        if col_old in df.columns:
            df = df.withColumnRenamed(col_old, col_new)

    df = df.dropna(subset=["date", "Close", "Open", "High", "Low", "Volume", "symbol"])  # tipo básico

    df = (
        df.withColumn("date", F.to_date("date"))
          .withColumn("year", F.year("date"))
          .withColumn("month", F.month("date"))
          .withColumn("day", F.dayofmonth("date"))
          .dropDuplicates(["symbol", "date"])
    )

    # Enriquecimento com categorias (se necessário)
    if "category" in df.columns:
        df = df.fillna({"category": "uncategorized"})
    else:
        try:
            with open(categories_path, "r") as f:
                categories = json.load(f)
            rows = [{"symbol": s, "category": c} for c, syms in categories.items() for s in syms]
            cat_df = spark.createDataFrame(rows)
            df = df.join(cat_df, on="symbol", how="left")
            df = df.fillna({"category": "uncategorized"})
        except Exception:
            df = df.withColumn("category", F.lit("uncategorized"))

    # Particiona por símbolo para facilitar consultas
    (
        df.repartition("symbol", "category")
          .write.mode("overwrite")
          .parquet(curated_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()