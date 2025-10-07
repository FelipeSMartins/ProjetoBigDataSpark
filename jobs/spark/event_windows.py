import os
import json
import re
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F


def slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9_\-]", "-", name.lower().replace(" ", "-"))


def main():
    spark = SparkSession.builder.appName("EventWindows").getOrCreate()

    datasets_base = os.environ.get("DATASETS_BASE")
    if datasets_base:
        curated_path = f"{datasets_base}/curated/prices"
        out_base = f"{datasets_base}/curated/event_windows"
        events_path = os.environ.get("EVENTS_PATH", "/opt/config/events.json")
    else:
        curated_path = os.path.join("data", "exports", "curated", "prices")
        out_base = os.path.join("data", "exports", "event_windows")
        events_path = os.environ.get("EVENTS_PATH", os.path.join("config", "events.json"))

    df = spark.read.parquet(curated_path)
    df = df.withColumn("date", F.to_date("date"))

    with open(events_path, "r") as f:
        events = json.load(f)

    for ev in events:
        name = ev["name"]
        anchor_str = ev["anchor_date"]
        window_days = int(ev.get("window_days", 60))
        end_str = ev.get("end_date")  # opcional

        anchor = datetime.strptime(anchor_str, "%Y-%m-%d").date()

        pre_start = (anchor - timedelta(days=window_days)).isoformat()
        pre_end = (anchor - timedelta(days=1)).isoformat()
        post_start = (anchor + timedelta(days=1)).isoformat()
        post_end = (anchor + timedelta(days=window_days)).isoformat()

        during_start = anchor_str
        during_end = end_str if end_str else anchor_str

        slug = slugify(name)
        out_pre = f"{out_base}/{slug}/pre"
        out_during = f"{out_base}/{slug}/during"
        out_post = f"{out_base}/{slug}/post"

        df_pre = df.filter((F.col("date") >= F.lit(pre_start)) & (F.col("date") <= F.lit(pre_end)))
        df_during = df.filter((F.col("date") >= F.lit(during_start)) & (F.col("date") <= F.lit(during_end)))
        df_post = df.filter((F.col("date") >= F.lit(post_start)) & (F.col("date") <= F.lit(post_end)))

        df_pre.repartition("category", "symbol").write.mode("overwrite").parquet(out_pre)
        df_during.repartition("category", "symbol").write.mode("overwrite").parquet(out_during)
        df_post.repartition("category", "symbol").write.mode("overwrite").parquet(out_post)

    spark.stop()


if __name__ == "__main__":
    main()