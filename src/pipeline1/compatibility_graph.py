from pyspark.sql.functions import (col, collect_set, coalesce, countDistinct, explode, floor,
                                       lit, max as spark_max, min as spark_min,
                                       unix_timestamp, when)
from pyspark.sql.types import DoubleType
from pyspark.sql import functions as F
import networkx as nx
from src.config import *


def _time_overlap(start_a, end_a, start_b, end_b):
    if start_a is None or end_a is None or start_b is None or end_b is None:
        return 0.0

    def intervals(start, end):
        if end >= start:
            return [(start, end)]
        return [(start, 86400), (0, end)]

    segs_a = intervals(start_a, end_a)
    segs_b = intervals(start_b, end_b)
    overlap = 0.0
    total_a = sum(e - s for s, e in segs_a)
    total_b = sum(e - s for s, e in segs_b)
    if total_a == 0 or total_b == 0:
        return 0.0

    for sa, ea in segs_a:
        for sb, eb in segs_b:
            overlap += max(0.0, min(ea, eb) - max(sa, sb))

    return float(overlap / max(total_a, total_b))


overlap_udf = F.udf(_time_overlap, DoubleType())


def build_graph(spark, servers, windows, apps):
    servers = servers.select("server_id", "environment").distinct()
    apps = apps.select("server_id", "application_id", "window_label", "sla_ratio").distinct()
    windows = windows.select("window_label", "frequency", "day_start", "day_end", "start_time", "end_time").distinct()

    app_capacity = apps.groupBy("application_id") \
        .agg(
            countDistinct("server_id").alias("NStot"),
            spark_max("sla_ratio").alias("sla_ratio")
        ) \
        .withColumn("NSmax_down", floor(col("NStot") * (lit(1.0) - col("sla_ratio"))))

    server_windows = apps.join(windows, "window_label", "left") \
        .select("server_id", "window_label", "start_time", "end_time") \
        .distinct()

    server_pairs = servers.alias("a").join(
        servers.alias("b"),
        col("a.server_id") < col("b.server_id")
    ).select(
        col("a.server_id").alias("src"),
        col("b.server_id").alias("dst"),
        when(col("a.environment") == col("b.environment"), lit(1.0)).otherwise(lit(0.0)).alias("C_env")
    )

    pair_windows = server_windows.alias("a").join(
        server_windows.alias("b"),
        col("a.server_id") < col("b.server_id")
    ).withColumn("start_a", unix_timestamp(col("a.start_time"), "HH:mm:ss")) \
     .withColumn("end_a", unix_timestamp(col("a.end_time"), "HH:mm:ss")) \
     .withColumn("start_b", unix_timestamp(col("b.start_time"), "HH:mm:ss")) \
     .withColumn("end_b", unix_timestamp(col("b.end_time"), "HH:mm:ss")) \
     .withColumn("C_time", overlap_udf(col("start_a"), col("end_a"), col("start_b"), col("end_b")))

    time_scores = pair_windows.groupBy(
        col("a.server_id").alias("src"),
        col("b.server_id").alias("dst")
    ).agg(spark_max("C_time").alias("C_time"))

    shared_apps = apps.alias("a").join(
        apps.alias("b"),
        (col("a.application_id") == col("b.application_id")) &
        (col("a.server_id") < col("b.server_id"))
    ).select(
        col("a.server_id").alias("src"),
        col("b.server_id").alias("dst"),
        col("a.application_id")
    ).join(app_capacity, "application_id", "left")

    shared_apps = shared_apps.withColumn(
        "VC_app",
        when(col("NSmax_down") == 0, lit(0.0))
        .otherwise(1.0 - (lit(2.0) / col("NSmax_down")))
    ).withColumn(
        "VC_app",
        when(col("VC_app") < 0.0, lit(0.0)).otherwise(col("VC_app"))
    )

    app_scores = shared_apps.groupBy("src", "dst").agg(spark_min("VC_app").alias("C_app"))

    edges = server_pairs.join(time_scores, ["src", "dst"], "left") \
        .join(app_scores, ["src", "dst"], "left") \
        .withColumn("C_time", coalesce(col("C_time"), lit(0.0))) \
        .withColumn("C_app", coalesce(col("C_app"), lit(1.0)))

    edges = edges.withColumn(
        "compatibility",
        ALPHA * col("C_time") +
        BETA * col("C_env") +
        GAMMA * col("C_app")
    )

    edges = edges.filter(col("compatibility") >= THRESHOLD)

    # Converti in NetworkX per trovare componenti connesse
    edges_pd = edges.select("src", "dst").toPandas()
    G = nx.Graph()
    G.add_edges_from(edges_pd.values.tolist())

    # Trova componenti connesse
    components = list(nx.connected_components(G))

    # Crea dataframe clusters
    clusters_data = []
    for i, component in enumerate(components):
        clusters_data.append((i, list(component)))

    clusters_df = spark.createDataFrame(clusters_data, ["component", "servers"])
    return clusters_df
