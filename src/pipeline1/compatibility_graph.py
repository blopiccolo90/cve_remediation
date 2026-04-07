from pyspark.sql.functions import *
from graphframes import GraphFrame
from src.config import *

def build_graph(spark, servers, windows, apps):

    df = servers.join(windows, "server_id")

    df = df.withColumn(
        "slot",
        explode(sequence(col("start_time"), col("end_time"), expr("interval 1 hour")))
    )

    # SLA capacity
    app_capacity = apps.groupBy("application_id") \
        .agg(countDistinct("server_id").alias("NStot")) \
        .join(sla, "application_id") \
        .withColumn(
            "NSmax_down",
            floor(col("NStot") * (1 - col("min_active_ratio")))
        )

    # pairs
    pairs = df.alias("a").join(
        df.alias("b"),
        (col("a.slot") == col("b.slot")) &
        (col("a.environment") == col("b.environment")) &
        (col("a.server_id") < col("b.server_id"))
    )

    pairs = pairs.withColumn("C_time", lit(1.0)) \
                 .withColumn("C_env", lit(1.0))

    # applicazioni
    apps_a = apps.withColumnRenamed("application_id", "app_a")
    apps_b = apps.withColumnRenamed("application_id", "app_b")

    pairs_apps = pairs \
        .join(apps_a, pairs["a.server_id"] == apps_a["server_id"]) \
        .join(apps_b, pairs["b.server_id"] == apps_b["server_id"])

    pairs_apps = pairs_apps.withColumn(
        "application_id",
        explode(array(col("app_a"), col("app_b")))
    )

    pairs_apps = pairs_apps.join(app_capacity, "application_id")

    pairs_apps = pairs_apps.withColumn(
        "NSpair",
        when(col("app_a") == col("app_b"), 2).otherwise(1)
    )

    pairs_apps = pairs_apps.withColumn(
        "VC_app",
        when(col("NSmax_down") == 0, 0.0)
        .otherwise(1 - (col("NSpair") / col("NSmax_down")))
    )

    pairs_apps = pairs_apps.withColumn(
        "VC_app",
        when(col("VC_app") < 0, 0.0).otherwise(col("VC_app"))
    )

    C_app = pairs_apps.groupBy(
        col("a.server_id").alias("src"),
        col("b.server_id").alias("dst")
    ).agg(min("VC_app").alias("C_app"))

    pairs_base = pairs.select(
        col("a.server_id").alias("src"),
        col("b.server_id").alias("dst"),
        "C_time",
        "C_env"
    ).distinct()

    edges = pairs_base.join(C_app, ["src", "dst"])

    edges = edges.withColumn(
        "compatibility",
        ALPHA * col("C_time") +
        BETA * col("C_env") +
        GAMMA * col("C_app")
    )


    # genero il grafo
    edges = edges.filter(col("compatibility") >= THRESHOLD)
    vertices = servers.select(col("server_id").alias("id"))
    g = GraphFrame(vertices, edges)
    components = g.connectedComponents()
    clusters = components.groupBy("component").agg(collect_set("id").alias("servers"))

    return clusters