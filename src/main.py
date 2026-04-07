from src.utils.spark_session import create_spark
from src.data.loader import load_data
from src.pipeline1.compatibility_graph import build_graph


def main():
    spark = create_spark()
    servers, windows, apps, cves, scanned = load_data(spark)

    # print("=== SERVERS ===")
    # servers.show()
    # print("\n=== WINDOWS ===")
    # windows.show()
    # print("\n=== APPS SLA ===")
    # apps.show()
    # print("\n=== CVEs ===")
    # cves.show()
    # print("\n=== SCANNED ===")
    # scanned.show()
    
    # pipeline 1   
    clusters = build_graph(spark, servers, windows, apps)

    print("\n=== CLUSTERS ===")
    clusters.show(truncate=False)

    # pipeline 2
    # from src.pipeline2.scoring import compute_scores
    # batch_scores = compute_scores(clusters, cves)

    # from src.pipeline2.scheduling import schedule_batches
    # schedule = schedule_batches(batch_scores)
    # schedule.show()

if __name__ == "__main__":
    main()

