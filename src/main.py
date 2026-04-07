from src.utils.spark_session import create_spark
from src.data.loader import load_data
from src.pipeline1.compatibility_graph import build_graph
from src.pipeline2.scoring import compute_scores
from src.pipeline2.scheduling import schedule_batches

def main():
    spark = create_spark()
    servers, windows, apps, cves = load_data(spark)

    servers.show()
    windows.show()
    apps.show()
    cves.show()
    

    # pipeline 1    
    #clusters = build_graph(spark, servers, windows, apps)

    # pipeline 2
    #batch_scores = compute_scores(clusters, cves)

    #schedule = schedule_batches(batch_scores)
    #schedule.show()

if __name__ == "__main__":
    main()
