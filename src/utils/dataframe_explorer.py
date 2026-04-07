from pyspark.sql import DataFrame

def explore_dataframe(df: DataFrame, name: str = "DataFrame"):
    """
    Esplora un dataframe Spark mostrando schema, count e campioni di dati.
    
    Args:
        df: Dataframe Spark da esplorare
        name: Nome descrittivo del dataframe
    """
    print(f"\n{'='*60}")
    print(f"  {name.upper()}")
    print(f"{'='*60}")
    
    print(f"\nRighe totali: {df.count()}")
    print(f"Colonne: {len(df.columns)}")
    
    print(f"\nSchema:")
    df.printSchema()
    
    print(f"\nPrimi 10 record:")
    df.show(10, truncate=False)
    
    print(f"\nDescrizione statistica:")
    df.describe().show(truncate=False)


def get_dataframes_stats(spark):
    """
    Carica tutti i dataframe e mostra le loro statistiche.
    """
    from src.data.loader import load_data
    
    servers, windows, apps, cves, scanned = load_data(spark)
    
    explore_dataframe(servers, "Servers")
    explore_dataframe(windows, "Windows")
    explore_dataframe(apps, "Apps SLA")
    explore_dataframe(cves, "CVEs")
    explore_dataframe(scanned, "Scanned")
    
    return servers, windows, apps, cves, scanned
