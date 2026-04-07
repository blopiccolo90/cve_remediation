from pyspark.sql import SparkSession

def create_spark():
    """
    Crea e configura una sessione Spark per la pipeline di remediation CVE.
    """
    return (SparkSession.builder
            .appName("RemediationPipeline")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate())

