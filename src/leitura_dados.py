from pyspark.sql import SparkSession

def iniciar_spark():
    return SparkSession.builder \
        .appName("Bolsa Familia 2025") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate() 

def carregar_dataframe(spark, caminho_csv="dados/NovoBolsaFamilia25.csv"):
    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .option("sep", ";") \
        .option("encoding", "ISO-8859-1") \
        .csv(caminho_csv) 
    return df