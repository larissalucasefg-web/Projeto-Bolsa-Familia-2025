from pyspark.sql.functions import col, regexp_replace, substring, concat, lit, to_date

def tratar_dados(df):
    # Padronização de colunas
    colunas_padrao = {
        "MÊS COMPETÊNCIA": "data_competencia",
        "MÊS REFERÊNCIA": "data_referencia",
        "CÓDIGO MUNICÍPIO SIAFI": "codigo_municipio",
        "UF": "uf",
        "NOME MUNICÍPIO": "nome_municipio",
        "CPF FAVORECIDO": "cpf_favorecido",
        "NIS FAVORECIDO": "nis_favorecido",
        "NOME FAVORECIDO": "nome_favorecido",
        "VALOR PARCELA": "valor_parcela"
    } 

    for antiga, nova in colunas_padrao.items():
        df = df.withColumnRenamed(antiga, nova) 

    # Tratamento de tipos e limpeza
    df_tratado = (
        df
        .dropna()
        .withColumn(
            "valor_parcela",
            regexp_replace(col("valor_parcela"), ",", ".").cast("decimal(10,2)")
        )
        .withColumn(
            "data_competencia_data",
            to_date(
                concat(
                    substring(col("data_competencia"), 1, 4),
                    lit("-"),
                    substring(col("data_competencia"), 5, 2),
                    lit("-01")
                )
            )
        )
        .withColumn("ano_competencia", substring(col("data_competencia"), 1, 4))
        .withColumn("mes_competencia", substring(col("data_competencia"), 5, 2))
    ) 
    
    return df_tratado