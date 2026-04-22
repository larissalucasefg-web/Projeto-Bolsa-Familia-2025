from pyspark.sql.functions import sum, avg, desc, round, max, min, count

def resultados_gerais(df):
    return df.agg(
        round(sum("valor_parcela"), 2).alias("total_pago"),
        round(avg("valor_parcela"), 2).alias("media_pagamento"),
        round(max("valor_parcela"), 2).alias("valor_maximo"),
        round(min("valor_parcela"), 2).alias("valor_minimo")
    )


def capturar_resumo(df):
    resumo = df.agg(
        round(sum("valor_parcela"), 2).alias("total_pago"),
        round(avg("valor_parcela"), 2).alias("media_pagamento"),
        round(max("valor_parcela"), 2).alias("valor_maximo"),
        round(min("valor_parcela"), 2).alias("valor_minimo")
    ).toPandas()

    return resumo.iloc[0]


def ranking_favorecidos(df):
    return df.groupBy("cpf_favorecido", "nome_favorecido") \
        .agg(
            sum("valor_parcela").alias("valor_total_acumulado"),
            count("valor_parcela").alias("quantidade_parcelas")
        ) \
        .orderBy(desc("valor_total_acumulado"))


def media_geral(df):
    return df.agg(avg("valor_parcela")).collect()[0][0]


def calcular_amplitude(resumo_row):
    return float(resumo_row['valor_maximo'] - resumo_row['valor_minimo'])