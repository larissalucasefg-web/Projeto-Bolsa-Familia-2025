from leitura_dados import iniciar_spark, carregar_dataframe
from tratamento_dados import tratar_dados
from pyspark.sql.functions import sum, avg, max, min, round, desc, count, countDistinct
from IPython.display import display 

def executar_analises():
    spark = iniciar_spark()
    df_bruto = carregar_dataframe(spark)
    df = tratar_dados(df_bruto)

    # --- 3.1. RESULTADOS GERAIS E MÉDIAS ---
    print("\n" + "="*50)
    print(" 3.1. RESULTADOS GERAIS E MÉDIAS ")
    print("="*50)
    
    resumo = df.agg(
        round(sum("valor_parcela"), 2).alias("total_pago"),
        round(avg("valor_parcela"), 2).alias("media_geral"),
        round(max("valor_parcela"), 2).alias("valor_maximo"),
        round(min("valor_parcela"), 2).alias("valor_minimo")
    )

    resumo_row = resumo.collect()[0]
    faturamento_total = float(resumo_row['total_pago'])

    print(f" > FATURAMENTO TOTAL (INVESTIMENTO): R$ {faturamento_total:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
    display(resumo.toPandas()) 

    # --- 3.2. MÉDIA POR UF ---
    print("\n" + "-"*50)
    print(" 3.2. MÉDIA POR UF ")
    print("-"*50)
    media_uf = df.groupBy("uf") \
        .agg(round(avg("valor_parcela"), 2).alias("media_pago_uf")) \
        .orderBy(desc("media_pago_uf"))
    display(media_uf.toPandas())

    # --- 3.3 RANKING TOP 5 ESTADOS ---
    print("\n" + "-"*50)
    print(" 3.3 RANKING DOS 5 ESTADOS COM MAIORES PAGAMENTOS ")
    print("-"*50)
    top_5_estados = df.groupBy("uf") \
        .agg(round(sum("valor_parcela"), 2).alias("total_acumulado_uf")) \
        .orderBy(desc("total_acumulado_uf")) \
        .limit(5)
    display(top_5_estados.toPandas())

    # --- 3.4 RANKING DAS PESSOAS (TOP 10) ---
    print("\n" + "-"*50)
    print(" 3.4 TOP 10 BENEFICIÁRIOS POR VALOR ACUMULADO ")
    print("-"*50)
    ranking_cpf = df.groupBy("cpf_favorecido", "nome_favorecido") \
        .agg(
            round(sum("valor_parcela"), 2).alias("valor_total_pessoa"),
            count("valor_parcela").alias("quantidade_parcelas")
        ) \
        .orderBy(desc("valor_total_pessoa")) \
        .limit(10)
    display(ranking_cpf.toPandas())

    # --- 3.5 QUANTIDADE DE BENEFICIÁRIOS ---
    print("\n" + "-"*50)
    print(" 3.5 ANÁLISE DE ALCANCE (BENEFICIÁRIOS UNICOS) ")
    print("-"*50)
    
    total_cpfs = df.agg(countDistinct("cpf_favorecido").alias("total_beneficiarios_unicos"))
    display(total_cpfs.toPandas())

    beneficiarios_por_uf = df.groupBy("uf") \
        .agg(countDistinct("cpf_favorecido").alias("qtd_beneficiarios_uf")) \
        .orderBy(desc("qtd_beneficiarios_uf"))
    
    print("Quantidade de beneficiários por UF:")
    display(beneficiarios_por_uf.toPandas())

    # --- CÁLCULO DE AMPLITUDE ---
    print("\n" + "="*50)
    amplitude_26 = float(resumo_row['valor_maximo'] - resumo_row['valor_minimo'])
    print(f" ANÁLISE DE EQUIDADE 2026 ")
    print(f" Amplitude (Maior vs Menor): R$ {amplitude_26:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
    print("="*50 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    executar_analises()