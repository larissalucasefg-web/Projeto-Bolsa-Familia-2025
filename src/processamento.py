# ---------------------------------------------------------
# 📦 IMPORTS
# ---------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, regexp_replace, sum, avg, desc, round,
    substring, max, min, concat, lit,
    count, year, month, to_date
)

import matplotlib.pyplot as plt
import numpy as np
import builtins


# ---------------------------------------------------------
# ⚙️ CONFIGURAÇÃO DO SPARK
# ---------------------------------------------------------
spark = SparkSession.builder \
    .appName("Bolsa Familia 2025") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()


# ---------------------------------------------------------
# 📂 LEITURA DOS DADOS
# ---------------------------------------------------------
caminho_csv = "dados/NovoBolsaFamilia25.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .option("sep", ";") \
    .option("encoding", "ISO-8859-1") \
    .csv(caminho_csv)

print("\n--- DADOS ORIGINAIS ---")
df.show(5)


# ---------------------------------------------------------
# 🧹 PADRONIZAÇÃO DE COLUNAS
# ---------------------------------------------------------
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


# ---------------------------------------------------------
# 🔄 TRATAMENTO DOS DADOS
# ---------------------------------------------------------
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

print("\n--- DADOS TRATADOS ---")
df_tratado.show(5)
df_tratado.printSchema()


# ---------------------------------------------------------
# 📊 RESULTADOS GERAIS
# ---------------------------------------------------------
print("\n--- RESULTADOS GERAIS ---")
df_tratado.agg(
    round(sum("valor_parcela"), 2).alias("total_pago"),
    round(avg("valor_parcela"), 2).alias("media_pagamento"),
    round(max("valor_parcela"), 2).alias("valor_maximo"),
    round(min("valor_parcela"), 2).alias("valor_minimo")
).show()


# ---------------------------------------------------------
# 📊 CAPTURAR RESULTADOS EM VARIÁVEL
# ---------------------------------------------------------
resumo = df_tratado.agg(
    round(sum("valor_parcela"), 2).alias("total_pago"),
    round(avg("valor_parcela"), 2).alias("media_pagamento"),
    round(max("valor_parcela"), 2).alias("valor_maximo"),
    round(min("valor_parcela"), 2).alias("valor_minimo")
).toPandas()

resumo_row = resumo.iloc[0]


# ---------------------------------------------------------
# 📌 MÉTRICAS FINAIS
# ---------------------------------------------------------
amplitude_25 = float(resumo_row['valor_maximo'] - resumo_row['valor_minimo'])

print(f"\nANÁLISE DE EQUIDADE 2025:")
print(f"Amplitude (Maior vs Menor): R$ {amplitude_25:,.2f}".replace(',', '.'))


# ---------------------------------------------------------
# 🏆 RANKING FAVORECIDOS
# ---------------------------------------------------------
ranking_favorecido = df_tratado.groupBy("cpf_favorecido", "nome_favorecido") \
    .agg(
        sum("valor_parcela").alias("valor_total_acumulado"),
        count("valor_parcela").alias("quantidade_parcelas")
    ) \
    .orderBy(desc("valor_total_acumulado"))

print("Ranking dos 10 primeiros com valores acumulados")
ranking_favorecido.show(10, truncate=False)


# ---------------------------------------------------------
# 📈 MÉDIA GERAL
# ---------------------------------------------------------
media_geral = df_tratado.agg(avg("valor_parcela")).collect()[0][0]
print(f"Média geral: R$ {media_geral:.2f}")


# ---------------------------------------------------------
# 📊 GRÁFICO 1 - TOP 5 UFs
# ---------------------------------------------------------
df_uf = df_tratado.groupBy("uf") \
    .agg(sum("valor_parcela").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .limit(5) \
    .toPandas()

df_uf = df_uf.sort_values(by="total_pago", ascending=True)
colors = plt.cm.Blues(np.linspace(0.5, 0.9, len(df_uf)))

plt.figure(figsize=(10, 6))
bars = plt.barh(df_uf["uf"], df_uf["total_pago"], color=colors)

plt.title("Top 5 UFs - Total Pago Bolsa Família", fontsize=16, fontweight='bold')
plt.grid(axis='x', linestyle='--', alpha=0.4)

for bar in bars:
    largura = bar.get_width()
    plt.text(largura, bar.get_y() + bar.get_height()/2,
             f'R$ {largura:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."))

plt.tight_layout()
plt.show()


# ---------------------------------------------------------
# 📈 GRÁFICO 2 - EVOLUÇÃO
# ---------------------------------------------------------
df_tempo = df_tratado.withColumn("ano", year("data_competencia_data")) \
                     .withColumn("mes", month("data_competencia_data"))

df_evolucao = df_tempo.groupBy("ano", "mes") \
    .agg(sum("valor_parcela").alias("total_pago")) \
    .orderBy("ano", "mes") \
    .toPandas()

df_evolucao["data"] = df_evolucao["ano"].astype(str) + "-" + df_evolucao["mes"].astype(str).str.zfill(2)

plt.figure(figsize=(12, 6))
plt.plot(df_evolucao["data"], df_evolucao["total_pago"], marker='o')

plt.xticks(rotation=45)
plt.grid(True)

plt.tight_layout()
plt.show()


# ---------------------------------------------------------
# 📊 GRÁFICO 3 - SALTO ORÇAMENTÁRIO
# ---------------------------------------------------------
periodos = ['Jan/2021', 'Jan/2025']

valor_2021 = 2683780840.00 / 1e9
valor_2025 = float(resumo_row['total_pago']) / 1e9

faturamentos_bi = [valor_2021, valor_2025]
crescimento = ((valor_2025 - valor_2021) / valor_2021) * 100
ano_inicio = int(periodos[0].split('/')[1])
ano_fim = int(periodos[1].split('/')[1])
anos = ano_fim - ano_inicio
texto_crescimento = f"Aumento de +{crescimento:.1f}% em {anos} anos"

plt.figure(figsize=(10,5))
plt.plot(periodos, faturamentos_bi, marker='o', linewidth=3)
plt.fill_between(periodos, faturamentos_bi, alpha=0.2)

for i, v in enumerate(faturamentos_bi):
    plt.text(
        i,
        v + 0.3,
        f'R$ {v:.2f} Bi',
        ha='center',
        fontsize=11,
        fontweight='bold'
    )

max_valor = builtins.max(faturamentos_bi)

plt.text(
    0.5,
    max_valor * 1.2,
    texto_crescimento,
    ha='center',
    fontsize=14,
    fontweight='bold'
)

plt.annotate(
    '',
    xy=(1, valor_2025),
    xytext=(0, valor_2021),
    arrowprops=dict(arrowstyle='->', linewidth=2)
)

plt.title('Salto Orçamentário: Evolução do Valor Total Pago', fontsize=16, fontweight='bold')
plt.ylabel('Bilhões de Reais (R$)')
plt.grid(axis='y', linestyle='--', alpha=0.4)

for spine in ['top', 'right']:
    plt.gca().spines[spine].set_visible(False)

plt.ylim(0, max_valor * 1.3)
plt.tight_layout()
plt.savefig("salto_orcamentario.png", dpi=300, bbox_inches='tight')
plt.show()

# ---------------------------------------------------------
# 🔚 FINALIZAÇÃO
# ---------------------------------------------------------
spark.stop()