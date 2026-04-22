from leitura_dados import iniciar_spark, ler_dados
from tratamento_dados import padronizar_colunas, tratar_dados
from analises import resultados_gerais, capturar_resumo, ranking_favorecidos, media_geral, calcular_amplitude
from graficos import grafico_top_ufs, grafico_evolucao, grafico_salto

spark = iniciar_spark()

df = ler_dados(spark, "dados/NovoBolsaFamilia25.csv")

df = padronizar_colunas(df)
df_tratado = tratar_dados(df)

# análises
resultados_gerais(df_tratado).show()

resumo_row = capturar_resumo(df_tratado)

amplitude = calcular_amplitude(resumo_row)
print(f"Amplitude: R$ {amplitude:,.2f}".replace(',', '.'))

ranking_favorecidos(df_tratado).show(10)

print(f"Média geral: R$ {media_geral(df_tratado):.2f}")

# gráficos
grafico_top_ufs(df_tratado)
grafico_evolucao(df_tratado)
grafico_salto(resumo_row)

spark.stop()