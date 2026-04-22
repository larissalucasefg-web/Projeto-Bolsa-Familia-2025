import matplotlib.pyplot as plt
import numpy as np
import builtins
from pyspark.sql.functions import sum, year, month

# ---------------------------------------------------------
# 1. GRÁFICO DE TOP 5 UFs
# ---------------------------------------------------------
def grafico_top_ufs(df):
    df_uf = df.groupBy("uf") \
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
                 f' R$ {largura:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),
                 va='center', fontsize=10, fontweight='bold')

    plt.tight_layout()
    plt.show()

# ---------------------------------------------------------
# 2. GRÁFICO DE EVOLUÇÃO MENSAL
# ---------------------------------------------------------
def grafico_evolucao(df):
    df_tempo = df.withColumn("ano", year("data_competencia_data")) \
                 .withColumn("mes", month("data_competencia_data"))

    df_evolucao = df_tempo.groupBy("ano", "mes") \
        .agg(sum("valor_parcela").alias("total_pago")) \
        .orderBy("ano", "mes") \
        .toPandas()

    df_evolucao["data"] = df_evolucao["ano"].astype(str) + "-" + df_evolucao["mes"].astype(str).str.zfill(2)

    plt.figure(figsize=(12, 6))
    plt.plot(df_evolucao["data"], df_evolucao["total_pago"], marker='o', linewidth=2)
    plt.title("Evolução Mensal do Faturamento", fontsize=14, fontweight='bold')
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.show()

# ---------------------------------------------------------
# 3. GRÁFICO DE SALTO ORÇAMENTÁRIO (FORMATO ORIGINAL)
# ---------------------------------------------------------
def grafico_salto(resumo_row):
    periodos = ['Jan/2021', 'Jan/2025']

    valor_2021 = 2683780840.00 / 1e9
    valor_2025 = float(resumo_row['total_pago']) / 1e9

    faturamentos_bi = [valor_2021, valor_2025]

    # Cálculos automáticos conforme sua lógica
    crescimento = ((valor_2025 - valor_2021) / valor_2021) * 100
    ano_inicio = int(periodos[0].split('/')[1])
    ano_fim = int(periodos[1].split('/')[1])
    anos = ano_fim - ano_inicio

    texto_crescimento = f"Aumento de +{crescimento:.1f}% em {anos} anos"

    plt.figure(figsize=(10, 5))

    # Linha e Área
    plt.plot(periodos, faturamentos_bi, marker='o', linewidth=3)
    plt.fill_between(periodos, faturamentos_bi, alpha=0.2)

    # Rótulos dos valores individuais
    for i, v in enumerate(faturamentos_bi):
        plt.text(
            i,
            v + 0.3,
            f'R$ {v:.2f} Bi',
            ha='center',
            fontsize=11,
            fontweight='bold'
        )

    # Texto principal automático (Posicionado acima)
    max_valor = builtins.max(faturamentos_bi)

    plt.text(
        0.5,
        max_valor * 1.2,
        texto_crescimento,
        ha='center',
        fontsize=14,
        fontweight='bold'
    )

    # Seta indicativa de crescimento
    plt.annotate(
        '',
        xy=(1, valor_2025),
        xytext=(0, valor_2021),
        arrowprops=dict(arrowstyle='->', linewidth=2)
    )

    # Título e Eixos
    plt.title('Salto Orçamentário: Evolução do Valor Total Pago', fontsize=16, fontweight='bold')
    plt.ylabel('Bilhões de Reais (R$)')

    # Grid personalizado
    plt.grid(axis='y', linestyle='--', alpha=0.4)

    # Limpeza visual (Spines)
    for spine in ['top', 'right']:
        plt.gca().spines[spine].set_visible(False)

    # Margem de segurança no topo para o texto não cortar
    plt.ylim(0, max_valor * 1.3)

    plt.tight_layout()
    
    # Exportação em alta qualidade
    plt.savefig("salto_orcamentario.png", dpi=300, bbox_inches='tight')
    plt.show()