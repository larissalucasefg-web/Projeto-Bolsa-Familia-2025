import matplotlib.pyplot as plt
import numpy as np
import builtins
import plotly.express as px
from pyspark.sql.functions import sum, avg, max
from leitura_dados import iniciar_spark, carregar_dataframe
from tratamento_dados import tratar_dados

# --- FUNÇÕES DE APOIO ---
def formatar_real(valor, casas_decimais=2):
    """Formata valores para o padrão brasileiro R$ 1.234,56"""
    if casas_decimais == 0:
        return f'R$ {valor:,.0f}'.replace(",", "X").replace(".", ",").replace("X", ".")
    return f'R$ {valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")

def label_bars(rects, ax, suffix=""):
    """Adiciona rótulos de dados sobre as barras"""
    for rect in rects:
        height = rect.get_height()
        val_form = formatar_real(height)
        ax.annotate(f'{val_form}{suffix}',
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, 5), textcoords="offset points",
                    ha='center', va='bottom', fontsize=10, fontweight='bold')

def gerar_graficos():
    spark = iniciar_spark()
    df_bruto = carregar_dataframe(spark)
    df_tratado = tratar_dados(df_bruto)
    
    # Dados de 2026 para cálculos
    resumo_df = df_tratado.agg(
        sum("valor_parcela").alias("total_pago"),
        avg("valor_parcela").alias("media_pagamento"),
        max("valor_parcela").alias("valor_maximo")
    ).toPandas()
    resumo_row = resumo_df.iloc[0]

    # --- GRÁFICO 1: TOP 5 UFs ---
    df_uf = df_tratado.groupBy("uf") \
        .agg(sum("valor_parcela").alias("total_pago")) \
        .orderBy("total_pago", ascending=False) \
        .limit(5).toPandas()

    df_uf = df_uf.sort_values(by="total_pago", ascending=True)
    colors = plt.cm.Blues(np.linspace(0.5, 0.9, len(df_uf)))

    plt.figure(figsize=(10, 6))
    bars = plt.barh(df_uf["uf"], df_uf["total_pago"], color=colors)
    plt.title("Top 5 UFs - Total Pago Bolsa Família", fontsize=16, fontweight='bold')
    plt.grid(axis='x', linestyle='--', alpha=0.4)

    for spine in ["top", "right"]:
        plt.gca().spines[spine].set_visible(False)

    for bar in bars:
        largura = bar.get_width()
        plt.text(largura, bar.get_y() + bar.get_height()/2, f' {formatar_real(largura)}', va='center')

    plt.tight_layout()
    plt.show()

    # --- GRÁFICO 2: SALTO ORÇAMENTÁRIO ---
    valor_2021 = 2683780840.00 / 1e9
    valor_2025 = float(resumo_row['total_pago']) / 1e9
    faturamentos_bi = [valor_2021, valor_2025]
    crescimento = ((valor_2025 - valor_2021) / valor_2021) * 100

    plt.figure(figsize=(10,5))
    plt.plot(['Jan/2021', 'Jan/2025'], faturamentos_bi, marker='o', linewidth=3)
    plt.fill_between(['Jan/2021', 'Jan/2025'], faturamentos_bi, alpha=0.2)

    for i, v in enumerate(faturamentos_bi):
        plt.text(i, v + 0.3, f'R$ {v:.2f} Bi', ha='center', fontsize=11, fontweight='bold')

    plt.text(0.5, builtins.max(faturamentos_bi) * 1.2, f"Aumento de +{crescimento:.1f}% em 4 anos", 
             ha='center', fontsize=14, fontweight='bold')
    
    plt.title('Salto Orçamentário: Evolução do Valor Total Pago', fontsize=16, fontweight='bold')
    plt.grid(axis='y', linestyle='--', alpha=0.4)
    for spine in ['top', 'right']: plt.gca().spines[spine].set_visible(False)
    plt.ylim(0, builtins.max(faturamentos_bi) * 1.4)
    plt.tight_layout()
    plt.show()

    # --- GRÁFICO 3: COMPARATIVO REAL (ESCALA DUPLA) ---
    total_25_bi = float(resumo_row['total_pago']) / 1e9
    media_25, max_25 = float(resumo_row['media_pagamento']), float(resumo_row['valor_maximo'])
    total_21_bi, media_21, max_21 = 2683780840.00 / 1e9, 190.67, 1782.00

    labels = ['Total Pago', 'Média', 'Máximo']
    x = np.arange(len(labels))
    width = 0.35

    fig_comp, ax1 = plt.subplots(figsize=(12, 7), facecolor='white')
    ax2 = ax1.twinx()

    r1_t = ax1.bar(x[0] - width/2, total_21_bi, width, label='2021', color='#abd9e9')
    r2_t = ax1.bar(x[0] + width/2, total_25_bi, width, label='2025', color='#2c7bb6')
    r1_r = ax2.bar(x[1:] - width/2, [media_21, max_21], width, color='#abd9e9', alpha=0.6)
    r2_r = ax2.bar(x[1:] + width/2, [media_25, max_25], width, color='#2c7bb6')

    ax1.set_ylabel('Escala: Bilhões de R$', fontweight='bold', color='#2c7bb6')
    ax2.set_ylabel('Escala: Reais (Unitário)', fontweight='bold', color='#444444')
    ax1.set_title('Comparativo Real 2021 vs 2025\n(Escalas Ajustadas por Categoria)', fontsize=16, fontweight='bold', pad=20)
    ax1.set_xticks(x)
    ax1.set_xticklabels(['Total Pago\n(Bi R$)', 'Média por Família\n(R$)', 'Valor Máximo\n(R$)'])

    label_bars(r1_t, ax1, " Bi")
    label_bars(r2_t, ax1, " Bi")
    label_bars(r1_r, ax2)
    label_bars(r2_r, ax2)

    ax1.set_ylim(0, total_25_bi * 1.3)
    ax2.set_ylim(0, max_25 * 1.3)
    plt.tight_layout()
    plt.show()

    # --- GRÁFICO 4: DISTRIBUIÇÃO DA MÉDIA POR ESTADO (PLOTLY) ---
    df_media_uf = df_tratado.groupBy("uf") \
        .agg(avg("valor_parcela").alias("media_pago")) \
        .orderBy("media_pago", ascending=False).toPandas()

    df_media_uf["media_pago"] = df_media_uf["media_pago"].astype(float)

    fig_plotly = px.bar(
        df_media_uf,
        x='uf',
        y='media_pago',
        labels={'uf': 'Estado', 'media_pago': 'Valor Médio (R$)'},
        color='media_pago',
        color_continuous_scale='GnBu'
    )

    fig_plotly.update_traces(
        hovertemplate="<b>Estado:</b> %{x}<br><b>Média:</b> R$ %{y:.2f}<extra></extra>",
        marker_line_color='rgb(8,48,107)',
        marker_line_width=1.5,
        opacity=0.8
    )

    fig_plotly.update_layout(
        plot_bgcolor='#f8f9fa',
        paper_bgcolor='#f8f9fa',
        margin=dict(l=50, r=50, t=80, b=50),
        font=dict(family="Arial", size=12, color="#212529"),
        title={
            'text': '<b>DISTRIBUIÇÃO DA MÉDIA POR ESTADO | JANEIRO 2025</b>',
            'x': 0.5,
            'xanchor': 'center',
            'font': dict(size=20)
        },
        xaxis=dict(showgrid=False),
        yaxis=dict(gridcolor='#dee2e6'),
        coloraxis_showscale=False
    )

    fig_plotly.show()

    spark.stop()

if __name__ == "__main__":
    gerar_graficos()