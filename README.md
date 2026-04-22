
<div align="center">
  <img src="https://i.imgur.com/c50SjWw.png" width="100px" alt="Logo ou Avatar" />
  <h1>Grupo 3</h1>
  <p>Bolsa Familia 2025</p>
</div>

# 📊 Análise de Dados - Bolsa Família 2025 com PySpark

## 📌 O que é o Programa Bolsa Família?
O **Bolsa Família** é o maior programa de transferência direta de renda do Brasil, focado em famílias em situação de vulnerabilidade. Em 2025, o programa consolidou um modelo de renda variável, onde o valor do benefício é ajustado conforme a composição familiar (nutrizes, gestantes, crianças e adolescentes), garantindo maior equidade e justiça social.

---

## 📝 Sobre o Projeto
Este projeto utiliza o ecossistema **Apache Spark** para processar e analisar grandes volumes de dados (*Big Data*) provenientes do Portal da Transparência.  

O foco principal é a **comparação entre os cenários de 2021 e 2025**, mensurando:
- O impacto do salto orçamentário  
- A eficiência da distribuição de renda nas diferentes regiões do país  

---

## 📁 Estrutura do Projeto


📦 BOLSA FAMILIA 2025\
├── 📂 dados\
│   ├── 📄 BolsaFamilia21.csv\
│   └── 📄 NovoBolsaFamilia25.csv\
├── 📂 notebooks\
│   ├── 📄 analise_exploratoria.ipynb\
│   ├── 🖼️ evolucao_pagamentos.png\
│   ├── 🖼️ salto_orcamentario.png\
│   └── 🖼️ top_ufs_bolsa_familia.png\
├── 📂 src\
│   ├── 📂 __pycache__\
│   ├── 📄 analises.py\
│   ├── 📄 graficos.py\
│   ├── 📄 leitura_dados.py\
│   ├── 📄 main.py\
│   ├── 📄 processamento.py\
│   └── 📄 tratamento_dados.py\
├── 📂 venv\
├── 📄 .gitignore\
├── 📄 README.md\
└── 📄 requirements.txt

---

## 🚀 Tecnologias Utilizadas
Python 3.x

PySpark (Spark SQL): Processamento distribuído em larga escala.

Matplotlib & NumPy: Criação de gráficos com anotações dinâmicas.

Pandas: Interface para renderização de plots estatísticos.

## 📋 Funcionalidades do Pipeline

### 1. ⚙️ Configuração Big Data
O ambiente é otimizado para lidar com arquivos massivos utilizando uma SparkSession configurada com 8GB de RAM e ajuste de partições de shuffle para máxima performance de CPU.

### 2. 🔄 Processo de ETL (Extract, Transform, Load)
Padronização: Renomeação de colunas complexas para o formato snake_case.

Casting: Conversão de valores monetários brasileiros (ponto e vírgula) para o tipo Decimal(10,2).

Enriquecimento: Extração de metadados temporais (Ano/Mês) para análises de tendência.

### 3. 📊 Insights Gerados
Ticket Médio: Evolução do valor por família (R$ 190,67 em 2021 vs. R$ 671,27 em 2025).

Salto Orçamentário: Identificação de um crescimento de +314% no investimento total.

Equidade Social: Análise da amplitude de pagamentos, demonstrando que o novo modelo atende melhor famílias numerosas.

### 📈 Exemplo de Resultado
O projeto gera automaticamente o gráfico de Salto Orçamentário, que compara visualmente o faturamento total entre os anos analisados, utilizando setas indicativas e rótulos de porcentagem calculados em tempo real pelo script.

(Nota: O gráfico acima é gerado automaticamente na pasta notebooks)

## 🎯 Finalidade
Este repositório foi desenvolvido para fins de estudo e aplicação prática em:

Engenharia de Dados com foco em Spark

Análise de Políticas Públicas e Impacto Social

## 👩‍💻 Autores
Projeto desenvolvido por:
Bruno, Leandro, Emmily, Larissa e Jhulyana

