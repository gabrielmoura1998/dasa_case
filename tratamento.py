import os
import pandas as pd
from sqlalchemy import create_engine

user     = os.getenv("DB_USER", "dasa_user")
password = os.getenv("DB_PASSWORD", "dasa_pwd")
host     = os.getenv("DB_HOST", "localhost")
port     = os.getenv("DB_PORT", "5432")
dbname   = os.getenv("DB_NAME", "dasa_db")

url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(url)

df_bronze = pd.read_sql(
    "SELECT * FROM bronze.hr_analytics_attrition_dataset",
    con=engine
)

# Aqui eu preferi usar o Pandas por um motivo muito obvio: praticidade. 
# No SQL eu teria que realizar diversas queries para trazer os mesmos resultados, e o pandas ja me retorna muita coisa util sobre o dataset...
# Entao, nada contra o SQL, mas quem nao ama o Pandas nessas horas?

print("Mostrando linhas e colunas:")
print(df_bronze.shape)
print("---------------------------------")

print("Mostrando indice das colunas:")
print(df_bronze.columns)
print("---------------------------------")

print("Mostrando tipagem dos dados em cada coluna:")
print(df_bronze.dtypes)
print("---------------------------------")

print("Um dos meus preferidos -> Mostrando o resumo geral do dataframe:")
print(df_bronze.info(verbose=True, memory_usage=True))
print("---------------------------------")

print("Mostrando estatisticas gerais do dataframe:")
print(df_bronze.describe(include="all"))
print("---------------------------------")

# Descobrimos que temos tipagem int64 e object (que pelo cenario, o object pode ser string ou boolean)
# Também não há valores nulos, todas as colunas e todas as linhas possuem valores bem definidos
# O método info já demonstra os valores não-nulos por coluna, ou seja, temos todas as linhas sem valores nulos.
# O método describe mostra diversas estatísticas (de tipo int\float, string ou boolean), não sendo necessário avaliar coluna a coluna.

# Estatísticas mostradas: 
##### Count -> Contagem de linhas
##### Unique -> Contagem de valores únicos
##### Freq -> Frequência, ou seja, valores mais frequentes (no caso mostra qual o valor mais frequente em cada coluna)
##### Mean -> Média (só calcula se for número int ou float)
##### Std -> Desvio-padrão da coluna
##### min -> Mínimo (valor mínimo)
##### 25% \ 50% \ 75% -> valor dos quartis, ou seja, 25% da coluna Age (idade) é representado pelo valor 30 = lê-se que 25% dos funcionários possuem 30 anos. Assim por diante...
##### max -> Máximo (valor máximo)

# Agora vamos criar um dataframe prévio para criarmos a camada silver (com um tratamento dos dados)

mapa = {
            "Age": "idade",
            "Attrition": "perda_de_hc",
            "BusinessTravel": "frequencia_viagens_trabalho",
            "DailyRate": "remuneracao_diaria",
            "Department": "departamento",
            "DistanceFromHome": "distancia_casa_trabalho",
            "Education": "escolaridade",
            "EducationField": "formacao",
            "EmployeeCount": "contagem_funcionario",
            "EmployeeNumber": "id_funcionario",
            "EnvironmentSatisfaction": "satisfacao_no_ambiente",
            "Gender": "genero",
            "HourlyRate": "remuneracao_por_hora",
            "JobInvolvement": "envolvimento_no_trabalho",
            "JobLevel": "nivel_hierarquico",
            "JobRole": "funcao",
            "JobSatisfaction": "satisfacao_no_cargo",
            "MaritalStatus": "estado_civil",
            "MonthlyIncome": "remuneracao_mensal_moedalocal",
            "MonthlyRate": "remuneracao_mensal_unidade",
            "NumCompaniesWorked": "num_empresas_trabalhadas",
            "Over18": "maior_de_idade",
            "OverTime": "pratica_horas_extras",
            "PercentSalaryHike": "percentual_aumento_salarial",
            "PerformanceRating": "desempenho",
            "RelationshipSatisfaction": "satisfacao_relacionamento",
            "StandardHours": "horas_trabalhadas_semanais",
            "StockOptionLevel": "nivel_de_acionista",
            "TotalWorkingYears": "anos_trabalhados",
            "TrainingTimesLastYear": "numero_de_treinamentos",
            "WorkLifeBalance": "equilibrio_vidapessoal_trabalho",
            "YearsAtCompany": "anos_na_empresa",
            "YearsInCurrentRole": "anos_na_funcaoatual",
            "YearsSinceLastPromotion": "anos_desde_ultima_promocao",
            "YearsWithCurrManager": "anos_sob_mesma_gerencia"
            }

df_silver = df_bronze.rename(columns=mapa)

print(df_silver.dtypes)