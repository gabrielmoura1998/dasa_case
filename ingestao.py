"""
O próprio Kaggle fornece uma biblioteca com a documentação completa para import direto.
"""

# Eu criei um token na minha conta Kaggle, esse token gerou um arquivo JSON 
# Nas instruções do Kaggle, recomenda-se mover esse token para o caminho "C:\Users\<USUARIO>\.kaggle\"
# Ou seja, você deve fazer isso manualmente movendo o arquivo e criando uma pasta no seu usuário dentro
# do seu PC local

from kaggle.api.kaggle_api_extended import KaggleApi
from sqlalchemy import create_engine, text
import pandas as pd
import os

### Função para tentar autenticar a API antes de realizar qualquer outra etapa.
### Caso dê erro, você precisa configurar corretamente a API do Kaggle em sua conta no site oficial.

### Obs: É bem simples, vai no seu perfil do Kaggle (clica na sua foto de perfil), vai em configurações (Settings)
### Vai ter um bloco escrito "API" em negrito, e lá terão algumas instruções em inglês, exatamente iguais a essa abaixo:

### API
### Using Kaggle's beta API, you can interact with Competitions and Datasets to download data, make submissions, and more via the command line. Read the docs

### Ensure kaggle.json is in the location ~/.kaggle/kaggle.json to use the API.

### Dismiss

### Após essas instruções, há um campo de "CREATE NEW TOKEN", clica nesse botão que irá baixar o JSON que você deve colocar na pasta...
### C:\Users\<USUARIO>\.kaggle

def autenticar():
    try:
        api = KaggleApi()
        api.authenticate()
        print("Deu bom! A API foi autenticada :) ")
        return api
    except Exception as e:
        print("Deu ruim. Você não conseguiu autenticar a API, veja as instruções no GitHub.")
        print(f"Erro -> {e}")
        exit(1)


"""
    Autoexplicativo, baixa o dataset utilizando o token da API
"""
def baixar_dataset(api, dataset, destino):
    os.makedirs(destino, exist_ok=True)

    # Aqui eu usei as instruções da própria biblioteca do Kaggle, colocando parâmetros como dataset, destino, descompactar e quiet
    api.dataset_download_files(
        dataset,
        path=destino,
        unzip=True,
        quiet=False
    )

"""
    Transforma o CSV em um DataFrames no Pandas
"""
def carregar_dataframe(destino, nome_arquivo):
    # Esse nome do arquivo é o mesmo nome do download, se mudar o código irá falhar
    arquivo_csv = os.path.join(destino, nome_arquivo)
    df = pd.read_csv(arquivo_csv)
    return df

def criar_tabela_postgres(dataframe, nometabela, nomeschema):
    df = dataframe
    nome_da_tabela = nometabela
    nome_do_schema = nomeschema

    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    dbname = os.getenv("DB_NAME")


    url = fr"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(url)

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHHEMA IF NOT EXISTS {nome_do_schema}"))

    try:
        dataframe.to_sql(
            name=nome_da_tabela,
            con=engine,
            schema=nome_do_schema,
            if_exists="replace",
            index=False
        )

        return print(f"Deu bom! Criou a tabela {nometabela} no schema {nomeschema} dentro do PostgreSQL :) ")
    
    except Exception as e:
        return print(f"Deu ruim :( -> Olha o erro: {e})")


def main():
    rh_dataset = 'pavansubhasht/ibm-hr-analytics-attrition-dataset'
    destino = 'dataset/rh'
    api = autenticar()

    baixar_dataset(api, rh_dataset, destino)

    # Esse nome abaixo é o nome do arquivo quando você baixa o CSV lá no site do Kaggle
    nome_arquivo = 'WA_Fn-UseC_-HR-Employee-Attrition.csv'  # Lembra de checar o nome

    df = carregar_dataframe(destino, nome_arquivo)

    criar_tabela_postgres(df, "hr_analytics_attrition_dataset", "bronze")

if __name__ == "__main__":
    main()