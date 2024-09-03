import sqlite3
import os
import pandas as pd
from dotenv import load_dotenv
import assets.utils as utils
from assets.utils import logger
import datetime

load_dotenv()

def data_clean(df, metadados):
    '''
    Função principal para saneamento dos dados
    INPUT: Pandas DataFrame, dicionário de metadados
    OUTPUT: Pandas DataFrame, base tratada
    '''
    df["data_voo"] = pd.to_datetime(df[['year', 'month', 'day']]) 
    df = utils.null_exclude(df, metadados["cols_chaves"])
    df = utils.convert_data_type(df, metadados["tipos_originais"])
    df = utils.select_rename(df, metadados["cols_originais"], metadados["cols_renamed"])
    df = utils.string_std(df, metadados["std_str"])

    df.loc[:,"datetime_partida"] = df.loc[:,"datetime_partida"].str.replace('.0', '')
    df.loc[:,"datetime_chegada"] = df.loc[:,"datetime_chegada"].str.replace('.0', '')

    for col in metadados["corrige_hr"]:
        lst_col = df.loc[:,col].apply(lambda x: utils.corrige_hora(x))
        df[f'{col}_formatted'] = pd.to_datetime(df.loc[:,'data_voo'].astype(str) + " " + lst_col)
    
    logger.info(f'Saneamento concluído; {datetime.datetime.now()}')
    return df

def classifica_hora(hra):
    if 0 <= hra < 6: return "MADRUGADA"
    elif 6 <= hra < 12: return "MANHA"
    elif 12 <= hra < 18: return "TARDE"
    else: return "NOITE"

def feat_eng(df):
    """
    Função que cria novas colunas
    INPUT: Pandas DataFrame
    OUTPUT: Pandas DataFrame com novas colunas: tempo_voo_esperado, tempo_voo_hr, atraso, dia_semana, flag_atraso, flag_adiantado e horario (manhã/tarde/noite/madrugada).
    """
    # Criação de coluna 'tempo_voo_esperado'
    df["tempo_voo_esperado"] = (df["datetime_chegada_formatted"] - df["datetime_partida_formatted"]) / pd.Timedelta(hours=1)
    logger.info(f"'tempo_voo_esperado' criada em {datetime.datetime.now()}")

    # Criação de coluna 'tempo_voo_hr'
    df["tempo_voo_hr"] = df["tempo_voo"] / 60
    logger.info(f"'tempo_voo_hr' criada em {datetime.datetime.now()}")

    # Criação de coluna 'atraso'
    df["atraso"] = df["tempo_voo_hr"] - df["tempo_voo_esperado"]
    logger.info(f"'atraso' criada em {datetime.datetime.now()}")

    # Criação de coluna 'flag_atraso'
    df["flag_atraso"] = df["atraso"] > 0.6
    logger.info(f"'flag_atraso' criada em {datetime.datetime.now()}")

    # Criação de coluna 'flag_adiantado'
    df["flag_adiantado"] = df["atraso"] < -0.5
    logger.info(f"'flag_adiantado' criada em {datetime.datetime.now()}")

    # Criação de coluna 'dia_semana'
    df["dia_semana"] = df["data_voo"].dt.dayofweek  # 0=segunda
    logger.info(f"'dia_semana' criada em {datetime.datetime.now()}")

    # Criação de coluna 'horario'
    df["horario"] = df["datetime_partida_formatted"].dt.hour.apply(classifica_hora)
    logger.info(f"'horario' criada em {datetime.datetime.now()}")

    logger.info(f"Feature Engineering concluída em {datetime.datetime.now()}")
    return df


def save_data_sqlite(df):
    try:
        conn = sqlite3.connect("./NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    df.to_sql('nyflights', con=conn, if_exists='replace')
    conn.commit()
    logger.info(f'Dados salvos com sucesso; {datetime.datetime.now()}')
    conn.close()

def fetch_sqlite_data(table):
    try:
        conn = sqlite3.connect("./NyflightsDB.db")
        logger.info(f'Conexão com banco estabelecida ; {datetime.datetime.now()}')
    except:
        logger.error(f'Problema na conexão com banco; {datetime.datetime.now()}')
    c = conn.cursor()
    c.execute(f"SELECT * FROM {table} LIMIT 5")
    print(c.fetchall())
    conn.commit()
    conn.close()


if __name__ == "__main__":
    logger.info(f'Inicio da execução ; {datetime.datetime.now()}')
    metadados  = utils.read_metadado(os.getenv('META_PATH'))
    df = pd.read_csv(os.getenv('DATA_PATH'),index_col=0)
    df = data_clean(df, metadados)
    print(df.head())
    utils.null_check(df, metadados["null_tolerance"])
    utils.keys_check(df, metadados["cols_chaves"])
    df = feat_eng(df)
    #save_data_sqlite(df)
    fetch_sqlite_data(metadados["tabela"][0])
    logger.info(f'Fim da execução ; {datetime.datetime.now()}')