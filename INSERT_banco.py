import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import uuid
import hashlib

DB_NAME = "Case_LCGBR_Bank"
DB_USER = "postgres"
DB_PWD = "root"
DB_HOST = "::1"
DB_PORT = "5432"

table_columns = {
    'country': ['country_id', 'country'],
    'state': ['state_id', 'state', 'country_id'],
    'city': ['city_id', 'city', 'state_id'],
    
    'customers': ['customer_id', 'first_name', 'last_name', 'customer_city', 'country_name', 'cpf'],
    'accounts': ['account_id', 'customer_id', 'created_at', 'status', 'account_branch', 'account_check_digit', 'account_number'],
    
    'd_month': ['month_id', 'action_month'],
    'd_week': ['week_id', 'action_week'],
    'd_weekday': ['weekday_id', 'action_weekday'],
    'd_year': ['year_id', 'action_year'],
    'd_time': ['time_id', 'action_timestamp', 'week_id', 'month_id', 'year_id', 'weekday_id'],
    
    'transfer_ins': ['id', 'account_id', 'amount', 'transaction_requested_at', 'transaction_completed_at', 'status'],
    'transfer_outs': ['id', 'account_id', 'amount', 'transaction_requested_at', 'transaction_completed_at', 'status'],
    'pix_movements': ['id', 'account_id', 'in_or_out', 'pix_amount', 'pix_requested_at', 'pix_completed_at', 'status'],
    
}

date_columns = ['created_at', 'action_timestamp']

uuid_columns = ['state_id', 'customer_id', 'account_id', 'id', 'country_id']

trimm_columns = ['transaction_requested_at', 'transaction_completed_at', 'pix_requested_at', 'pix_completed_at']

def conectar_banco():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PWD,
            host=DB_HOST,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print('Erro na conexão com banco de dados: ', e)
        return None

def tratar_datas(df: pd.DataFrame, colunas_datas: list[str]) -> pd.DataFrame:
    for coluna in colunas_datas:
        if coluna in df.columns:
            df[coluna] = pd.to_datetime(df[coluna], errors='coerce').copy()
    return df

def generate_uuid(value):
    # Gera um novo UUID baseado no valor de entrada
    # Aqui, estamos gerando um UUID a partir de um hash MD5 do valor.
    # Isso não é um UUID puro, mas assegura que a mesma entrada sempre resultará no mesmo UUID.
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(value)))


def tratar_uuid(df: pd.DataFrame, uuid_columns: list[str]) -> pd.DataFrame:
    for column in uuid_columns:
            if column in df.columns:  # Verifica se a coluna existe
                df[column] = df[column].apply(generate_uuid)  # Substitui os valores na coluna com UUIDs
    return df

def tratar_colunas_inteiro(df: pd.DataFrame, trimm_columns: list) -> pd.DataFrame:
    for column in trimm_columns:
        if column in df.columns:
            df[column] = df[column].apply(lambda x: int(str(x).replace('.0', '')) if pd.notna(x) else x).astype('Int64')
    return df

def checa_duplicados(conn, table_name, row, columns) -> bool:
    cursor = conn.cursor()
    where_conditions = [
        sql.SQL("{} = %s").format(sql.Identifier(col)) for col in columns
    ]
    query = sql.SQL("SELECT COUNT(*) FROM {} WHERE {}").format(
        sql.Identifier(table_name),
        sql.SQL(" AND ").join(where_conditions)
    )
    
    #values = [row[col] for col in columns]
    values = []
    for col in columns:
        value = row[col]

        if isinstance(value, int) and col in ['account_branch', 'account_check_digit', 'account_number']:  # Substitua 'account_branch' pelos nomes apropriados
            value = str(value)
        values.append(value)

    cursor.execute(query, values)
    result = cursor.fetchone()[0] > 0
    cursor.close()

    return result


def persiste_banco(conn, table_name, df):
    cursor = conn.cursor()
    columns = table_columns[table_name]

    insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns))
    )

    for _, row in df.iterrows():
        #if not checa_duplicados(conn, table_name, row, columns):
        cursor.execute(insert_query, tuple(row[col] for col in columns))

    conn.commit()
    cursor.close()

def main():
    conn = conectar_banco()
    if conn is None:
        print("Conexão falhou.")
        return

    csv_dir = './csv'

    for table_name in table_columns.keys():

        file_path = os.path.join(csv_dir, f'{table_name}.csv')

        if os.path.isfile(file_path):
            columns = table_columns[table_name]
            df = pd.read_csv(file_path, usecols=columns)
            print(f"Persistindo dados para a tabela {table_name}...")

            if any(coluna in df.columns for coluna in date_columns):
                df = tratar_datas(df, date_columns)

            if any(coluna in df.columns for coluna in uuid_columns):
                df = tratar_uuid(df, uuid_columns)

            if any(coluna in df.columns for coluna in trimm_columns):
                df = tratar_colunas_inteiro(df, trimm_columns)

            persiste_banco(conn, table_name, df)
        else:
            print(f"Aviso: {table_name} não corresponde a nenhuma tabela mapeada.")

    conn.close()
    print("Processo de persistência finalizado com sucesso!")


if __name__ == "__main__":
    main()
