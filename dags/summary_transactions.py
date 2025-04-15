from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
import logging
from sqlalchemy.exc import SQLAlchemyError

def extract_data(ti):

    # Initialize the MySQL hook
    mysql_hook = MySqlHook(mysql_conn_id='db_master')

    # Get the connection
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    # Fetch all table names from the database
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    all_data = {}

    # Loop through each table and fetch its data
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT * FROM {table_name}")
        results = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]

        # Create a pandas dataframe for each table
        df = pd.DataFrame(results, columns=column_names)

        # 👇 DEBUG: Cek isi dataframe-nya
        logging.info(f"Data from table {table_name}:\n{df.head()}")

        all_data[table_name] = df.to_json()

    # Push all tables' data to XCom for the next task
    ti.xcom_push(key='all_tables_data', value=all_data)

    # Close the cursor and connection
    cursor.close()
    connection.close()


def transform_data(ti):
    all_tables_data = ti.xcom_pull(key='all_tables_data', task_ids='extract_task')

    # Load the extracted data into pandas DataFrames
    all_data = {table: pd.read_json(data) for table, data in all_tables_data.items()}

    # Ensure the required tables exist
    if 'orders' not in all_data or 'users' not in all_data:
        raise ValueError("Required tables 'orders' or 'users' are missing in the extracted data.")

    orders_df = all_data['orders']
    users_df = all_data['users']

    # Perform the join operation
    joined_df = orders_df.merge(users_df, left_on='user_id', right_on='id', how='inner')

    # Logging hasil join
    logging.info(f"Joined DataFrame preview:\n{joined_df.head()}")
    logging.info(f"Jumlah baris hasil join: {len(joined_df)}")
    logging.info(f"Kolom hasil join: {joined_df.columns.tolist()}")

    # Push the transformed data to XCom for the next task
    ti.xcom_push(key='transaction_groupby', value=joined_df.to_json())

def load_data(ti):
    import pandas as pd
    import logging
    from airflow.hooks.postgres_hook import PostgresHook
    from sqlalchemy.exc import SQLAlchemyError

    # Ambil data dari XCom
    transformed_data_json = ti.xcom_pull(key='transaction_groupby', task_ids='transform_task')
    transformed_data = pd.read_json(transformed_data_json)

    # Logging sebelum rename
    logging.info(f"Transformed Data before renaming:\n{transformed_data.head()}")

    # Rename kolom sesuai kebutuhan
    transformed_data.rename(columns={
        'id_x': 'user_id',
        'created_at_x': 'created_at',
        'updated_at_x': 'updated_at',
        'id_y': 'user_id',  # ini bakal overwrite id_x -> user_id, make sure datanya oke
    }, inplace=True)

    # Convert bigint ke datetime (milidetik -> datetime)
    for col in ['created_at', 'updated_at']:
        if col in transformed_data.columns:
            transformed_data[col] = pd.to_datetime(transformed_data[col], unit='ms')

    # Logging setelah rename
    logging.info(f"Transformed Data after renaming columns:\n{transformed_data.head()}")

    # Kolom yang bener-bener mau diinsert ke tabel
    important_columns = ['user_id', 'order_status', 'total_harga', 'created_at', 'updated_at', 'name', 'email']
    transformed_data = transformed_data[[col for col in important_columns if col in transformed_data.columns]]

    # Connect ke PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='db_warehouse')
    conn = postgres_hook.get_conn()

    try:
        cur = conn.cursor()

        # Bikin tabel kalau belum ada
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transaction_summary (
                id SERIAL PRIMARY KEY,
                user_id INT,
                order_status VARCHAR(50),
                total_harga FLOAT,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                name VARCHAR(100),
                email VARCHAR(100)
            );
        """)
        conn.commit()
        logging.info("Table `transaction_summary` created or already exists.")

        # Kosongin data lama (optional)
        cur.execute("DELETE FROM transaction_summary;")
        conn.commit()
        logging.info("Cleared existing data from `transaction_summary`.")

        # Insert data ke tabel pakai to_sql
        engine = postgres_hook.get_sqlalchemy_engine()
        transformed_data.to_sql('transaction_summary', con=engine, if_exists='append', index=False)
        logging.info(f"Inserted {len(transformed_data)} rows into `transaction_summary`.")

    except SQLAlchemyError as e:
        logging.error(f"Error during SQL operation: {e}")
        conn.rollback()

    except Exception as e:
        logging.error(f"Error: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()
        logging.info("PostgreSQL connection closed.")

default_args = {
    'owner' : 'Ahmad',
    'start_date': datetime(2023,1,1),
    'retries': 1
}

dag = DAG(
    dag_id = 'summary_transactions',
    default_args = default_args,
    description = "A simple dag to extract data from postgres",
    schedule_interval='0 2 * * *',
    max_active_runs = 1,
    catchup = False
)


extract_task = PythonOperator( 
    task_id = 'extract_task',
    python_callable = extract_data,
    dag=dag

)

transform_task = PythonOperator( 
    task_id = 'transform_task',
    python_callable = transform_data,
    dag=dag

)

load_task = PythonOperator( 
    task_id = 'load_task',
    python_callable = load_data,
    dag=dag

)

extract_task >> transform_task >> load_task