import mysql.connector
import pandas as pd
from pandas import (
    DataFrame as PandasDataFrame,
    read_sql,
)
from sqlalchemy import create_engine, text
from dagster import ConfigurableIOManager, OutputContext, InputContext
from contextlib import contextmanager
import pyodbc
from typing import Optional, Sequence
import mysql.connector


@contextmanager
def connect_sql(config):
    user = config['user']
    password = config['password']
    database = config['database']
    port = config['port']
    server = config['server']

    connection_string = f'mysql+mysqlconnector://{user}:{password}@{server}:{port}/{database}'
    try:
        conn = create_engine(connection_string).connect()
        yield conn
    finally:
        if conn:
            conn.close()


class SQLIOManager(ConfigurableIOManager):
    user: str
    password: str
    database: str
    port: str
    server: str

    @property
    def _config(self):
        return self.dict()

    def handle_output(self, context: OutputContext, obj: PandasDataFrame):
        schema, table, cleanup = context.asset_key.path[0], context.asset_key.path[1], context.asset_key.path[2]

        if cleanup == 'cleanup':
            with connect_sql(config=self._config) as con:
                try:
                    context.log.info('Query to run: ' + self._get_cleanup_statement(table, schema))
                    result = con.execute(text(self._get_cleanup_statement(table, schema)))
                    context.log.info('Number of rows deleted: ' + str(result.rowcount))
                except:
                    context.log.info('Table does not exist!')

        if isinstance(obj, pd.DataFrame):
            with connect_sql(config=self._config) as con:
                obj.to_sql(table, con=con, if_exists='append', schema=schema, index=False, chunksize=10000)

    def _get_cleanup_statement(self, table: str, schema: str):
        return f"truncate {schema}.{table}"

    def load_input(self, context: InputContext) -> PandasDataFrame:
        schema, table, query = context.asset_key.path[0], context.asset_key.path[-3], context.asset_key.path[-2]

        with connect_sql(config=self._config) as con:
            result = read_sql(
                sql=self._get_select_statement(
                    table,
                    schema,
                    (context.metadata or {}).get('columns'),
                ),
                con=con
            )
        result.columns = map(str.lower, result.columns)
        return result

    def _get_select_statement(self,
                              table: str,
                              schema: str,
                              columns: Optional[Sequence[str]]
                              ):
        col_list = ', '.join(columns) if columns else '*'
        return f'SELECT {col_list} FROM {schema}.{table}'

class MySQLDirectConnection:
    def __init__(self, port, database, user, password, server):
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.server = server

        self.connection_string = f'mysql+mysqlconnector://{user}:{password}@{server}:{port}/{database}'
        self.engine = create_engine(self.connection_string)
        self.conn = self.engine.connect()

        self.cursor_conn = mysql.connector.connect(
            user=user, password=password, host=server, database=database
        )

        self.cursor = self.cursor_conn.cursor()

    def run_query(self, query):
        try:
            print('Query to run: ' + query)
            df = pd.read_sql(query, self.conn)
            self.conn.close()
        except pyodbc.ProgrammingError as error:
            print(f'Warning: \n {error}')
        return df

    def run_query_no_output(self, query):
        try:
            print('Query to run: ' + query)
            self.cursor.execute(query)
            self.cursor.close()
            self.cursor_conn.close()
        except pyodbc.ProgrammingError as error:
            print(f'Warning: \n {error}')
        return None
