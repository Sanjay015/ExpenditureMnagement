"""
Module to create DB manager.
"""
import logging
import sqlalchemy
import pandas as pd
from constants.constants import DB_CREDENTIALS
from constants.query import CREATE_DATABASE_SCHEMA

logger = logging.getLogger(__name__)


class DBManager:
    """
    Context manager for DB operations.
    This will help to connect to db and execute the query.
    """

    def __init__(self, **kwargs):
        super(DBManager, self).__init__()
        self.kwargs = kwargs

    def sql_to_df(self, query, **kwargs):
        """Return query result to pandas DataFrame."""
        return pd.read_sql(query, self.conn, **kwargs)

    def df_to_sql(self, df, table, **kwargs):
        """Push data from a pandas DataFrame to SQL table."""
        # cols = {col: col.strip().lower() for col in df.columns}
        # df = df.rename(columns=cols)
        df.to_sql(table, self.conn, **kwargs)

    def execute(self, query, **kwargs):
        """Insert/Update/Delete/Execute Raw Query to database."""
        try:
            return self.conn.execute(query).execution_options(**kwargs)
        except Exception as ex:
            logger.error("Exception has occurred while executing query {}. {}".format(query, ex))

    def schema_exists(self, db_config):
        """Create database schema if not exists."""
        schema_conn = 'mysql+mysqldb://{user}:{password}@{host}:{port}'
        query = CREATE_DATABASE_SCHEMA.format(**db_config)
        try:
            schema_conn = sqlalchemy.create_engine(schema_conn.format(**db_config)).connect()
            inspect_dbs = sqlalchemy.inspect(schema_conn)
            if db_config['schema'].lower() not in [sch.lower() for sch in inspect_dbs.get_schema_names()]:
                schema_conn.execute(query)
        except Exception as ex:
            schema_conn = False
            logger.error("Exception has occurred fetching schema info {}.".format(ex))
        self.close(schema_conn)

    def close(self, conn):
        """Close DB connection."""
        try:
            conn.close()
        except Exception as ex:
            logger.warning("Exception has occurred while closing DB connection.{}".format(ex))

    def __enter__(self):
        """Initialize database connection."""
        db_config = self.kwargs or DB_CREDENTIALS
        if 'schema' not in db_config:
            db_config['schema'] = "freelance"
            logger.warning('Schema does not exists in configuration, selecting default schema `freelance`')
        # Create schema if not exists
        self.schema_exists(db_config)

        conn_str = 'mysql+mysqldb://{user}:{password}@{host}:{port}/{schema}'
        self.conn_str = conn_str.format(**db_config)
        try:
            self.engine = sqlalchemy.create_engine(self.conn_str)
            self.conn = self.engine.connect()
            logger.debug('Successfully connected to database')
        except Exception as ex:
            logger.debug('Could not connect to database. Please check you credentials. {}'.format(ex))
        return self

    def __exit__(self, errortype, value, traceback):
        """Close DB connection on exit."""
        self.close(self.conn)
