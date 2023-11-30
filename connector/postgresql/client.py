"""

"""
from urllib.parse import quote

import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine

from utils.log import get_logger

logger = get_logger(__name__)


class PostgresConnector:
    def __init__(self, conn_dict):
        logger.info("Initiating postgres database connection")
        self.connection = None
        try:
            self.host = conn_dict["host"]
            self.username = conn_dict["username"]
            self.password = conn_dict["password"]
            self.dbname = conn_dict["dbname"]
            self.port = conn_dict["port"]
            using = (
                f"dbname='{self.dbname}' user='{self.username}' host='{self.host}' "
                f"password='{self.password}' port='{self.port}'"
            )
            self._db_connection = psycopg2.connect(using)
            self._db_cur = self._db_connection.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            )
            self.url = f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}"
            engine_url = f"postgresql://{self.username}:%s@{self.host}:{self.port}/{self.dbname}" % quote(
                self.password
            )
            self.engine = create_engine(engine_url)

            self.driver = "org.postgresql.Driver"
            self.properties = {
                "user": self.username,
                "password": self.password,
                "driver": "org.postgresql.Driver",
            }
            logger.info(
                f"Connection established to postgres database using username {self.username}"
            )
        except psycopg2.Error as err:
            logger.error(
                "Database connection failed due to postgres state {}".format(
                    " ".join(err.args)
                )
            )

    def __exit__(self):
        logger.info("Closing postgres database connection")
        self._db_connection.close()
        return self._db_cur.close()

    def execute_query(self, query):
        logger.debug(f"Executing {query} on postgres")
        try:
            self._db_cur.execute(query)
            try:
                result = self._db_cur.fetchall()
                if result is not None:
                    return result

            except Exception:
                pass
            return None
        except Exception as error:
            logger.error(f'error executing query "{query}", error: {error}')
            raise
        finally:
            self._db_connection.commit()

    def execute_commit(self):
        try:
            self._db_connection.commit()
        except Exception as error:
            logger.error(f"error in db commit {error}")
            return True
        else:
            return False
    def write_dataframe(
        self,
        df,
        table_name,
        mode="append",
        custom_schema="",
        init_session="",
    ):
        """

        Args:
            df (DataFrame): Spark DataFrame
            table_name (str): Table which will be populated by DataFrame
            mode (str): Default is append, can be overwrite also
            custom_schema (str): Schema if applicable
            init_session (str): sql query to be executed on the DB before write

        Returns:
            None

        """
        try:
            df.write.format("jdbc").option("url", self.url).option(
                "user", self.username
            ).option("password", self.password).option("port", self.port).option(
                "driver", self.driver
            ).option(
                "isolationLevel", "NONE"
            ).option(
                "sessionInitStatement", init_session
            ).option(
                "dbtable", table_name
            ).option(
                "customSchema", custom_schema
            ).option(
                "numPartitions", 100
            ).mode(
                mode
            ).save()
        except Exception as error:
            logger.error(f"error executing query write_dataframe, error: {error}")
            raise

    def write_to_db(self, df, table_name):
        df.to_sql(table_name, self.engine, if_exists="replace", index=False)

    def get_df(self, query):
        import pandas as pd

        df = pd.read_sql_query(query, con=self.engine)
        return df
    def read_and_return_as_df(
        self,
        spark,
        query,
        custom_schema="",
        partition_column=None,
        lower_bound=1,
        upper_bound=1,
        num_partitions=100,
        fetch_size=1000,
    ):
        """
        Reads a query and return a DataFrame.
        https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html

        Args:
            spark (SparkSession): SparkSession
            query (str): Query String
            custom_schema (str): Custom Schema
            partition_column (str): Column on which partition is done for parallelism
            lower_bound (int): Min Id
            upper_bound (int): Max Id
            num_partitions (int): Number of partitions
            fetch_size (int): Number of rows fetched per round trip

        Returns:
            DataFrame: Spark DataFrame

        """
        spark_query = f"({query})  a"
        try:
            if partition_column is None:

                df = (
                    spark.read.format("jdbc")
                    .option("url", self.url)
                    .option("user", self.username)
                    .option("password", self.password)
                    .option("port", self.port)
                    .option("driver", self.driver)
                    .option("dbtable", spark_query)
                    .option("customSchema", custom_schema)
                    .load()
                )
            else:
                df = (
                    spark.read.format("jdbc")
                    .option("url", self.url)
                    .option("user", self.username)
                    .option("password", self.password)
                    .option("port", self.port)
                    .option("driver", self.driver)
                    .option("partitionColumn", partition_column)
                    .option("lowerBound", lower_bound)
                    .option("upperBound", upper_bound)
                    .option("numPartitions", num_partitions)
                    .option("fetchsize", fetch_size)
                    .option("dbtable", spark_query)
                    .load()
                )
            return df
        except Exception as error:
            logger.error(f'error executing query "{query}", error: {error}')
            raise
