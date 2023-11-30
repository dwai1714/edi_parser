# COPYRIGHT NOTICE

# Copyright © 2022 Breakwater Solutions, LLC. All rights reserved.
#
#CONFIDENTIAL AND PROPRIETARY
# This is unpublished proprietary source code for a program. The copyright notice
# above does not evidence any actual or intended publication of the source code,
# and the source code is not otherwise divested of its trade secrets, irrespective
# of what may have been deposited with the U.S. Copyright Office in connection
# with any registration relating to the program.

#

import numpy as np
import pandas as pd

from utils.log import get_logger

logger = get_logger(__name__)


class ExcelConnector:
    """
    Excel Connector to connect to Excel and return DataFrame
    """

    def __init__(self, file_name, sheet_name):
        self.file_name = file_name
        self.sheet_name = sheet_name

    def read_excel(self, spark, schema=None):
        """
        Read Excel and Return a DataFrame

        Args:
            spark (SparkSession): SparkSession
            schema (DataFrame Schema): Optional Schema else schema is derived

        Returns:
            DataFrame: Spark DataFrame

        """
        try:
            excel_data_df = pd.read_excel(
                self.file_name,
                sheet_name=self.sheet_name,
            )
            excel_data_df = excel_data_df.where(pd.notnull(excel_data_df), None)
            excel_data_df = excel_data_df.replace({np.nan: None})

            if schema is not None:
                df = spark.createDataFrame(excel_data_df, schema)
            else:
                df = spark.createDataFrame(excel_data_df)
            return df
        except Exception as error:
            logger.warning(
                f"Reading excel file {self.file_name} failed "
                f"with error: {error} "
                f"Proceeding with empty data frame",
                exc_info=error,
            )
            return spark.range(0).drop("id")  # Empty DF

    def write_excel(self, df):
        """
        Write Excel from Spark DataFrame

        Args:
            df (DataFrame): Spark DataFrame

        Returns:
            None

        """
        pandas_df = df.toPandas()
        pandas_df.to_excel(self.file_name)
