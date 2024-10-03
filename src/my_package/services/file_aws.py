"""
Services for reading and writing from and to AWS S3 of various file formats
"""

import os
from typing import Optional, List
import json
from io import StringIO
import pandas as pd
# import psycopg2
# import psycopg2.extras
from my_package.services.s3_client import S3Client
from my_package.services.base_file import BaseService


class CSVservice(BaseService, S3Client):
    def __init__(
        self,
        path: Optional[str] = "",
        delimiter: str = "\t",
        encoding: str = "UTF-8",
        schema_map: Optional[dict] = None,
        root_path: Optional[str] = "",
        verbose: bool = False,
    ):
        """Read/write service instance for CSV files
        Args:
            path (str, optional): Filename. Defaults to "".
            delimiter (str, optional): see pd.read_csv. Defaults to "\t".
            encoding (str, optional): see pd.read_csv. Defaults to "UTF-8".
            schema_map (Optional[dict], optional): mapping scheme for renaming of columns, see pandas rename. Defaults to None.
            root_path (str, optional): root path where file is located.
            verbose (bool, optional): should user information be displayed? Defaults to False.
        """
        BaseService.__init__(self, path, root_path)
        S3Client.__init__(self)
        self.path = os.path.join(root_path, path)
        self.delimiter = delimiter
        self.verbose = verbose
        self.encoding = encoding
        self.schema_map = schema_map

    def doRead(self, **kwargs) -> pd.DataFrame:
        """Read data from CSV
        Returns:
            pd.DataFrame: data converted to dataframe
        """
        s3obj = self.get_object(bucket=self.bucket_name, key=self.path)
        df = pd.read_csv(
            s3obj["Body"],
            index_col=False,
            encoding=self.encoding,
            delimiter=self.delimiter,
            **kwargs,
        )
        if self.verbose:
            print(f"CSV Service Read from File: {str(self.path)}")
        if self.schema_map:
            df.rename(columns=self.schema_map, inplace=True)
        return df

    def doWrite(self, X: pd.DataFrame, **kwargs):
        """Write df to CSV file in S3
        Args:
            df (pd.DataFrame): input data
        """
        csv_buffer = StringIO()
        X.to_csv(csv_buffer, encoding=self.encoding, sep=self.delimiter, **kwargs)
        self.put_object(
            bucket=self.bucket_name, key=self.path, obj=csv_buffer.getvalue()
        )
        if self.verbose:
            print(f"CSV Service Output to File: {self.path}")


class JSONservice(BaseService, S3Client):
    def __init__(
        self,
        path: Optional[str] = "",
        root_path: Optional[str] = "",
        verbose: bool = False,
    ):
        """Read/write service instance for JSON files
        Args:
            path (str, optional): Filename. Defaults to "".
            root_path (str, optional): root path where file is located.
            verbose (bool, optional): should user information be displayed? Defaults to False.
        """
        BaseService.__init__(self, path, root_path)
        S3Client.__init__(self)
        self.path = os.path.join(root_path, path)
        self.verbose = verbose

    def doRead(self, **kwargs) -> dict:
        """Read data from JSON
        Returns:
            dict: data converted to dict
        """
        s3obj = self.get_object(bucket=self.bucket_name, key=self.path)
        json_content = s3obj["Body"].read().decode("utf-8")
        data = json.loads(json_content)
        if self.verbose:
            print(f"JSON Service Read from File: {str(self.path)}")
        return data

    def doWrite(self, X: dict, **kwargs):
        """Write dict to JSON file in S3
        Args:
            X (dict): input data
        """
        json_content = json.dumps(X, **kwargs)
        self.put_object(bucket=self.bucket_name, key=self.path, obj=json_content)
        if self.verbose:
            print(f"JSON Service Output to File: {self.path}")


class TXTservice(BaseService, S3Client):
    """
    A class that provides services for reading and writing text files from AWS S3.

    Args:
        path (str, optional): The path of the file. Defaults to "".
        root_path (str, optional): The root path of the file. Defaults to "".
        verbose (bool, optional): Whether to enable verbose output. Defaults to False.
    """

    def __init__(
        self,
        path: Optional[str] = "",
        root_path: Optional[str] = "",
        verbose: bool = False,
    ):
        BaseService.__init__(self, path, root_path)
        S3Client.__init__(self)
        self.path = os.path.join(root_path, path)
        self.verbose = verbose

    def doRead(self, **kwargs) -> List:
        """
        Reads the content of the text file from AWS S3.

        Returns:
            List: The content of the text file as a list of lines.
        """
        try:
            s3obj = self.get_object(bucket=self.bucket_name, key=self.path)
            data = s3obj["Body"].read().decode("utf-8").splitlines()
            if self.verbose:
                print(f"TXT Service read from file: {str(self.path)}")
        except Exception as e:
            print(f"Error reading file: {str(e)}")
            data = []
        return data

    def doWrite(self, X: str, **kwargs):
        """
        Writes the content of a DataFrame to a text file in AWS S3.

        Args:
            X (str): The string of data to write to the text file.
            **kwargs: Additional keyword arguments.
        """
        try:
            # txt_content = "\n".join(X)
            self.put_object(bucket=self.bucket_name, key=self.path, obj=X)
            if self.verbose:
                print(f"TXT Service output to file: {self.path}")
        except Exception as e:
            print(f"Error writing file: {str(e)}")
            pass


# class PostgresService(BaseService):
#     def __init__(
#         self,
#         qry: Optional[str] = None,
#         output_tbl: str = "my_table",
#         verbose: bool = True,
#     ) -> None:
#         """
#         Initializes the PostgresService with a connection from AWS Secrets Manager.

#         Args:
#             qry (Optional[str]): SQL query string for reading from the database.
#             output_tbl (str): Default table name for writing data to the database.
#             verbose (bool): Toggle verbose output for debugging purposes.
#         """
#         self.qry = qry
#         self.output_tbl = output_tbl
#         self.verbose = verbose
#         self.conn = self.create_connection()

#     def __del__(self) -> None:
#         """Destructor to close the database connection when the object is destroyed."""
#         if self.conn:
#             self.conn.close()
#             if self.verbose:
#                 print("Database connection closed.")

#     @staticmethod
#     def getAWScredentials(secret_name: str, dbname: str) -> Dict[str, str]:
#         """
#         Fetch database credentials from AWS Secrets Manager.

#         Args:
#             secret_name (str): The secret name in AWS Secrets Manager.
#             dbname (str): The database name to connect to.

#         Returns:
#             Dict[str, str]: A dictionary containing database credentials.
#         """
#         client = boto3.client("secretsmanager")
#         get_secret_value_response = client.get_secret_value(SecretId=secret_name)
#         secret = json.loads(get_secret_value_response["SecretString"])
#         return {
#             "user": secret["username"],
#             "password": secret["password"],
#             "host": secret["host"],
#             "dbname": dbname,
#         }

#     def create_connection(self) -> psycopg2.extensions.connection:
#         """
#         Creates a PostgreSQL database connection using credentials from AWS Secrets Manager.

#         Returns:
#             psycopg2.extensions.connection: The connection object to the database.
#         """
#         try:
#             # Retrieve database details from input/output yaml:
#             secret_name, dbname, _ = config.io["output"]["pg"].values()
#             db_connection_details = self.getAWScredentials(
#                 secret_name=secret_name, dbname=dbname
#             )
#             conn = psycopg2.connect(**db_connection_details)
#             if self.verbose:
#                 print("Database connection successfully created.")
#             return conn
#         except Exception as e:
#             print(f"Failed to connect to database: {e}")
#             sys.exit(1)

#     def doRead(self) -> Optional[pd.DataFrame]:
#         """
#         Executes a read query stored in self.qry.

#         Returns:
#             Optional[pd.DataFrame]: The result of the query as a DataFrame, or None if an error occurs.
#         """
#         if not self.qry:
#             print("Error - no SQL query string provided")
#             return None
#         try:
#             with self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
#                 cursor.execute(self.qry)
#                 result = cursor.fetchall()
#                 return pd.DataFrame(
#                     result, columns=[desc[0] for desc in cursor.description]
#                 )
#         except Exception as e:
#             print(f"Query not successful: {e}")
#             return None

#     def doInsert(
#         self,
#         X: pd.DataFrame,
#         key_columns: Optional[List[str]] = None,
#         update_columns: Optional[List[str]] = None,
#     ) -> bool:
#         """
#         Inserts or upserts data from a DataFrame into the specified table. Uses all columns for
#         insert if key_columns and update_columns are not provided, and uses them to handle conflicts if provided.

#         Args:
#             X (pd.DataFrame): The DataFrame containing data to be inserted.
#             key_columns (Optional[List[str]]): Columns to use for detecting conflicts.
#             update_columns (Optional[List[str]]): Columns to update in case of conflict.

#         Returns:
#             bool: True if the insertion is successful, False otherwise.
#         """
#         if X.empty:
#             print("Empty DataFrame")
#             return False

#         df = X.replace({pd.NA: None})
#         tuples = [tuple(x) for x in df.to_numpy()]
#         cols = ",".join(list(df.columns))
#         placeholders = ",".join(["%s"] * len(df.columns))

#         if key_columns and update_columns:
#             # Building an ON CONFLICT clause for upsert
#             conflict_target = ", ".join(key_columns)
#             update_expr = ", ".join(f"{col}=EXCLUDED.{col}" for col in update_columns)
#             query = f"INSERT INTO {self.output_tbl} ({cols}) VALUES ({placeholders}) ON CONFLICT ({conflict_target}) DO UPDATE SET {update_expr};"
#         else:
#             # Regular insert with ON CONFLICT DO NOTHING
#             query = f"INSERT INTO {self.output_tbl} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"

#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.executemany(query, tuples)
#                 self.conn.commit()
#                 if self.verbose:
#                     print(f"Rows successfully inserted into table {self.output_tbl}.")
#             return True
#         except Exception as e:
#             print(f"Could not insert into database: {e}")
#             self.conn.rollback()
#             return False

#     def doUpdate(
#         self, X: pd.DataFrame, update_columns: List[str], key_columns: List[str]
#     ) -> bool:
#         """
#         Updates specified columns in a database table based on the contents of a DataFrame.

#         Args:
#             X (pd.DataFrame): DataFrame containing at least the key columns and the update columns.
#             update_columns (List[str]): List of column names in df that should be updated.
#             key_columns (List[str]): List of column names in df that should be used to identify the rows to update.

#         Returns:
#             bool: True if the update is successful, False otherwise.
#         """
#         if X.empty:
#             print("Empty DataFrame")
#             return False

#         set_clause = ", ".join([f"{col} = %s" for col in update_columns])
#         where_clause = " AND ".join([f"{col} = %s" for col in key_columns])

#         # Create a list of tuples for the executemany() function
#         values = [
#             tuple(row[col] for col in update_columns + key_columns)
#             for index, row in X.iterrows()
#         ]

#         query = f"UPDATE {self.output_tbl} SET {set_clause} WHERE {where_clause};"

#         try:
#             with self.conn.cursor() as cursor:
#                 cursor.executemany(query, values)
#                 self.conn.commit()
#                 if self.verbose:
#                     print(f"Rows successfully updated in table {self.output_tbl}.")
#             return True
#         except Exception as e:
#             print(f"Could not update database: {e}")
#             self.conn.rollback()
#             return False

#     def doWrite(
#         self,
#         X: pd.DataFrame,
#         type: str = "insert",
#         update_columns: Optional[List[str]] = None,
#         key_columns: Optional[List[str]] = None,
#     ) -> bool:
#         """
#         Writes or updates a DataFrame to the specified table in the database.

#         Args:
#             X (pd.DataFrame): DataFrame to write to the database.
#             type (str): Specifies the operation type ('insert' or 'update').
#             update_columns (Optional[List[str]]): Columns to update if type is 'update'.
#             key_columns (Optional[List[str]]): Key columns to identify rows if type is 'update'.

#         Returns:
#             bool: True if the operation is successful, False otherwise.
#         """
#         if type == "insert":
#             return self.doInsert(X)
#         elif type == "update":
#             if update_columns is None or key_columns is None:
#                 raise ValueError(
#                     "update_columns and key_columns must be provided for update operations."
#                 )
#             return self.doUpdate(X, update_columns, key_columns)
#         else:
#             raise ValueError("Invalid type provided. Supported types: insert, update.")
