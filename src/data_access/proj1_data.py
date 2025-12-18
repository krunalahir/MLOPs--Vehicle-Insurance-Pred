import sys
import pandas as pd
import numpy as np
from typing import Optional

from src.configuration.mongo_db_connection import MongoDBClient
from src.constants import DATABASE_NAME
from src.exception import MyException

class Proj1Data:
    """
    A class to export MongoDb records as a panda dataframe.
    """

    def __init__(self):
        """
        Initializes the Mongodb client connection.
        """
        try:
            self.client = MongoDBClient(DATABASE_NAME)
        except Exception as e:
            raise MyException(e,sys)

    def export_collection_as_dataframe(self,collection_name:str ,database_name:Optional[str] = None) -> pd.DataFrame:
        """
        Exports a collection as a pandas dataframe.
        Parameters:
        collection_name (str): The name of the MongoDb collection to export .
        database_name (str): The name of the MongoDb database to export .
        returns:
        pd.DataFrame: A pandas dataframe that contain collection data ,id column removed  and "na" values replaced with NaN
        """
        try:
            if database_name is None:
                collection =self.mongo_client.database[collection_name]
            else:
                collection= self.mongo_client[database_name][collection_name]

                # Convert collection data to DataFrame and preprocess
                print("Fetching data from mongoDB")
                df = pd.DataFrame(list(collection.find()))
                print(f"Data fecthed with len: {len(df)}")
                if "id" in df.columns.to_list():
                    df = df.drop(columns=["id"], axis=1)
                df.replace({"na": np.nan}, inplace=True)
                return df

        except Exception as e:
            raise MyException(e, sys)