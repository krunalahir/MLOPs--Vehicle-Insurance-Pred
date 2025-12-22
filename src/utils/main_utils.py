import os
import sys

import numpy as np
import dill
import yaml
from pandas import DataFrame

from src.exception import MyException
from src.logger import logging

def read_yaml_file(file_path:str)->dict:
    try:
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)

    except Exception as e:
        raise MyException(e,sys) from e


def write_yaml_file(file_path:str, content:object ,replace:bool = False ) -> None:
    try:
        if replace:
            if os.path.exists(file_path):
                os.remove(file_path)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, 'w') as file:
                yaml.dump(content, file)
    except Exception as e:
        raise MyException(e,sys) from e


def load_object(file_path:str)->object:
    """
    Returns model/object from project directory
    file_path :str location of file to load
    return :Model/obj
    """
    try:
        with open(file_path, "rb") as file_obj:
            obj =dill.load(file_obj)
            return obj
    except Exception as e:
        raise MyException(e,sys) from e


def save_numpy_array_data(file_path:str,array: np.array) -> None:
     """
     Saves numpy array to file
     file_str:str location of file to save
     np.array:a numpy array
     """
     try:
         dir_path =os.path.dirname(file_path)
         os.makedirs(dir_path, exist_ok=True)
         with open(file_path, "wb") as file_obj:
             np.save(file_obj, array)

     except Exception as e:
         raise MyException(e,sys) from e


def load_numpy_array_data(file_path:str)->np.array:
    """
    load numpy array from file
    file_path:str location of file to load
    return:np.array data loaded
    """
    try:
        with open(file_path, "rb") as file_obj:
            return np.load(file_obj)
    except Exception as e:
        raise MyException(e,sys) from e


def save_object(file_path:str,obj:object) -> None:
    logging.info("Entered the save_object method of utils")

    try:
        os.makedirs(os.path.dirname(file_path),exist_ok=True)
        with open(file_path,"wb") as file_obj:
            dill.dump(obj,file_obj)

        logging.info("Exited the save_object method of utils")
    except Exception as e:
        raise MyException(e,sys) from e





