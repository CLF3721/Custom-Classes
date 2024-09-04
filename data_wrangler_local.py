#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

'''
@File      :   data_wrangler_local.py
@Time      :   2024/07/30 11:27:08
@Author    :   CLF
@Version   :   1.0
@Contact   :   https://www.linkedin.com/in/clf3721
@License   :   MIT, 2024, CLF
@Desc      :   Wrangles data from csv files in a specified directory, standardizes column names, conducts basic cleaning, then stores each dataframe in a dictionary of DataFrames.

Required Packages:
pip install --upgrade pip setuptools pandas pathlib

Usage:
In a separate script, import the s3DataWrangler class.
Create an instance with the directory path as the argument. 
Then call the instance to get the dictionary of DataFrames.

Example Usage:
from modules.data_wrangler import LocalDataWrangler

directory = Path(r'path/to/data/directory')

wrangle = LocalDataWrangler(directory)
dict_of_dfs = wrangle()

#Print the code required to view the info and head of each DataFrame, copy then paste in editor, and run.
for x,y in wrangle.get_filenames():
    print(x + " = dict_of_dfs['" + y + "'].copy()")
    print(x + ".info(verbose=True, show_counts=True)")
    print(x + ".head()")
    print('\n')

'''


###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~>
###~> Import Required Packages
###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~>
import re
import pandas as pd
from pathlib import Path


###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~>
###~> LocalDataWrangler Class
###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~>
class LocalDataWrangler:
    def __init__(
        self, 
        directory: str,
        dtypes:dict = None,
        ) -> None:
        '''
        Initialize the LocalDataWrangler.
        :param directory: Path to the directory containing the files
        :param dtypes: (str, optional) Dictionary of column names to data types for read_csv function. Defaults to None.
        '''
        self.directory = Path(directory)
        self.dtypes = dtypes
    
    def __call__(self) -> dict:
        '''
        Reads all CSV files in the specified directory into DataFrames.
        :param dtype: (dict, optional) A dictionary of column names to read in as specific data types. Defaults to None.
        :return: dictionary where the keys are the CSV file names and the values are the corresponding DataFrames.
        '''
        dfs = {}
        for file_path in sorted(self.directory.glob('*.csv')):
            try:
                if file_path.endswith('.csv'):
                    if self.dtypes  is not None:
                        df_raw = pd.read_csv(
                            file_path, 
                            dtype=self.dtypes,
                            low_memory=False
                            )
                    else:
                        df_raw = pd.read_csv(
                            file_path,
                            low_memory=False
                            )
                elif file_path.endswith('.json'):
                    df_raw = pd.read_json(file_path)
                elif file_path.endswith('.parquet'):
                    df_raw = pd.read_parquet(file_path)
                else:
                    raise ValueError("Unsupported file type")
                
                df = self._clean_dataframe(df_raw)
                dfs[file_path.stem] = df
            except Exception as e:
                print(f"Error processing {file_path.name}: {str(e)}")
        return dfs
        
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        Performs basic cleaning operations on the input DataFrame.
        :param df: Input DataFrame
        :return: Cleaned DataFrame
        '''
        df = df.copy()
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.strip().str.lower()
        df.drop_duplicates(ignore_index=True, inplace=True)
        df.columns = self._clean_column_names(df.columns)
        return df
    
    @staticmethod
    def _clean_column_names(columns):
        '''
        Standardizes df column names into SCREAMING_SNAKE_CASE.
        :param columns: List of column names
        :return: List of cleaned column names
        '''
        columns = [' '.join(col.strip().split()) for col in columns]
        columns = [re.sub(r'\s*\([^)]*\)', '', col) for col in columns]
        columns = [col.upper().strip().replace(' ', '_') for col in columns]
        return columns
    
    def get_filenames(self) -> list:
        '''
        Produces a list of the filenames and stemmed filenames 
            to be used in main script.
        :return: Zipped List of filenames and df_names.
        '''
        filenames = [file.stem for file in sorted(self.directory.glob("*.csv"))]
        df_names = [(file.rsplit('.', 1)[1]).lower() for file in filenames]
        zipped_filenames = list(zip(df_names, filenames))
        return zipped_filenames

