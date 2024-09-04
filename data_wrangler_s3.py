#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

'''
@File      :   data_wrangler_s3.py
@Time      :   2024/08/22 15:28:19
@Author    :   CLF
@Version   :   1.0
@Contact   :   https://www.linkedin.com/in/clf3721
@License   :   MIT, 2024, CLF
@Desc      :   Wrangles data from csv files in a specified AWS s3 bucket, standardizes column names, conducts basic cleaning, then stores each dataframe in a dictionary of DataFrames.

https://saturncloud.io/blog/python-aws-boto3-how-to-read-files-from-s3-bucket/

Required Packages:
pip install --upgrade pip setuptools pandas boto3 python-dotenv

Usage:
In a separate script, import the s3DataWrangler class.
Create an instance with the directory path as the argument. 
Then call the instance to get the dictionary of DataFrames.

Example Usage:
from modules.data_wrangler_s3 import s3DataWrangler

bucket_name = 'dynamic-bidding'
prefix = 'data/clean/'

wrangle = s3DataWrangler(bucket_name, prefix)
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
# import os
import re
import boto3
import pandas as pd
from io import BytesIO


###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~>
###~> s3DataWrangler Class
###~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~>
class s3DataWrangler:
    def __init__(
        self, 
        aws_access_key_id:str, 
        aws_secret_access_key:str, 
        bucket_name:str, 
        prefix:str = None,
        dtypes:dict = None,
        ) -> None:
        
        '''
        Initialize the s3DataWrangler.
        :param bucket_name: Name of the s3 bucket
        :param aws_access_key_id: Boto3 client access key
        :param aws_secret_access_key: Boto3 client secret access key
        :param prefix: (str, optional) Prefix to the files in the bucket. Defaults to None.
        :param dtypes: (str, optional) Dictionary of column names to data types for read_csv function. Defaults to None.

        '''
        self.s3 = boto3.client(
            's3',
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
        )
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.dtypes = dtypes
        self.object_contents = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix).get('Contents', [])
        
        list_of_file_keys = []
        for obj in self.object_contents:
            list_of_file_keys.append(obj['Key'])
        self.file_keys = [e for e in list_of_file_keys if '.' in e]
    
    def __call__(self) -> dict:
        '''
        Reads all files in the specified s3 bucket into dataframes, 
            does basic data cleaning, then returns a dictionary of
            the DataFrames where the keys are the file_keys 
            and the values are the corresponding DataFrames.
        :return: Dictionary with filename stems as keys and DataFrames as values
        '''
        dfs = {}
        for file_key in sorted(self.file_keys):
            try:
                self.obj = self.s3.get_object(Bucket=self.bucket_name, Key=file_key)
                self.body = self.obj['Body'].read()
                if file_key.endswith('.csv'):
                    if self.dtypes is not None:
                        df_raw = pd.read_csv(
                            BytesIO(self.body),
                            dtype=self.dtypes,
                            low_memory=False
                            )
                    else:
                        df_raw = pd.read_csv(
                            BytesIO(self.body),
                            low_memory=False
                            )
                elif file_key.endswith('.json'):
                    df_raw = pd.read_json(BytesIO(self.body))
                elif file_key.endswith('.parquet'):
                    df_raw = pd.read_parquet(BytesIO(self.body))
                else:
                    raise ValueError("Unsupported file type")
                
                df = self._clean_dataframe(df_raw)
                dfs[file_key] = df
                print(file_key)
                
            except Exception as e:
                print(f'Error processing {file_key.name}: {str(e)}')
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
    def _clean_column_names(columns: list) -> list:
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
        Produces a list of the filenames and stemmed filenames to be used in main script.
        :return: Zipped List of filenames and df_names.
        '''
        filenames = self.file_keys
        df_names = [re.split(r"[./]+", file)[2] for file in filenames]
        zipped_filenames = list(zip(df_names, filenames))
        return zipped_filenames
