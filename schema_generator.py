#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

'''
@File      :   schema_generator.py
@Time      :   2024/09/04 09:53:43
@Author    :   CLF
@Version   :   1.0
@Contact   :   https://www.linkedin.com/in/clf3721
@License   :   MIT, 2024, CLF
@Desc      :   Generates a model schema for specified features in a DataFrame and saves it to a JSON file.

Required Packages:
pip install --upgrade pip setuptools json

Usage:
In working script, import the SchemaGenerator class.
Initiate with the feature matrix and filepath+filename as arguments. 
Call the instance to get the dictionary of DataFrames.

Example Usage:
from modules.schema_generator import SchemaGenerator

X = df.drop(columns='target')
filepath = 'path/to/schema.json'

schema_generator = SchemaGenerator(X, filepath)
schema_generator()

>> JSON schema has been saved to path/to/schema.json

'''

import json

class SchemaGenerator:
    def __init__(self, features, filepath='schema.json'):
        self.features = features.columns.to_list()
        self.filepath = filepath

    def __call__(self):
        # Create the schema
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "title": "Generated schema for Root",
            "type": "object",
            "properties": {},
            "required": self.features
        }

        # Add properties for each feature
        for feature in self.features:
            schema["properties"][feature] = {"type": "number"}

        # Convert the schema to JSON
        json_schema = json.dumps(schema, indent=4)

        # Save the JSON schema to a file
        with open(self.filepath, 'w') as file:
            file.write(json_schema)

        print(f'JSON schema has been saved to {self.filepath}')

