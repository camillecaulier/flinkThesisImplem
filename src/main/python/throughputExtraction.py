import pandas
import pandas as pd
import io
import uuid
import datetime
import pathlib
from fabric import Connection
import os
import ast
import re
from index import *

import numpy as np


pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Set display width to None for unlimited
pd.set_option('display.max_colwidth', None)  # Show full content of each column



def extract_last_job_entries(file_path):
    # Dictionary to store the last line for each job name
    last_job_entries = {}

    # Open and read the file line by line
    with open(file_path, 'r') as file:
        for line in file:
            # Skip lines that start with {'jobs'
            if line.startswith("{'jobs") or line.startswith("starting fake prometheus"):
                continue

            # Attempt to parse the line as a dictionary
            try:
                job_info = ast.literal_eval(line)
                # Check if the 'job name' key exists in the parsed dictionary
                if 'job name' in job_info:
                    job_name = job_info['job name']
                    # Update the last job entry for this job name
                    last_job_entries[job_name] = job_info
            except (SyntaxError, ValueError) as e:
                print(f"Error parsing line: {line}. Error: {e}")

    return last_job_entries



def extract_setup(line):
    pattern = r"Benchmarking operator:\s*(\w+)\s*sourceParallelism:\s*(\d+)\s*mainParallelism:\s*(\d+)\s*aggregatorParallelism:\s*(\d+)\s*with file:\s*([\w,\.E\-]+)"

    match = re.search(pattern, line)

    if match:
        operator = match.group(1)
        source_parallelism = int(match.group(2))
        main_parallelism = int(match.group(3))
        aggregator_parallelism = int(match.group(4))
        file_params = match.group(5).split(',')

        # Ensure the file_params list has the expected number of elements
        if len(file_params) == 5:
            distribution, num_elements, num_windows, keysize, skew = file_params
            skew = float(skew)
        else:
            raise ValueError("File parameters do not match expected format.")
        if skew == 1.0E-15:
            skew = 0.0
        # Create the dictionary with the extracted values
        # print(file_params)
        result = {
            'Operator': operator,
            'source_parallelism': source_parallelism,
            'main_parallelism': main_parallelism,
            'aggregator_parallelism': aggregator_parallelism,
            'distributionType': distribution,
            'eventPerWindow': int(num_elements),
            'amountOfWindows': int(num_windows),
            'keySize': int(keysize),
            'skew': float(skew)
        }

        df = pd.DataFrame.from_records([result])

        return df

    else:
        raise ValueError("Input line does not match expected format.")




def extract_operator_metrics(job_data, operator_name:str):
    # print(job_data)
    operator_data = job_data.get(operator_name, {})

    # Extract lists from dictionaries
    accumulate_list = list(operator_data.get('accumulate', {}).values())
    numBytesInTotal_list = list(operator_data.get('numBytesInTotal', {}).values())
    numBytesOutTotal_list = list(operator_data.get('numBytesOutTotal', {}).values())
    numRecordsOutTotal_list = list(operator_data.get('numRecordsOutTotal', {}).values())
    numRecordsInTotal_list = list(operator_data.get('numBytesInTotal', {}).values())

    return {
        'accumulate': accumulate_list,
        'numBytesInTotal': numBytesInTotal_list,
        'numBytesOutTotal': numBytesOutTotal_list,
        'numRecordsOutTotal': numRecordsOutTotal_list,
        'numRecordsInTotal': numRecordsInTotal_list
    }

# def combineTwo dictionaries(operator_data_1)

def extract_job_metrics_hybrid(job_data):
    # Extract metrics for each operator
    source_metrics = extract_operator_metrics(job_data, 'Source: source -> switchNodeEventBasic')
    partial_function_metrics_split = extract_operator_metrics(job_data, 'roundRobinOperator')
    hash_metrics = extract_operator_metrics(job_data, 'hashOperator')
    aggregator_metrics = extract_operator_metrics(job_data, 'reconciliationOperator')

    return pd.DataFrame({
        'SourceAccumulate': [source_metrics['accumulate']],
        'SourceNumBytesIn': [source_metrics['numBytesInTotal']],
        'SourceNumBytesOut': [source_metrics['numBytesOutTotal']],
        'SourceNumRecordsOut': [source_metrics['numRecordsOutTotal']],
        'SourceNumRecordsIn': [source_metrics['numRecordsInTotal']],
        'PartialFunctionAccumulate': [partial_function_metrics_split['accumulate'] + hash_metrics['accumulate']],
        'PartialFunctionNumBytesIn': [partial_function_metrics_split['numBytesInTotal']  + hash_metrics['numBytesInTotal']],
        'PartialFunctionNumBytesOut': [partial_function_metrics_split['numBytesOutTotal'] + hash_metrics['numBytesOutTotal']],
        'PartialFunctionNumRecordsOut': [partial_function_metrics_split['numRecordsOutTotal'] + hash_metrics['numRecordsOutTotal']],
        'PartialFunctionNumRecordsIn': [partial_function_metrics_split['numRecordsInTotal']+ hash_metrics['numRecordsInTotal']],
        'AggregatorAccumulate': [aggregator_metrics['accumulate']],
        'AggregatorNumBytesIn': [aggregator_metrics['numBytesInTotal']],
        'AggregatorNumBytesOut': [aggregator_metrics['numBytesOutTotal']],
        'AggregatorNumRecordsOut': [aggregator_metrics['numRecordsOutTotal']],
        'AggregatorNumRecordsIn': [aggregator_metrics['numRecordsInTotal']],

    })

def extract_job_metrics(job_data):
    # Extract metrics for each operator
    source_metrics = extract_operator_metrics(job_data, 'Source: source')
    partial_function_metrics = extract_operator_metrics(job_data, 'PartialFunctionOperator')
    aggregator_metrics = extract_operator_metrics(job_data, 'aggregator')

    return pd.DataFrame({
        'SourceAccumulate': [source_metrics['accumulate']],
        'SourceNumBytesIn': [source_metrics['numBytesInTotal']],
        'SourceNumBytesOut': [source_metrics['numBytesOutTotal']],
        'SourceNumRecordsOut': [source_metrics['numRecordsOutTotal']],
        'SourceNumRecordsIn': [source_metrics['numRecordsInTotal']],
        'PartialFunctionAccumulate': [partial_function_metrics['accumulate']],
        'PartialFunctionNumBytesIn': [partial_function_metrics['numBytesInTotal']],
        'PartialFunctionNumBytesOut': [partial_function_metrics['numBytesOutTotal']],
        'PartialFunctionNumRecordsOut': [partial_function_metrics['numRecordsOutTotal']],
        'PartialFunctionNumRecordsIn': [partial_function_metrics['numRecordsInTotal']],
        'AggregatorAccumulate': [aggregator_metrics['accumulate']],
        'AggregatorNumBytesIn': [aggregator_metrics['numBytesInTotal']],
        'AggregatorNumBytesOut': [aggregator_metrics['numBytesOutTotal']],
        'AggregatorNumRecordsOut': [aggregator_metrics['numRecordsOutTotal']],
        'AggregatorNumRecordsIn': [aggregator_metrics['numRecordsInTotal']],

    })

def has_empty_value(elems):
    if elems is np.nan:
        return False
    # if elems
    # print(elems)
    for elem in elems:
        if elem is None or elem == []:
            return True
    return False

def replace_with_nan(df):

    # Columns to process
    columns_to_check = [
        "SourceAccumulate",
        "SourceNumBytesOut",
        "SourceNumRecordsOut",
        "PartialFunctionAccumulate",
        "PartialFunctionNumBytesIn",
        "PartialFunctionNumBytesOut",
        "PartialFunctionNumRecordsOut",
        "PartialFunctionNumRecordsIn",
        "AggregatorNumBytesIn",
        "AggregatorNumRecordsIn"
    ]

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        # Check if the operator is "MeanHash" (special case)
        is_mean_hash = row.get("Operator") == "MeanHash"

        for column in columns_to_check:
            value = row.get(column)
            # print(value)

            # Replace with NaN if value is an empty list
            if value == []:
                df.at[index, column] = np.nan
            # Replace with NaN if the list is full of zeros and not MeanHash (where all zeros are ok)
            # elif isinstance(value, list) :
            #     if not (is_mean_hash and column in ["PartialFunctionNumBytesOut", "PartialFunctionNumRecordsOut"]):
            #         df.at[index, column] = np.nan
            elif has_empty_value(value):
                df.at[index, column] = np.nan

    return df


def extract_all_job_data(filename):
    extractedLastJobEntries = extract_last_job_entries(filename)
    # print(extractedLastJobEntries)
    # print(type(extractedLastJobEntries))
    df = pd.DataFrame()
    for job in extractedLastJobEntries.values():
        # print(type(job))
        # print(job['job name'])
        setup = extract_setup(job['job name'])
        operator_metrics = None
        if(setup['Operator'] == 'MeanHybrid').any():
            operator_metrics = extract_job_metrics_hybrid(job)
        else:
            # print(setup['operator'])
            operator_metrics = extract_job_metrics(job)
        dfLine = pd.concat([setup,operator_metrics], axis=1)

        # print(dfLine)
        df = pd.concat([df, dfLine], ignore_index=True)

    return df


import pandas as pd



def custom_best_row(group, columns_to_sum):
    """
    Determines the best row in the group based on the product of sums of list values in specified columns.

    Args:
    - group (pd.DataFrame): The group of rows with the same operator, keySize, and skew.
    - columns_to_sum (list): List of column names to evaluate for the maximum product of sums of list values.

    Returns:
    - pd.Series: The row considered the best based on the highest product of sums in specified columns.
    """
    # Calculate the sum of each list column in each row
    sum_values = group[columns_to_sum].map(lambda x: sum(x) if isinstance(x, list) or x is not np.NaN else np.NaN)

    # Calculate the product of the sums for each row
    sum_values['product_of_sums'] = sum_values.prod(axis=1)

    # Find the row index with the highest product of sums
    best_row_idx = sum_values['product_of_sums'].idxmax()
    best_row = group.loc[best_row_idx]

    return best_row

def select_best_rows(df, list_columns):
    """
    Selects the best rows from the DataFrame using a custom function.

    Args:
    - df (pd.DataFrame): The input DataFrame.
    - list_columns (list): List of columns with lists for evaluation.

    Returns:
    - pd.DataFrame: DataFrame with the best rows selected.
    """
    group_columns = [operatorString, keysizeString, skewString]

    best_rows_df = df.groupby(group_columns, group_keys=False).apply(lambda group: custom_best_row(group, list_columns))

    return best_rows_df.reset_index(drop=True)




cur_dir=os.getcwd() # save current directory to save the generated CSV files
# print(cur_dir)
n_window = 360
n_elements_per_window = 100000
# pathlib.Path(experimentDirectory).mkdir(exist_ok=True, parents=True)
n_experiments = 5
start_experirment = 0
source_parallelism = 6
main_parallelism = 6
aggregator_parallelism = 1
experimentName = "%s_w%s_s%s_p%s_a%s" % (n_elements_per_window, n_window,  source_parallelism, main_parallelism, aggregator_parallelism)
experimentDirectory = "%s/experiments/experiment_%s/" % (cur_dir,experimentName)
dataDirectory = "param_100000_1440.csv"
data = extract_all_job_data("%s%s/throughput.dat" % (experimentDirectory, 0))
data = replace_with_nan(data)


main_df = pd.DataFrame()
for i in range(n_experiments):
    experimentFile = f"{experimentDirectory}{i}/output.dat"
    throughputFile = f"{experimentDirectory}{i}/throughput.dat"

    throughputDF = extract_all_job_data(throughputFile)
    throughputDF = replace_with_nan(throughputDF)



    # print(throughputDF.columns.tolist())


    throughputDF['skew'] = throughputDF['skew'].astype(float)
    throughputDF['Operator'] = throughputDF['Operator'].astype(str)
    throughputDF[keysizeString] = throughputDF[keysizeString].astype(int)



    # Concatenate into the main DataFrame
    main_df = pd.concat([main_df, throughputDF], ignore_index=True)

print(select_best_rows(main_df,['SourceAccumulate']))
print(select_best_rows(main_df,['PartialFunctionNumRecordsIn', 'PartialFunctionNumRecordsOut']))
print(main_df[operatorString].drop_duplicates().to_list())
main_df.to_csv(f"{experimentDirectory}/test.csv", index=False)