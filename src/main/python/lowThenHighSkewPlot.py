import ast
import pandas as pd
import matplotlib.pyplot as plt

import ast

def extract_num_records_out(line):
    """
    Extracts the numRecordsOutTotal data from the PartialFunctionOperator section in the given line.

    Parameters:
        line (str): A string representation of the line containing the data.

    Returns:
        dict: A dictionary with subtask indices as keys and numRecordsOutTotal as values.
    """
    try:
        # Convert the string representation of the dictionary to an actual dictionary
        data = ast.literal_eval(line)
        # Access PartialFunctionOperator data
        partial_function_data = data.get('PartialFunctionOperator', {})
        # Extract numRecordsOutTotal
        num_records_out = partial_function_data.get('numRecordsInTotal', {})
        return num_records_out
    except (ValueError, SyntaxError):
        # Return an empty dictionary if parsing fails
        return {}
def process_throughput_data(file_path):
    # Read the file contents
    with open(file_path, 'r') as file:
        file_contents = file.readlines()

    # Filter out lines that don't contain dictionaries
    filtered_lines = [line for line in file_contents if line.strip().startswith("{")]

    # Parse the lines and extract the necessary data
    records_out_list = []

    for line in filtered_lines:
        try:
            extracted_data = extract_num_records_out(line)
            if(extracted_data not in records_out_list):
                records_out_list.append(extracted_data)
        except (ValueError, SyntaxError):
            # Skip lines that cannot be parsed
            continue

    list_vals = all_values_summed(records_out_list)

    return list_vals

def transform_dictionary_summed(elements:dict):
    l = []
    for elem in elements.values():
        l.append(elem)

    return sum(l)

def all_values_summed(elements:[dict]):
    l = []
    for elem in elements:
        l.append(transform_dictionary_summed(elem))
    return l

def cumulative_to_throughput_per_second(elements):
    l = []
    last_val =  0
    for elem in elements:
        throughput= (elem - last_val)/5
        l.append(throughput)
        last_val = elem
    return l
def plot_data(df):
    # Plotting the summed total values
    plt.figure(figsize=(12, 6))
    plt.plot(df['Total'], marker='o')
    plt.title('Total Number of Records Out per Unique Configuration')
    plt.xlabel('Configuration Index')
    plt.ylabel('Total Records Out')
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    # Path to the throughput data file
    file_path = 'C:/Users/Camil/OneDrive/Documents/masters/memoire/flinkImplemProject/src/main/python/experiments/experiment_100000_w3000_s6_p6_a1_lowThenHighSkew/0/throughput.dat'

    # Process the data
    total_cumulative_throughput = process_throughput_data(file_path)
    throughput = cumulative_to_throughput_per_second(total_cumulative_throughput)
    # Display the processed DataFrame
    print("Extracted and Summarized Data:")
    print(throughput)

    # Plot the data
    # plot_data(df)
