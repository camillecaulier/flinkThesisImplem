def parse_data_csvfile(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

    metric_pattern = re.compile(r'^metric:(.+)$')
    runtime_pattern = re.compile(r'Job Runtime: (\d+) ms')

    data = []
    current_runtime = None  # To store runtime for next metric entry

    for line in lines:
        runtime_match = runtime_pattern.search(line)
        if runtime_match:
            current_runtime = int(runtime_match.group(1))

        metric_match = metric_pattern.match(line)
        if metric_match and current_runtime is not None:
            metric_data = metric_match.group(1).split(',')
            csv_source = metric_data[5].split("_")
            # print(csv_source)
            csv_source[5] = csv_source[5].replace(".csv", "")
            if(csv_source[5] == "1.0E-15"):
                csv_source[5] = "0.0"
            # print(csv_source)
            csv_source.remove("distribution")
            for val in csv_source:
                metric_data.append(val)

            metric_data.append(current_runtime)  # Append the runtime to metric data
            data.append(metric_data)
            current_runtime = None  # Reset runtime after adding to data

    # Define DataFrame column names
    column_names = ['Operator', 'Duration', 'MainParallelism', 'HybridParallelism', 'Choices', 'CSVSource','distributionType','eventPerWindow','keySize','amountOfWindows','skew', 'Runtime (ms)']
    # Create DataFrame
    df = pd.DataFrame(data, columns=column_names)
    return df