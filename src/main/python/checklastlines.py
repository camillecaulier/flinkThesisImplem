def print_last_20_lines(filename):
    try:
        # Open the file in read mode.
        with open(filename, 'r') as file:
            lines = file.readlines()
            last_20_lines = lines[-20:] if len(lines) > 20 else lines
            for line in last_20_lines:
                print(line, end='')
    except FileNotFoundError:
        print(f"The file {filename} does not exist.")
    except Exception as e:
        print(f"An error occurred: {e}")


print_last_20_lines('../../../dataSmall/zipf_distribution100000_1_50_0.7.csv')