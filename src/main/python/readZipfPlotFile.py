import pandas as pd
import matplotlib.pyplot as plt
import os as os

cur_dir = os.getcwd()

# Function to create and save histogram from a CSV file
def create_histogram(ax, csv_file, exponent):
    # Load the CSV file
    df = pd.read_csv(csv_file, header=None, names=['key', 'value', 'timestamp'])

    # Count occurrences of each key
    key_counts = df['key'].value_counts().sort_values(ascending=False)

    total_count = key_counts.sum()
    frequencies = key_counts / total_count
    # print(key_counts[0:5])
    print(frequencies.values[0 : 5] )

    # Plot histogram
    ax.bar(range(1, len(frequencies) + 1), frequencies.values, color='blue')
    ax.set_xlabel('Rank')
    ax.set_ylabel('Frequency in percentage')
    ax.set_ylim(0,frequencies.values[0] )
    ax.set_title(f'Custom Zipf Distribution with exponent {exponent} and keyspace of ~19.5k')


# List of file paths and their corresponding exponents
csv_files = [
    (cur_dir+'\exampleMultiSourceData\zipf_distribution_100000_3_1_1.0E-15.csv', 0.0),
(cur_dir+'\exampleMultiSourceData\zipf_distribution_100000_3_1_0.7.csv', 0.7),
(cur_dir+'\exampleMultiSourceData\zipf_distribution_100000_3_1_1.4.csv', 1.4),
(cur_dir+'\exampleMultiSourceData\zipf_distribution_100000_3_1_2.1.csv',2.1),
    # Add the paths to the other four CSV files here, with their exponents
]


# Create subplots
fig, axes = plt.subplots(3, 2, figsize=(18, 12))  # Adjusted for 5 plots
axes = axes.flatten()

# Loop through each file and create its histogram
for i, (csv_file, exponent) in enumerate(csv_files):
    create_histogram(axes[i], csv_file, exponent)

# Hide any unused subplots (if there are fewer than 6)
for j in range(len(csv_files), len(axes)):
    fig.delaxes(axes[j])

# Adjust layout
plt.tight_layout()
plt.savefig("plot_distributions_3")
plt.show()
