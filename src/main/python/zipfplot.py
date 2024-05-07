import numpy as np
import matplotlib.pyplot as plt
import os
def custom_zipf(n, a, size=1000):
    """ Generate Zipf-like random variables, but works well for a=1. """
    ranks = np.arange(1, n+1)
    weights = 1 / ranks**a
    weights /= weights.sum()
    return np.random.choice(ranks, size=size, p=weights)

# # Parameters
# a = 0.001 # Zipf distribution parameter (exponent)
# sample_size = 1000  # Number of samples
# max_rank = 27  # Maximum rank
#
# # Generate data
# data = custom_zipf(max_rank, a, sample_size)
#
# # Plotting the data
# plt.figure(figsize=(10, 6))
# plt.hist(data, bins=range(1, max_rank+2), density=True, alpha=0.75, color='blue')
# plt.title('Custom Zipf Distribution (a={})'.format(a))
# plt.xlabel('Rank')
# plt.ylabel('Frequency')
# plt.show()
#
#
# distributions = [0.0, 0.35,0.7, 1.4, 2.1]


# Parameters for the plots
distributions = [0.0, 0.35, 0.7, 1.4, 2.1]  # Zipf distribution parameters (exponents)
sample_size = 1000  # Number of samples
max_rank = 27  # Maximum rank

# Create a figure and axes with 3 rows and 2 columns
fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(12, 18))
axes = axes.flatten()  # Flatten the 2D array of axes to simplify indexing

# Loop over each distribution parameter and plot
for i, a in enumerate(distributions):
    data = custom_zipf(max_rank, a, sample_size)

    # Plot the data on the ith subplot
    axes[i].hist(data, bins=range(1, max_rank + 2), density=True, alpha=0.75, color='blue')
    axes[i].set_title('Custom Zipf Distribution (a={})'.format(a))
    axes[i].set_xlabel('Rank')
    axes[i].set_ylabel('Frequency')

# Hide the last subplot if there's an odd number of distributions (5 in this case)
if len(axes) > len(distributions):
    axes[-1].axis('off')

plt.tight_layout()
plt.savefig(os.path.join("fiveplots"))
plt.show()
