# import numpy as np
# import matplotlib.pyplot as plt
# import os
# def custom_zipf(n, a, size=1000):
#     """ Generate Zipf-like random variables, but works well for a=1. """
#     ranks = np.arange(1, n+1)
#     weights = 1 / ranks**a
#     weights /= weights.sum()
#     return np.random.choice(ranks, size=size, p=weights)
#
#
#
# # Parameters for the plots
# exponents = [0.0, 0.35, 0.7, 1.4, 2.1]  # Zipf distribution exponents
# sample_size = 100000  # Number of samples
# max_rank = 27**3  # Maximum rank
#
# # Create a figure and axes with 3 rows and 2 columns
# fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(12, 18))
# axes = axes.flatten()  # Flatten the 2D array of axes to simplify indexing
#
# # Loop over each distribution parameter and plot
# for i, a in enumerate(exponents):
#     data = custom_zipf(max_rank, a, sample_size)
#
#     # Plot the data on the ith subplot
#     axes[i].hist(data, bins=range(1, max_rank + 2), density=True, alpha=0.75, color='blue')
#     axes[i].set_title('Custom Zipf Distribution  with exponent {}'.format(a))
#     axes[i].set_xlabel('Rank')
#     axes[i].set_ylabel('Frequency')
#
# # Hide the last subplot if there's an odd number of distributions (5 in this case)
# if len(axes) > len(exponents):
#     axes[-1].axis('off')
#
# plt.tight_layout()
# plt.savefig(os.path.join("fiveplots"))
# plt.show()

import numpy as np
import matplotlib.pyplot as plt
import os

def generate_data(a, n, size):
    """Generate data following a Zipf-like distribution or uniform if a is 0."""
    if a == 0.0:
        # Use uniform distribution if exponent is 0
        return np.random.randint(1, n + 1, size)
    else:
        # Use Zipf distribution for non-zero exponents
        from scipy.stats import zipf
        data = zipf.rvs(a, size=size)
        return data[data <= n]  # Limit max rank to avoid extreme values

# Parameters for the plots
exponents = [0.0, 0.35, 0.7, 1.4, 2.1]  # Zipf distribution exponents
sample_size = 100000  # Number of samples
max_rank = 27**3  # Maximum rank

# Create a figure and axes with 3 rows and 2 columns
fig, axes = plt.subplots(nrows=3, ncols=2, figsize=(12, 18))
axes = axes.flatten()  # Flatten the 2D array of axes to simplify indexing

# Loop over each distribution parameter and plot
for i, a in enumerate(exponents):
    data = generate_data(a, max_rank, sample_size)

    # Plot the data on the ith subplot
    axes[i].hist(data, bins=range(1, max_rank + 2), density=True, alpha=0.75, color='blue')
    axes[i].set_title('Distribution with exponent {:.2f}'.format(a))
    axes[i].set_xlabel('Rank')
    axes[i].set_ylabel('Frequency')

# Hide the last subplot if there's an odd number of distributions (5 in this case)
if len(axes) > len(exponents):
    axes[-1].axis('off')

plt.tight_layout()
plt.savefig(os.path.join("plots", "fiveplots"))
plt.show()

