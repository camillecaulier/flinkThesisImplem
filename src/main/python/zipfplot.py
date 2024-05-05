import numpy as np
import matplotlib.pyplot as plt

def custom_zipf(n, a, size=1000):
    """ Generate Zipf-like random variables, but works well for a=1. """
    ranks = np.arange(1, n+1)
    weights = 1 / ranks**a
    weights /= weights.sum()
    return np.random.choice(ranks, size=size, p=weights)

# Parameters
a = 0.001 # Zipf distribution parameter (exponent)
sample_size = 1000  # Number of samples
max_rank = 27  # Maximum rank

# Generate data
data = custom_zipf(max_rank, a, sample_size)

# Plotting the data
plt.figure(figsize=(10, 6))
plt.hist(data, bins=range(1, max_rank+2), density=True, alpha=0.75, color='blue')
plt.title('Custom Zipf Distribution (a={})'.format(a))
plt.xlabel('Rank')
plt.ylabel('Frequency')
plt.show()
