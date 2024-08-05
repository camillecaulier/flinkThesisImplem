
def gini_index(values):
    """
    Calculate the Gini index for a list of values.

    :param values: List of int
    :return: Gini index (floaty)
    """
    if not values:
        return 0  # Return 0 if the list is empty

    # Sort the list of values in ascending order
    sorted_values = sorted(values)

    cumulative_sum = 0
    total_sum = sum(sorted_values)
    n = len(values)

    for i, value in enumerate(sorted_values):
        cumulative_sum += value * (i + 1)

    gini = (2 * cumulative_sum) / (n * total_sum) - (n + 1) / n

    return gini