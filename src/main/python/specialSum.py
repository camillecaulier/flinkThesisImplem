import numpy as np
def specialSum(values):
    if values is np.nan:
        return 0
    # print(values)
    tot = 0
    for v in values:
        tot += v

    return tot