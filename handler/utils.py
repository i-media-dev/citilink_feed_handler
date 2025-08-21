import numpy as np

from handler.constants import (
    LOWER_OUTLIER_PERCENTILE,
    UPPER_OUTLIER_PERCENTILE
)


def calc_quantile(data):
    array = np.array(data)
    Q1 = np.quantile(array, LOWER_OUTLIER_PERCENTILE)
    Q3 = np.quantile(array, UPPER_OUTLIER_PERCENTILE)
    IQR = Q3 - Q1
    upper_outlier_threshold = Q3 + 1.5 * IQR
    lower_outlier_threshold = Q1 - 1.5 * IQR
    filtered_data = array[(array >= lower_outlier_threshold) &
                          (array <= upper_outlier_threshold)]
    return filtered_data.tolist()


def clear_min(data):
    filtered_data = calc_quantile(data)
    return min(filtered_data)


def clear_max(data):
    filtered_data = calc_quantile(data)
    return max(filtered_data)
