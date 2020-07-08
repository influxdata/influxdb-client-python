"""Extras to selectively import Pandas or NumPy."""

try:
    import pandas as pd
except ModuleNotFoundError as err:
    raise ImportError(f"`query_data_frame` requires Pandas which couldn't be imported due: {err}")

try:
    import numpy as np
except ModuleNotFoundError as err:
    raise ImportError(f"`data_frame` requires numpy which couldn't be imported due: {err}")

__all__ = ['pd', 'np']
