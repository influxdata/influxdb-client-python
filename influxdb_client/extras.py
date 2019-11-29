try:
    import pandas as pd
except ModuleNotFoundError as err:
    raise ImportError(f"`query_data_frame` requires Pandas which couldn't be imported due: {err}")

__all__ = ['pd']
