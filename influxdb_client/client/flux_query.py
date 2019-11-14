import pandas as pd
import time
from datetime import datetime

def build_from(bucket):
    return f'from(bucket: "{bucket}")'

def build_range(start, stop):
    return f'|> range(start:{start}, stop:{stop})'

def build_equals(key, value, equality = "==", prefix="r",):
    if type(value) == str:
        return f'{prefix}.{key} == "{value}"'
    if type(value) == int:
        return f'{prefix}.{key} {equality} {value}'

def build_filters(filters, equality = "=="):
    filters_queries = []

    for filter in filters:
        or_query = ' or '.join([build_equals(filter[0], value, equality) for value in filter[1]])
        filters_queries.append(f'|> filter(fn: (r) => {or_query})')

    return "\n".join(filters_queries)

def build_flatten(flatten=True):
    if flatten == True:
        return f'|> group()'
    else: 
        pass 

def build_flux_query(bucket, filters, start, stop, equality = "==", flatten = True):
    return build_from(bucket) + "\n" + build_range(start, stop) + "\n" + build_filters(filters, equality) + "\n" + build_flatten(flatten=True)

def dataframe(result,precision,tags):
    raw = []
    for table in result:
        for record in table.records:
            for t in tags:
                raw.append((record.get_measurement(), t, record.values.get(t), record.get_field(), record.get_value(), record.get_time(), record.get_start(), record.get_stop()))

    df=pd.DataFrame(raw, columns=['measurement','tag_key','tag_value','field','value','time','start','stop'], index=None)
    # df['datetime'] = df['datetime'].values.astype('<M8'+ '[' + precision + ']')
    return df

    
