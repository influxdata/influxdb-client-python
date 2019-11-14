import pandas as pd
import time
from datetime import datetime

def lp(df,measurement,tag_key,tag_value,field,value,datetime):
    lines= [str(df[measurement][d]) + "," 
            + str(df[tag_key][d]) + "=" + str(df[tag_value][d]) 
            + " " + str(df[field][d]) + "=" + str(df[value][d]) 
            + " " + str(int(time.mktime(df[datetime][d].timetuple()))) + "000000000" for d in range(len(df))]
    return lines