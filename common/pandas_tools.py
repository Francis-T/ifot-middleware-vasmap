import pandas as pd
import numpy as np

def df_split(in_df, nodes):
    split = in_df.shape[0] // nodes
    df_arr = []
    for g, df in in_df.groupby(np.arange(len(in_df)) // split):
        df_arr.append(df)
    return df_arr