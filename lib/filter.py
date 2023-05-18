import pandas as pd


def between(time_start: str, time_end: str, df):
    return df[
        (df["timestamp"] >= time_start) & (df["timestamp"] < time_end)
    ].reset_index(drop=True)
