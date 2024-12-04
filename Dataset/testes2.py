import pandas as pd
import polars as pl
import modin.pandas as mpd
from pyspark.sql import SparkSession
import time
from memory_profiler import memory_usage

dataset_path = "Dataset\\bankdataset.xlsx"

df = pd.read_excel(dataset_path)
print(df.head())