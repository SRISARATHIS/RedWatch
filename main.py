import pandas as pd

df =pd.read_parquet('full.parquet')
print(df.head(20))