
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

def filter_valid_age(df:DataFrame)->DataFrame:
    valid_df = df.filter((col("Age").isNotNull()) & (col("Age") > 0))
    return valid_df