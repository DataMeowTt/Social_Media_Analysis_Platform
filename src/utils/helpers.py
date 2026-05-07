from pyspark.sql import Column
from pyspark.sql import functions as F

# TODO: change format when host API changes
TWITTER_DATE_FORMAT = "EEE MMM dd HH:mm:ss Z yyyy"

def parse_twitter_timestamp(col: Column) -> Column:
    return F.to_timestamp(col, TWITTER_DATE_FORMAT)
