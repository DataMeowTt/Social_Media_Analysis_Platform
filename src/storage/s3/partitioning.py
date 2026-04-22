from datetime import datetime

def get_partition_prefix(partition_time: datetime, partition_cols: list[str]) -> str:

    partition_values = []
    for col in partition_cols:
        if col == "year":
            partition_values.append(f"year={partition_time.strftime('%Y')}")
        elif col == "month":
            partition_values.append(f"month={partition_time.strftime('%m')}")
        elif col == "day":
            partition_values.append(f"day={partition_time.strftime('%d')}")

    return "/".join(partition_values) + "/"
    

def get_s3_key(layer: str, dataset: str, filename: str, partition_time: datetime, partition_cols: list[str]) -> str:
    partition_prefix = get_partition_prefix(partition_time, partition_cols)
    
    return f"{layer}/{dataset}/{partition_prefix}{filename}"

###############################################################################
# example output: "raw/tweets/year=2024/month=06/day=15/tweets_2024-06-15.json"
###############################################################################