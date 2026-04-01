from datetime import datetime

def get_partition_prefix(ingestion_time: datetime, partition_cols: list[str]) -> str:

    partition_values = []
    for col in partition_cols:
        if col == "ingestion_date":
            partition_values.append(f"ingestion_date={ingestion_time.strftime('%Y-%m-%d')}")

        # TODO Complement partitioning for processed and analysis layer later. 
        # elif col == "year":
        #     ...
        # elif col == "month":
        #     ...
        # elif col == "day":
        #     ...

    return "/".join(partition_values) + "/"
    

def get_s3_key(layer: str, dataset: str, filename: str, ingestion_time: datetime, partition_cols: list[str]) -> str:
    
    partition_prefix = get_partition_prefix(ingestion_time, partition_cols)

    return f"{layer}/{dataset}/{partition_prefix}{filename}"
    # example output: "raw/tweets/year=2024/month=06/day=15/tweets_2024-06-15.json"