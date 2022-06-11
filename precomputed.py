from tokenize import group
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_trunc
from cassandra_client import CassandraClient
from datetime import datetime, timedelta

spark = SparkSession.builder.appName('big_data_project').getOrCreate()


def read_from_cassandra():
    rows = client.query_general()
    data = []
    for i in rows:
        data.append(i)
    return data


def rerport1(df):
    grouped_domain = df.withColumn("review_date", date_trunc("hour", col("review_date"))).groupBy(
        'domain', 'review_date').agg(F.count('page_id')).sort("review_date")
    # print(grouped_domain.show())

    schema = StructType([
        StructField("time_start", StringType()),
        StructField("time_end", StringType()),
        StructField("statistics", ArrayType(
            StructType([
                    StructField("domain", StringType()),
                    StructField("page_count", LongType())
                ])
        ))
    ])

    time_dct = {}

    for d in grouped_domain.collect():
        domain, review_date, count = d[0], d[1], d[2]
        if review_date not in time_dct:
            time_dct[review_date] = []
        time_dct[review_date].append([domain, count])

        for k in time_dct:
            data = [str(k), str(k+timedelta(hours=1)), time_dct[k]]

        res_df = spark.createDataFrame(data=data, schema=schema)
        res_df.printSchema()
        # res_df.show(truncate=False)

        res_df.write.mode("Overwrite").json(f"./{str(review_date)}")


if __name__ == "__main__":
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'big_data_project'
    client = CassandraClient(host, port, keyspace)
    client.connect()

    header = ['domain', 'page_id', 'page_title',
              'review_date', 'user_id', 'user_is_bot', 'user_text']
    data = read_from_cassandra()
    # print(data)
    df = spark.createDataFrame(data)
    # print(df.show())

    # call every hour
    rerport1(df)

    client.close()
