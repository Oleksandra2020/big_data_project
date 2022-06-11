from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_trunc, desc
from cassandra_client import CassandraClient
from datetime import timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

spark = SparkSession.builder.appName('big_data_project').getOrCreate()


def read_from_cassandra():
    rows = client.query_general()
    data = []
    for i in rows:
        data.append(i)
    return spark.createDataFrame(data)


def rerport1():
    grouped_domain = df.withColumn("review_date", date_trunc("hour", col("review_date"))).groupBy(
        'review_date', 'domain').agg(F.count('page_id')).sort("review_date")
    print(grouped_domain.show())

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
        domain, review_date, count = d[1], d[0], d[2]
        if review_date not in time_dct:
            time_dct[review_date] = []
        time_dct[review_date].append([domain, count])

    for k in time_dct:
        data = [str(k), str(k+timedelta(hours=1)), time_dct[k]]
        print(data)
        res_df = spark.createDataFrame(data=[data], schema=schema)
        res_df.printSchema()
        res_df.show(truncate=False)

        # r = str(k)[11:13] # retrieve hour
        # res_df.write.mode("Overwrite").json(f"./a_{r}")


def rerport2():
    grouped_domain = df.withColumn("review_date", date_trunc("hour", col("review_date"))).groupBy(
        'review_date', 'domain').agg(F.count('user_is_bot')).sort("review_date")
    print(grouped_domain.show())

    schema = StructType([
        StructField("time_start", StringType()),
        StructField("time_end", StringType()),
        StructField("statistics", ArrayType(
            StructType([
                    StructField("domain", StringType()),
                    StructField("created_by_bots", LongType())
                    ])
        ))
    ])

    bot_dct = {}

    for d in grouped_domain.collect():
        domain, review_date, bot = d[1], d[0], d[2]
        if review_date not in bot_dct:
            bot_dct[review_date] = []
        bot_dct[review_date].append([domain, bot])

    for k in bot_dct:
        data = [str(k), str(k+timedelta(hours=1)), bot_dct[k]]
        print(data)
        res_df = spark.createDataFrame(data=[data], schema=schema)
        res_df.printSchema()
        res_df.show(truncate=False)

        # r = str(k)[11:13] # retrieve hour
        # res_df.write.mode("Overwrite").json(f"./b_{r}")


def rerport3():
    grouped_domain = df.withColumn("review_date", date_trunc("hour", col("review_date"))).groupBy(
        'user_id').agg(F.count("page_id")).sort(desc("count(page_id)")).limit(20)

    users = [u[0] for u in grouped_domain.collect()]
    filtered_users = df.filter(df.user_id.isin(users)).select("user_text", "user_id", "review_date", "page_title")

    schema = StructType([
        StructField("user_name", StringType()),
        StructField("user_id", LongType()),
        StructField("time_start", StringType()),
        StructField("time_end", StringType()),
        StructField("page_count", LongType()),
        StructField("page_titles", ArrayType(
            StructType([
                    StructField("page_title", StringType())
                ])
        ))
    ])
    data = []
    for user in users:
        f = filtered_users.select("user_text", "review_date", "page_title").where(filtered_users.user_id == user)
        page_titles = []
        for i in f.collect():
            page_titles.append(str(i['page_title']))

        data.append([i["user_text"], user, str(i['review_date']), str(i['review_date']+timedelta(hours=6)), len(page_titles), page_titles])

    res_df = spark.createDataFrame(data=data, schema=schema)
    res_df.printSchema()
    res_df.show(truncate=False)

    # res_df.write.mode("Overwrite").json(f"./report3_hour_{}")


if __name__ == "__main__":
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'big_data_project'
    client = CassandraClient(host, port, keyspace)
    client.connect()

    # # call every hour
    # scheduler = BlockingScheduler()
    # df = read_from_cassandra()
    # scheduler.add_job(rerport1, 'interval', hours=1)
    # scheduler.add_job(rerport2, 'interval', hours=1)
    # scheduler.add_job(rerport3, 'interval', hours=1)
    # scheduler.start()

    df = read_from_cassandra()
    rerport1()
    # rerport2()
    # rerport3()

    client.close()
