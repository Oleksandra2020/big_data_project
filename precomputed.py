from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.functions import col, date_trunc, desc
from cassandra_client import CassandraClient
from datetime import timedelta, datetime
from apscheduler.schedulers.blocking import BlockingScheduler

spark = SparkSession.builder.appName('big_data_project').getOrCreate()


def read_from_cassandra():
    rows = client.query_general()
    data = []
    for i in rows:
        data.append(i)
    return spark.createDataFrame(data)


def report1(df):
    now = datetime.now()
    time_start = (now - timedelta(hours=7)).replace(minute=0,
                                                    second=0, microsecond=0)
    time_end = (now - timedelta(hours=1)).replace(minute=0,
                                                  second=0, microsecond=0)

    grouped_domain = df.filter((df.review_date >= time_start) & (df.review_date <= time_end)).withColumn(
        "review_date", date_trunc("hour", col("review_date"))).groupBy(
        'review_date', 'domain').agg(F.count('page_id')).sort("review_date")
    # grouped_domain.show()

    time_dct = {}
    for d in grouped_domain.collect():
        domain, review_date, count = d[1], d[0], d[2]
        if review_date not in time_dct:
            time_dct[review_date] = []
        time_dct[review_date].append([domain, count])

    for k in time_dct:
        time_start, time_end, statistics = k, k + \
            timedelta(hours=1), f"{time_dct[k]}"
        statistics = statistics.replace("'", '"')
        query = f"INSERT INTO statistics1 (time_start, time_end, statistics) VALUES ('{time_start}', '{time_end}', '{statistics}');"
        client.execute(query)


def report2(df):
    now = datetime.now()
    time_start = (now - timedelta(hours=7)).replace(minute=0,
                                                    second=0, microsecond=0)
    time_end = (now - timedelta(hours=1)).replace(minute=0,
                                                  second=0, microsecond=0)

    grouped_domain = df.filter((df.review_date >= time_start) & (df.review_date <= time_end)).withColumn(
        "user_is_bot", col("user_is_bot").cast('integer')).groupBy(
        'domain').agg(F.sum('user_is_bot'))
    # grouped_domain.show()

    domain_bots = []

    for d in grouped_domain.collect():
        domain, bot = d['domain'], d['sum(user_is_bot)']
        domain_bots.append({"domain": domain, "created_by_bots": bot})

    time_start, time_end, statistics = time_start, time_end, f"{domain_bots}"
    statistics = statistics.replace("'", '"')
    query = f"INSERT INTO statistics2 (time_start, time_end, statistics) VALUES ('{time_start}', '{time_end}', '{statistics}');"
    client.execute(query)


def report3(df):
    now = datetime.now()
    time_start = (now - timedelta(hours=7)).replace(minute=0,
                                                    second=0, microsecond=0)
    time_end = (now - timedelta(hours=1)).replace(minute=0,
                                                  second=0, microsecond=0)

    grouped_domain = df.withColumn("review_date", date_trunc("hour", col("review_date"))).filter(
        (df.review_date >= time_start) & (df.review_date <= time_end)).groupBy(
        'user_id').agg(F.count("page_id")).sort(desc("count(page_id)")).limit(20)

    # grouped_domain.show()

    users = [u[0] for u in grouped_domain.collect()]
    statistics = []

    for user_id in users:
        page_titles = df.filter((df.review_date >= time_start) &
                                (df.review_date <= time_end) & (df.user_id == user_id)).select("page_title").distinct()
        user_name = df.filter((df.review_date >= time_start) &
                              (df.review_date <= time_end) & (df.user_id == user_id)).select("user_text").distinct()

        titles = []
        for i in page_titles.collect():
            titles.append(i[-1])

        name = user_name.collect()[0].user_text
        statistics.append([name, user_id, titles, len(titles)])

    statistics = f"{statistics}"
    statistics = statistics.replace("'", '"')
    query = f"INSERT INTO statistics3 (time_start, time_end, statistics) VALUES ('{time_start}', '{time_end}', '{statistics}');"
    client.execute(query)


def write_reports():
    df = read_from_cassandra()
    print("Time:", str(datetime.now()))
    report1(df)
    report2(df)
    report3(df)


if __name__ == "__main__":
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'big_data_project'
    client = CassandraClient(host, port, keyspace)
    client.connect()

    # call every hour
    scheduler = BlockingScheduler()
    scheduler.add_job(write_reports, 'interval', hours=1)
    scheduler.start()

    # df = read_from_cassandra()
    # report1(df)
    # report2(df)
    # report3(df)

    client.close()
