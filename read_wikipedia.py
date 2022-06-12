import requests
import json
from cassandra_client import CassandraClient
from datetime import datetime
import cassandra


def write_to_cassandra():
    while True:
        try:
            url = 'https://stream.wikimedia.org/v2/stream/page-create'
            req = requests.get(url=url, stream=True)

            for message in req.iter_lines(decode_unicode=True):
                if message[:4] == "data":
                    record = json.loads(message[5:])

                    client.insert_record(
                        "page_domain", {"domain": record["meta"]["domain"], "page_id": record["page_id"]})
                    client.insert_record(
                        "pages", {"page_id": record["page_id"], "page_title": record["page_title"]})
                    client.insert_record(
                        "page_user", {"user_id": record["performer"]["user_id"], "page_title": record["page_title"]})

                    review_date = record["rev_timestamp"]
                    review_date = datetime.strptime(
                        record["rev_timestamp"], "%Y-%m-%dT%H:%M:%SZ")

                    client.insert_record("info_per_date", {"review_date": review_date, "page_title": record["page_title"],
                                                           "page_id": record["page_id"], "user_id": record["performer"]["user_id"]})

                    client.insert_record("general", {"review_date": review_date, "domain": record["meta"]["domain"],
                                                     "page_id": record["page_id"], "user_is_bot": record["performer"]["user_is_bot"],
                                                     "page_title": record["page_title"],
                                                     "user_id": record["performer"]["user_id"],
                                                     "user_text": record["performer"]["user_text"]})
            break
        except (KeyError, cassandra.protocol.SyntaxException):
            continue


if __name__ == "__main__":
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'big_data_project'
    client = CassandraClient(host, port, keyspace)

    client.connect()
    write_to_cassandra()
