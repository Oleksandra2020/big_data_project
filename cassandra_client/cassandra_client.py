class CassandraClient:
    def __init__(self, host, port, keyspace):
        self.host = host
        self.port = port
        self.keyspace = keyspace
        self.session = None

    def connect(self):
        from cassandra.cluster import Cluster
        cluster = Cluster([self.host], port=self.port)
        self.session = cluster.connect(self.keyspace)

    def execute(self, query):
        self.session.execute(query)

    def close(self):
        self.session.shutdown()

    def insert_record(self, table, data):
        if table == "page_domain":
            query = f"INSERT INTO {table} (domain, page_id) VALUES ('{data['domain']}', {data['page_id']});"
        if table == "pages":
            query = f"INSERT INTO {table} (page_id, page_title) VALUES ({data['page_id']}, '{data['page_title']}');"
        if table == "info_per_date":
            query = f"INSERT INTO {table} (review_date, page_title, page_id, user_id) VALUES" + \
                f"('{data['review_date']}', '{data['page_title']}', {data['page_id']}, {data['user_id']});"
        if table == "general":
            query = f"INSERT INTO {table} (review_date, domain, page_id, user_is_bot," + \
                "page_title, user_id, user_text) VALUES" + \
                f"('{data['review_date']}', '{data['domain']}', {data['page_id']}, {data['user_is_bot']}, " + \
                f"'{data['page_title']}', {data['user_id']}, '{data['user_text']}');"

        self.session.execute(query)

    def query_one(self):
        query = "SELECT domain FROM page_domain;"
        rows = self.session.execute(query)
        print(rows)
        return rows

    def query_two(self):
        query = f"SELECT page_title, user_id FROM general;"
        rows = self.session.execute(query)
        return rows

    def query_three(self):
        query = f"SELECT page_id, domain FROM page_domain;"
        rows = self.session.execute(query)
        return rows

    def query_four(self, page_id):
        query = f"SELECT page_title FROM pages WHERE page_id={page_id};"
        rows = self.session.execute(query)
        return rows

    def query_five(self):
        query = "SELECT * FROM info_per_date;"
        rows = self.session.execute(query)
        return rows

    def query_general(self):
        query = "SELECT * FROM general;"
        rows = self.session.execute(query)
        return rows

    def query_statistics1(self):
        query = f"SELECT * FROM statistics1;"
        rows = self.session.execute(query)
        return rows

    def query_statistics2(self):
        query = "SELECT * FROM statistics2;"
        rows = self.session.execute(query)
        return rows

    def query_statistics3(self):
        query = "SELECT * FROM statistics3;"
        rows = self.session.execute(query)
        return rows
