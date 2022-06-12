from flask_restful import reqparse, Resource, Api
from cassandra_client import cassandra_client
from flask import Flask, request
from datetime import datetime, timedelta
import ast
import json


app = Flask(__name__)
api = Api(app)
app.config['BUNDLE_ERRORS'] = True


parser = reqparse.RequestParser()
parser.add_argument('request_num', type=int, required=True,
                    help='According to the task definition (from 1 to 3)')
parser.add_argument('file_name', type=str, required=False)


class RetrieveData(Resource):

    def get(self):

        args = request.get_json()

        now = datetime.now()
        time_start = (now - timedelta(hours=7)).replace(minute=0,
                                                        second=0, microsecond=0)
        time_end = (now - timedelta(hours=1)).replace(minute=0,
                                                      second=0, microsecond=0)
        if args["request_num"] == 1:

            result = client.query_statistics1()
            res = []

            for i in result:
                if i.time_start >= time_start and i.time_end <= time_end:
                    cur_time = {}
                    cur_time["time_start"] = str(i.time_start.hour) + ":00"
                    cur_time["time_end"] = str(i.time_end.hour) + ":00"
                    cur_time["statistics"] = []

                    statistics = ast.literal_eval(i.statistics)

                    for domain, count in statistics:
                        dct_statistics = {domain: count}
                        cur_time["statistics"].append(dct_statistics)
                    res.append(cur_time)

        elif args["request_num"] == 2:

            result = client.query_statistics2()

            res = {}
            res["time_start"] = str(time_start.hour) + ":00"
            res["time_end"] = str(time_end.hour) + ":00"
            res["statistics"] = []

            for i in result:
                if i.time_start >= time_start and i.time_end <= time_end:
                    statistics = ast.literal_eval(i.statistics)
                    res["statistics"].append(statistics)

        elif args["request_num"] == 3:

            res = {}
            result = client.query_statistics3()

            for i in result:
                if i.time_start >= time_start and i.time_end <= time_end:
                    res["time_start"] = str(i.time_start.hour) + ":00"
                    res["time_end"] = str(i.time_end.hour) + ":00"
                    statistics = ast.literal_eval(i.statistics)
                    user_info = []
                    for user_name, user_id, titles, page_count in statistics:
                        user_info.append(
                            {"user_name": user_name, "user_id": user_id, "titles": titles, "page_count": page_count})
                    res["statistics"] = user_info
        else:
            return "Invalid request"

        if 'file_name' in args:
            file_name = args['file_name']
            with open(f'{file_name}.json', 'w') as f:
                json.dump(res, f)

        return res


api.add_resource(RetrieveData, "/")

if __name__ == '__main__':
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'big_data_project'
    client = cassandra_client.CassandraClient(host, port, keyspace)
    client.connect()

    app.run(debug=True, host="0.0.0.0", port="8082")
