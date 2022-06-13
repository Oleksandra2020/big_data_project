from flask_restful import reqparse, Resource, Api
from cassandra_client import cassandra_client
from flask import Flask, request
from datetime import datetime
import json


app = Flask(__name__)
api = Api(app)
app.config['BUNDLE_ERRORS'] = True


parser = reqparse.RequestParser()
parser.add_argument('request_num', type=int, required=True,
                    help='According to the task definition (from 1 to 8)')
parser.add_argument('user_id', type=int, required=False)
parser.add_argument('domain', type=str, required=False)
parser.add_argument('page_id', type=int, required=False)
parser.add_argument('start_date', type=str, required=False)
parser.add_argument('end_date', type=str, required=False)
parser.add_argument('file_name', type=str, required=False)

# requests 1-5 are for Category B questions


class RetrieveData(Resource):

    def get(self):

        args = request.get_json()

        if args['request_num'] == 1:
            result = client.query_one()
            res = {"domains": []}
            for i in result:
                res["domains"].append(i.domain)
        elif args['request_num'] == 2:
            result = client.query_two()
            res = {"page_titles": []}
            for i in result:
                if i.user_id == args["user_id"]:
                    res["page_titles"].append(i.page_title)
        elif args['request_num'] == 3:
            result = client.query_three()
            res = {}
            count = 0
            for i in result:
                if i.domain == args['domain']:
                    count += 1
            res["page_count"] = count
        elif args['request_num'] == 4:
            result = client.query_four(args['page_id'])
            res = {"page_titles": []}
            for i in result:
                res["page_titles"].append(i.page_title)
        elif args['request_num'] == 5:
            result = client.query_five()
            res = {"info": []}
            for i in result:

                start_date = datetime.strptime(
                    args['start_date'], "%Y-%m-%dT%H:%M:%SZ")
                end_date = datetime.strptime(
                    args['end_date'], "%Y-%m-%dT%H:%M:%SZ")

                if i.review_date > start_date and i.review_date < end_date:
                    dct = {}
                    dct['page_id'] = i.page_id
                    dct['review_date'] = str(i.review_date)
                    dct['page_title'] = i.page_title
                    dct['user_id'] = i.user_id
                    res["info"].append(dct)
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

    app.run(debug=True, host="0.0.0.0", port="8081")
