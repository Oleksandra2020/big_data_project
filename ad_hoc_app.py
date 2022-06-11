import re
from flask_restful import reqparse, Resource, Api
from cassandra_client import CassandraClient
from flask import Flask, request
from datetime import datetime

app = Flask(__name__)
api = Api(app)
app.config['BUNDLE_ERRORS'] = True


parser = reqparse.RequestParser()
parser.add_argument('request_num', type=int, required=True,
                    help='According to the task definition (from 1 to 5)')
parser.add_argument('user_id', type=int, required=False)
parser.add_argument('domain', type=str, required=False)
parser.add_argument('page_id', type=int, required=False)
parser.add_argument('start_date', type=str, required=False)
parser.add_argument('end_date', type=str, required=False)


class RetrieveData(Resource):

    def get(self):

        args = request.get_json()

        if args['request_num'] == 1:
            result = client.query_one()
            res = {"domains": []}
            for i in result:
                res["domains"].append(i.domain)
        elif args['request_num'] == 2:
            result = client.query_two(args['user_id'])
            res = {"page_titles": []}
            for i in result:
                res["page_titles"].append(i.page_title)
        elif args['request_num'] == 3:
            result = client.query_three(args['domain'])
            res = {"page_count": []}
            for i in result:
                res["page_count"].append(i.count)
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
            client.close()
            return "Invalid request"

        return res


api.add_resource(RetrieveData, "/")

if __name__ == '__main__':
    host = 'cassandra-node1'
    port = 9042
    keyspace = 'big_data_project'
    client = CassandraClient(host, port, keyspace)
    client.connect()

    app.run(debug=True, host="0.0.0.0", port="8081")