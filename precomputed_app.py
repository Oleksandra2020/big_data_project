from flask_restful import Resource, Api
from flask import Flask

app = Flask(__name__)
api = Api(app)
app.config['BUNDLE_ERRORS'] = True



class RetrieveData(Resource):

    def get(self):
        print("----------------------------------Report1----------------------------------")
        # read 6 json files, merge and output
        print("----------------------------------Report2----------------------------------")
        # read 6 json files, merge and output
        print("----------------------------------Report3----------------------------------")
        # read 6 json files, merge and output
        return


api.add_resource(RetrieveData, "/")

if __name__ == '__main__':

    app.run(debug=True, host="0.0.0.0", port="8082")
