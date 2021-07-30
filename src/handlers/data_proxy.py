from types import MethodType
from flask_classful import FlaskView, route
from flask import request, Response
import handlers.data_access

class DataView(FlaskView):

    def index(self):
        #TODO: Data service get first 20 lines for display
        return "Hello"
    
    @route('/table_view/<string:table_name>', methods=['GET'])
    def get_table(self, table_name):
        limit = request.args.get('limit')
        order_by = request.args.get('order_by')
        order_by = order_by if 'None' != order_by else None
        order_asc_desc = request.args.get('order_asc_desc')
        order_asc_desc = order_asc_desc if 'None' != order_asc_desc else None
        limit = limit if limit is not None and limit.isnumeric() else None
        json_obj = handlers.data_access.read_table_from_db(table_name, limit, order_by, order_asc_desc)
        return json_obj

    @route('/audit', methods=['POST'])
    def audit_prediction(self):
        row = request.get_json()
        row_id = handlers.data_access.audit_predition(row)
        return str(row_id)

    @route('/feedback/<int:row_id>', methods=['PUT'])
    def prediction_feedback(self, row_id):
        data = request.get_json()
        handlers.data_access.submit_feedback(row_id, data['grade'], data['user_prediction'])

        return Response(status=200)

    @route('/alert', methods=['POST'])
    def add_alert(self):
        data=request.get_json()
        alert_id = handlers.data_access.add_alert(data)

        return Response(str(alert_id), status=200)

