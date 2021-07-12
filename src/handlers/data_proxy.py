from flask_classful import FlaskView, route
from flask import request,render_template
import handlers.data_access

class DataView(FlaskView):

    def index(self):
        #TODO: Data service get first 20 lines for display
        return "Hello"
    
    @route('/table_view/<string:table_name>')
    def get_table(self, table_name):
        limit = request.args.get('limit')
        limit = limit if limit.isnumeric() else None
        json_obj = handlers.data_access.read_table_from_db(table_name, limit)
        return json_obj

