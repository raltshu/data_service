from flask_classful import FlaskView, route
from flask import request,render_template
import handlers.data_access

class DataView(FlaskView):

    def index(self):
        #TODO: Data service get first 20 lines for display
        return "Hello"
        


