from flask_classful import FlaskView, route
from flask import request,render_template

class DataView(FlaskView):

    def index(self):
        #TODO: Data service get first 20 lines for display
        print('Hello')
        


