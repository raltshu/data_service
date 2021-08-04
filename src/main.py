import os, time
from flask import Flask
from handlers.data_proxy import DataView
import handlers.data_access

app = Flask(__name__)

time.sleep(10) # Addressing some timing issue in the Azure where MySQL service not ready and fails the image
handlers.data_access.init_tables()
DataView.register(app, route_base='/data')
handlers.data_access.load_diamonds_to_db()


if __name__ == '__main__':
    # app.run(host='0.0.0.0', port=5001)
    # load_diamonds_to_db()
    pass

