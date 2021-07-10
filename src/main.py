from flask import Flask
from handlers.data_proxy import DataView


app = Flask(__name__)


DataView.register(app, route_base='/data')


# if __name__ == '__main__':
#     app.run(host='0.0.0.0', port=5001)

