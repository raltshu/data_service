FROM python:3.9-slim

WORKDIR /app
ENV PYTHONPATH "${PYTHONPATH}:./src"
RUN apt-get update && apt-get install -y python3-dev default-libmysqlclient-dev build-essential
ENV FLASK_APP=main.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5002
ENV FLASK_ENV=development
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY ./src /app

EXPOSE 5002

CMD ["flask","run"]
# CMD python ./main.py