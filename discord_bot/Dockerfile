FROM python:3.10.5-slim-buster

WORKDIR /discord

COPY . /discord

RUN pip3 install -r /discord/requirements.txt

CMD ["python3", "./bot.py"]