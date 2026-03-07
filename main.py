from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.schedules import Interval

load_dotenv()

API_TOKEN = Secret.load("freecryptoapi-key").get()
WEBHOOK_URL = Secret.load("bitcoio-channel-webhook").get()


@task
def get_bitcoin_price():
    url = "https://api.freecryptoapi.com/v1/getData?symbol=BTC"
    params = {
        "symbol": "BTC",
    }
    headers = {"accept": "*/*", "Authorization": f"Bearer {API_TOKEN}"}

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        data = response.json()
        message_content = {}
        bitcoin_data = data["symbols"][0]
        message_content["current_price"] = bitcoin_data["last"]
        message_content["highest"] = bitcoin_data["highest"]
        message_content["lowest"] = bitcoin_data["lowest"]
        message_content["current_datetime"] = datetime.now().strftime(
            "%d/%m/%Y %H:%M:%S"
        )
        message_content["percentage_change"] = (
            float(bitcoin_data["daily_change_percentage"]) * 100
        )
        return message_content
    else:
        raise Exception(f"Erro ao obter dados do Bitcoin: {response.status_code}")


@task
def send_message(content):

    message = (
        "## Monitoramento de Preço do Bitcoin 💰️\n"
        f"* 🪙 1 BTC = ${content['current_price']}\n"
        f"* 🪙 Maior preço (24h): ${content['highest']}\n"
        f"* 🪙 Menor preço (24h): ${content['lowest']}\n"
        f"* 🪙 Variação percentual diária: {content['percentage_change']:.2f}%\n"
        f"* 🪙 Data e hora da consulta: {content['current_datetime']}"
    )

    payload = {
        "content": message,
        "username": "Satoshi",
        "avatar_url": "https://i.pinimg.com/736x/d1/79/06/d179067a684f405acef19f844baa0aef.jpg",
    }

    response = requests.post(WEBHOOK_URL, json=payload)

    if response.status_code == 204:
        print("Mensagem enviada com sucesso!")
    else:
        print(f"Erro ao enviar: {response.status_code}")


@flow
def main():
    message_data = get_bitcoin_price()
    send_message(message_data, wait_for=[message_data])


if __name__ == "__main__":
    main.serve(
        name="Bitcoin Price Monitor",
        schedule=Interval(
            timedelta(minutes=5),
            anchor_date=datetime(2026, 3, 7, 1, 30, 0),
            timezone="America/Sao_Paulo",
        ),
    )
