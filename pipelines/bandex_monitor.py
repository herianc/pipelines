from datetime import datetime, timedelta

import pandas as pd
import pytz
import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule

SHEET_ID = "1YvCqBrNw5l4EFNplmpRBFrFJpjl4EALlVNDk3pwp_dQ"
GID = "0"
SHEET_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"
)
DISCORD_WEBHOOK_URL = Secret.load("bandex-alert-channel").get()


@task
def get_semanal_menu():
    df = pd.read_csv(SHEET_URL, skiprows=2)

    days_of_week = [
        "Segunda-feira",
        "Terça-feira",
        "Quarta-feira",
        "Quinta-feira",
        "Sexta-feira",
        "Sábado",
        "Domingo",
    ]

    current_date = datetime.now(pytz.timezone("America/Sao_Paulo"))
    day_of_week = days_of_week[current_date.weekday()]
    column_day = current_date.weekday() + 1

    meal = "Almoço" if current_date.hour < 12 else "Janta"

    if meal == "Almoço":
        start, end = 0, 6
    else:
        start, end = 8, 15

    menu_list = df.iloc[start:end, column_day].to_list()
    menu_list = [item.strip().replace("  ", "") for item in menu_list]

    menu = {
        "entry": menu_list[0],
        "main_course": menu_list[1],
        "vegan_course": menu_list[2],
        "side_dish": menu_list[3],
        "accompaniment": menu_list[4],
        "dessert": menu_list[5],
    }

    return {
        "day_of_week": day_of_week,
        "meal": meal,
        "current_date": current_date.strftime("%d/%m/%Y"),
        "menu": menu,
    }


@task
def send_message(content):
    message = (
        f"# {content['day_of_week']} - {content['meal']} - {content['current_date']}",
        f"* 🥗 **Entrada**: {content['menu']['entry']}",
        f"* 🍲 **Prato Principal**: {content['menu']['main_course']}",
        f"* 🥦 **Prato Vegano**: {content['menu']['vegan_course']}",
        f"* 🥘 **Guarnição**: {content['menu']['side_dish']}",
        f"* 🍚 **Acompanhamentos**: {content['menu']['accompaniment']}",
        f"* 🍎 **Sobremesa**: {content['menu']['dessert']}",
    )
    message = "\n".join(message)

    payload = {
        "content": message,
        "username": "Bandex Bot",
    }

    response = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    if response.status_code == 204:
        print("Mensagem enviada com sucesso!")
    else:
        print(f"Erro ao enviar: {response.status_code}")


@flow
def bandex_alert():
    content = get_semanal_menu()
    send_message(content, wait_for=[content])


if __name__ == "__main__":
    bandex_alert.from_source(
        source="https://github.com/herianc/pipelines.git",
        entrypoint="pipelines/main.py:bandex_alert",
    ).deploy(
        name="bandex-alert",
        work_pool_name="default-work-pool",
        schedules=[
            CronSchedule(cron="0 8 * * 1-5", timezone="America/Sao_Paulo"),
            CronSchedule(cron="0 14 * * 1-5", timezone="America/Sao_Paulo"),
        ],
        job_variables={"pip_packages": ["pandas>=3.0.2"]},
    )
