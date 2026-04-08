
from datetime import datetime, timedelta

import pytz
import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule

DISCORD_WEBHOOK_URL = Secret.load("responde-ai-webhook").get()


@task
def send_reminder():
    message = (
        f"### Lembrete de Pagamento 🧾️",
        f"Amigos, este é um lembrete amigável do pagamento. ✨\n",
        f"**Valor**: R$ 10,00",
        f"**Código PIX**:",
    )
    message = "\n".join(message)

    payload = {
        "content": message,
        "username": "Seu Barriga",
    }

    response = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    if response.status_code == 204:
        print("Mensagem enviada com sucesso!")
    else:
        print(f"Erro ao enviar: {response.status_code}")

@task
def send_pix_code():
    message = f"n00020101021126330014br.gov.bcb.pix011113350669778520400005303986540510.005802BR5919HERIAN G CAVALCANTE6013RIO DE JANEIR62070503***6304CC09\n"

    payload = {
        "content": message,
        "username": "Seu Barriga",
    }

    response = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    if response.status_code == 204:
        print("Mensagem enviada com sucesso!")
    else:
        print(f"Erro ao enviar: {response.status_code}")

@flow 
def responde_ai_reminder():
    reminder = send_reminder()
    send_pix_code(wait_for=reminder)

if __name__ == "__main__":
    responde_ai_reminder.from_source(
        source="https://github.com/herianc/pipelines.git", 
        entrypoint="pipelines/responde_ai_reminder.py:responde_ai_reminder"
    ).deploy(
        name="responde-ai-reminder",
        work_pool_name="default-work-pool",
        schedules=[CronSchedule(cron="0 17 10 * *", timezone="America/Sao_Paulo")],
        job_variables={"pip_packages": ["requests>=2.32.5"]},
    )