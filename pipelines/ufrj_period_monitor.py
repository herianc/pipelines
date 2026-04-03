from datetime import datetime, timedelta

import pytz
import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule

DISCORD_WEBHOOK_URL = Secret.load("ufrj-period-webhook").get()
GOOGLE_TRANSLATE_KEY = Secret.load("google-translate-key").get()

@task
def get_advice():
    response = requests.get("https://api.adviceslip.com/advice")
    if response.status_code == 200:
        data = response.json()
        advice = data.get("slip").get("advice")
        return advice
    else:
        raise Exception("API is currently unavailable")

@task
def translate(text):
    url = "https://translation.googleapis.com/language/translate/v2"
    params = {"q": text, "target": "pt-BR", "key": GOOGLE_TRANSLATE_KEY}
    response = requests.post(url, data=params)

    if response.status_code == 200:
        data = response.json()
        translation = data.get("data").get("translations")[0].get("translatedText")
        return translation
    else:
        return f"Erro: {response.status_code} - {response.text}"

@task
def get_percentual_completed():
    tz = pytz.timezone("America/Sao_Paulo")

    # Datas consultadas no Calendário Acadêmico da UFRJ 2026.1
    # https://graduação.ufrj.br/images/_PR-1/CEG/Calendario-Academico/2026/SEI_6138127_Calendario_academico_2026-CONSUNI.pdf
    PERIOD_START = datetime(year=2026, month=3, day=9, tzinfo=tz)
    PERIOD_END = datetime(year=2026, month=7, day=18, tzinfo=tz)
    PERIOD_TOTAL = PERIOD_END - PERIOD_START

    current_date = datetime.now(tz=tz)
    days_completed = current_date - PERIOD_START
    days_remaning = PERIOD_END - current_date

    return {
        "start_date": PERIOD_START.strftime("%d/%m/%Y"),
        "end_date": PERIOD_END.strftime("%d/%m/%Y"),
        "total": PERIOD_TOTAL.days,
        "days_completed": days_completed.days,
        "days_remaning": days_remaning.days,
        "percentual_completed": round(days_completed / PERIOD_TOTAL * 100, 2),
    }

@task
def send_message(content):
    message = (
        f"## 🎓️ Período Letivo 2026.1",
        f"🗓️ Início: {content['start_date']}  |  🗓️ Fim: {content['end_date']}  |  🗓️ Total: {content['total']} dias\n",
        f"Você cursou **{content['days_completed']} dias** ({content['percentual_completed']}%) e ainda restam **{content['days_remaning']} dias** para o fim do período.\n",
        f"> {content['translation']}",
    )
    message = "\n".join(message)

    payload = {
        "content": message,
        "username": "UFRJ Bot",
    }

    response = requests.post(DISCORD_WEBHOOK_URL, json=payload)

    if response.status_code == 204:
        print("Mensagem enviada com sucesso!")
    else:
        print(f"Erro ao enviar: {response.status_code}")

@flow
def ufrj_period_monitor():
    advice = get_advice()
    translation = translate(advice)
    percentual_completed = get_percentual_completed()
    send_message({**percentual_completed, "translation": translation})

if __name__ == "__main__":
    ufrj_period_monitor.from_source(
        source="https://github.com/herianc/pipelines.git", 
        entrypoint="pipelines/ufrj_period_monitor.py:ufrj_period_monitor"
    ).deploy(
        name="ufrj-period-monitor-managed",
        work_pool_name="default-work-pool",
        schedules=[CronSchedule(cron="0 19 * * 1-5", timezone="America/Sao_Paulo")],
        job_variables={"pip_packages": ["requests>=2.32.5"]},
    )