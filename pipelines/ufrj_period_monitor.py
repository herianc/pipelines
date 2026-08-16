from datetime import datetime, timedelta, date

import pytz
import requests
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.client.schemas.schedules import CronSchedule


WAHA_API_KEY = Secret.load("waha-api-key").get()
GROUP_ID = Secret.load("whatsapp-group-id").get()
GOOGLE_TRANSLATE_KEY = Secret.load("google-translate-key").get()
URL = "https://aurah.cloud/api/sendText/" 

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
    
    # Datas consultadas no Calendário Acadêmico da UFRJ 2026.2
    # Início e fim do período
    PERIOD_START = date(year=2026, month=8, day=10)
    PERIOD_END = date(year=2026, month=12, day=19)
    PERIOD_TOTAL = PERIOD_END - PERIOD_START

    # Período de inscrição
    ENROLLMENT_START = date(year=2026, month=7, day=24)
    ENROLLMENT_END = date(year=2026, month=8, day=3)
    ENROLLMENT_TOTAL = ENROLLMENT_END - ENROLLMENT_START

    # Período de alteração
    CHANGE_START = date(year=2026, month=8, day=10)
    CHANGE_END = date(year=2026, month=8, day=21)
    CHANGE_TOTAL = CHANGE_END - CHANGE_START

    # Período de trancamento
    WITHDRAW_START = date(year=2026, month=8, day=31)
    WITHDRAW_END = date(year=2026, month=9, day=10)
    WITHDRAW_TOTAL = WITHDRAW_END - WITHDRAW_START

    current_date = date.today()

    days_completed = current_date - PERIOD_START
    days_remaining = PERIOD_END - current_date

    return {
        "current_date": current_date,
        "start_date": PERIOD_START,
        "end_date": PERIOD_END,
        "enrollment_start": ENROLLMENT_START,
        "enrollment_end": ENROLLMENT_END,
        "change_start": CHANGE_START,
        "change_end": CHANGE_END,
        "withdraw_start": WITHDRAW_START,
        "withdraw_end": WITHDRAW_END, 
        "period_total": PERIOD_TOTAL.days,
        "days_completed": days_completed.days,
        "days_remaining": days_remaining.days,
        "percentual_completed": round(days_completed.days / PERIOD_TOTAL.days * 100, 2),
    }

@task 
def check_dates_and_format_message(dates: dict, translation: str):
    current_date = dates.get('current_date')
    lines = []
    notice = None


    # Período de inscrição
    if  current_date < dates.get('start_date'):
        enroll_end : date = dates.get('enrollment_end')
        start_date : date = dates.get('start_date')
        days_remaining_enroll = (enroll_end - current_date).days

        lines = [f"Estamos no período de inscrição até o dia {enroll_end.strftime('%d/%m/%y')}. Faltam {days_remaining_enroll} dias para encerrar as inscrições.\n"]

        if days_remaining_enroll == 0:
            lines = [f'🚨 Hoje é o último dia do período de inscrição de disciplinas!. As aulas começam no dia {start_date.strftime("%d/%m/%y")}.\n']

    # Verifica se estamos no período de alteração 
    if  dates.get('change_start') <= current_date <= dates.get('change_end'):
        change_end = dates.get('change_end')
        days_remaining_change = (change_end - current_date).days

        notice = f'Estamos no período de *alteração de disciplinas* até o dia {change_end.strftime("%d/%m/%y")}. Você tem mais {days_remaining_change} para alterar a grade.\n'
        if days_remaining_change == 0:
            notice = f'- 🚨 *Hoje é o último dia do período de alteração!*\n'

    # Verifica se estamos no período de trancamento
    if dates.get('withdraw_start') <= current_date <= dates.get('withdraw_end'):
        withdraw_end = dates.get('withdraw_end')
        days_remaining_withdraw = (withdraw_end - current_date).days

        notice = f'Estamos no período de trancamento de disciplinas até o dia {withdraw_end.strftime("%d/%m/%y")}. Você tem mais {days_remaining_withdraw} para trancar disciplinas.\n'
        if days_remaining_withdraw == 0:
            notice = f'- 🚨 *Hoje é o último dia do período de trancamento!*\n'

    if dates.get('start_date') <= current_date <= dates.get('end_date'):
        start_date = dates.get('start_date')
        end_date = dates.get('end_date')
        days_completed = dates.get('days_completed')
        percentual_completed = dates.get('percentual_completed')
        days_remaining_period = dates.get('days_remaining')

        lines = [
            f'🎓️ Período Letivo 2026.2',
            f'🗓️ Inicio: {start_date.strftime("%d/%m/%y")} - Fim: {end_date.strftime("%d/%m/%y")}\n',
            f"Você cursou `{days_completed}` dias ({percentual_completed}%) e ainda restam `{days_remaining_period}` dias para o fim do período.\n",
        ]

    # Mensagem final
    if notice:
        lines.append(notice)
    if translation:
        lines.append(f'> {translation}')
    message = '\n'.join(lines)
    
    return message



@task
def send_message(message):
    headers = {
        "X-api-Key": WAHA_API_KEY,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    payload = {
        "session": "default",
        "chatId": GROUP_ID,
        "text": message
    }

    response = requests.post(url=URL, json=payload, headers=headers)

    if response.status_code != 201:
        raise Exception(status_code= response.status_code, message=response.json())

@flow
def ufrj_period_monitor():
    # 1. Pega a frase do dia
    advice = get_advice()

    # 2. Faz a tradução pra português
    translation = translate(advice)

    # 3. Calcula o percentual completo
    percentual_completed = get_percentual_completed()

    # 4. Checa datas e formata a mensagem
    message = check_dates_and_format_message(percentual_completed, translation)

    # 5. Manda mensagem
    send_message(message)

if __name__ == "__main__":
    ufrj_period_monitor.from_source(
        source="https://github.com/herianc/pipelines.git", 
        entrypoint="pipelines/ufrj_period_monitor.py:ufrj_period_monitor"
    ).deploy(
        name="ufrj-period-monitor-managed",
        work_pool_name="default-work-pool",
        schedules=[CronSchedule(cron="0 19 * * 1-5", timezone="America/Sao_Paulo")], # Segunda a Sexta as 19hrs
        job_variables={"pip_packages": ["requests>=2.32.5"]},
    )