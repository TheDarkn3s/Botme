import os
import logging
import json
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import schedule
import requests
from telegram import Bot
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# — DEBUG ENVIRONMENT —
logging.basicConfig(
    format='%(asctime)s %(levelname)s: %(message)s',
    level=logging.DEBUG
)
logging.debug("ENV KEYS: %r", list(os.environ.keys()))

# — Carga de .env —
load_dotenv()

# — Variables de entorno —
TOKEN       = os.environ['TELEGRAM_TOKEN']
CHAT_ID     = os.environ['TELEGRAM_CHAT_ID']
SPREADSHEET = os.environ['SPREADSHEET_URL']
CSV_PATH    = 'subscriber-list.csv'
MAPPING_SHEET    = os.getenv('MAPPING_SHEET_NAME', 'Mapping')
TWITCHDATA_SHEET = os.getenv('TWITCHDATA_SHEET_NAME', 'TwitchData')
SCHEDULE_TIME    = os.getenv('SCHEDULE_TIME', '00:00')

# Twitch API vars
TWITCH_CLIENT_ID     = os.environ['TWITCH_CLIENT_ID']
TWITCH_OAUTH_TOKEN   = os.environ['TWITCH_OAUTH_TOKEN']
TWITCH_BROADCASTER_ID= os.environ['TWITCH_BROADCASTER_ID']

# — Logger (reconfig to INFO for rest) —
logging.getLogger().setLevel(logging.INFO)

# — Inicializar bot de Telegram —
bot = Bot(token=TOKEN)

# — Autenticación Google Sheets —
service_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT_JSON'])
creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
gc = gspread.authorize(creds)
sh = gc.open_by_url(SPREADSHEET)

def fetch_subscribers():
    """Llama a la Twitch API y genera subscriber-list.csv"""
    url = 'https://api.twitch.tv/helix/subscriptions'
    headers = {
        'Client-ID': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {TWITCH_OAUTH_TOKEN}'
    }
    params = {
        'broadcaster_id': TWITCH_BROADCASTER_ID,
        'first': 100
    }
    all_data = []
    while True:
        resp = requests.get(url, headers=headers, params=params).json()
        data = resp.get('data', [])
        if not data:
            break
        all_data.extend(data)
        cursor = resp.get('pagination', {}).get('cursor')
        if cursor:
            params['after'] = cursor
        else:
            break

    df = pd.DataFrame([{
        'Username': sub['user_name'],
        'Subscribe Date': sub['created_at']
    } for sub in all_data])
    df.to_csv(CSV_PATH, index=False)
    logging.info(f"{len(df)} suscriptores escritos en {CSV_PATH}")

def check_subscriptions():
    """Lee Mapping + CSV, procesa expiraciones y envía alertas."""
    try:
        fetch_subscribers()

        ws_map = sh.worksheet(MAPPING_SHEET)
        df_map = pd.DataFrame(ws_map.get_all_records())
        df_map.columns = df_map.columns.str.strip().str.upper()
        df_map.rename(columns={
            'NOMBRE EN TWITCH': 'Username',
            'NOMBRE EN TELEGRAM': 'Telegram Username'
        }, inplace=True)

        df_twitch = pd.read_csv(CSV_PATH)
        df_twitch['Subscribe Date'] = pd.to_datetime(df_twitch['Subscribe Date'])

        df = pd.merge(df_twitch, df_map, on='Username', how='inner')
        if df.empty:
            logging.warning("No hay coincidencias entre CSV y Mapping.")
            return

        df['Expire Date'] = df['Subscribe Date'] + timedelta(days=30)
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        try:
            ws_data = sh.worksheet(TWITCHDATA_SHEET)
            ws_data.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws_data = sh.add_worksheet(
                title=TWITCHDATA_SHEET, rows="1000", cols="20"
            )
        ws_data.update([df.columns.tolist()] + df.values.tolist())
        logging.info("Hoja TwitchData actualizada.")

        for _, row in df.iterrows():
            days_left = (row['Expire Date'] - now).days
            tg_user = row['Telegram Username']

            if days_left <= 0:
                text = f"❌ @{tg_user}, SUSCRIPCIÓN CADUCADA"
            elif days_left <= 3:
                text = f"⚠️ @{tg_user}, VENCE EN {days_left} DÍAS"
            else:
                continue

            bot.send_message(chat_id=CHAT_ID, text=text)
            logging.info(f"Mensaje enviado a @{tg_user}")

    except Exception as e:
        logging.exception(f"Error en check_subscriptions: {e}")

if __name__ == "__main__":
    check_subscriptions()
    schedule.every().day.at(SCHEDULE_TIME).do(check_subscriptions)
    logging.info(f"Job diario programado a las {SCHEDULE_TIME} UTC")
    while True:
        schedule.run_pending()
        time.sleep(30)
