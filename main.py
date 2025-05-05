import os
import json
import base64
import traceback
from datetime import datetime, timedelta, timezone, time as dtime

import pandas as pd
import requests
from telegram import Bot, Update
from telegram.ext import Updater, CommandHandler, CallbackContext
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv
import pytz

# Debug ENV
print("🔍 ENV KEYS:", list(os.environ.keys()))
load_dotenv()
print("✅ .env cargado (si existe)")

# Load env vars
TOKEN = os.environ['TELEGRAM_TOKEN']
CHAT_ID = os.environ['TELEGRAM_CHAT_ID']
SPREADSHEET_URL = os.environ['SPREADSHEET_URL']
TWITCH_CLIENT_ID = os.environ['TWITCH_CLIENT_ID']
TWITCH_OAUTH_TOKEN = os.environ['TWITCH_OAUTH_TOKEN']
TWITCH_BROADCASTER_ID = os.environ['TWITCH_BROADCASTER_ID']
B64 = os.environ['GOOGLE_SERVICE_ACCOUNT_JSON_B64']
MAPPING_SHEET = os.getenv('MAPPING_SHEET_NAME', 'Mapping')
TWITCHDATA_SHEET = os.getenv('TWITCHDATA_SHEET_NAME', 'TwitchData')
SCHEDULE_TIME = os.getenv('SCHEDULE_TIME', '00:00')
print(f"⚙️ Configurado: schedule={SCHEDULE_TIME}, mapping={MAPPING_SHEET}, data={TWITCHDATA_SHEET}")

# Google Sheets auth
try:
    creds_json = base64.b64decode(B64).decode('utf-8')
    service_info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        service_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SPREADSHEET_URL)
    print("✅ Google Sheets autenticado")
except Exception:
    print("❌ Error autenticando Sheets:")
    traceback.print_exc()
    raise

CSV_PATH = 'subscriber-list.csv'

# Fetch subscribers
def fetch_subscribers() -> pd.DataFrame:
    print("🌐 Fetching subscribers from Twitch…")
    url = 'https://api.twitch.tv/helix/subscriptions'
    headers = {'Client-ID': TWITCH_CLIENT_ID, 'Authorization': f'Bearer {TWITCH_OAUTH_TOKEN}'}
    params = {'broadcaster_id': TWITCH_BROADCASTER_ID, 'first': 100}
    all_data = []
    while True:
        resp = requests.get(url, headers=headers, params=params).json()
        data = resp.get('data', [])
        print(f"  Recibidos {len(data)} registros")
        if not data:
            break
        all_data.extend(data)
        cursor = resp.get('pagination', {}).get('cursor')
        if cursor:
            params['after'] = cursor
        else:
            break
    rows = []
    for sub in all_data:
        date_str = sub.get('created_at') or sub.get('gifted_at') or datetime.now(timezone.utc).isoformat()
        rows.append({'Username': sub.get('user_name', ''), 'Subscribe Date': date_str})
    df = pd.DataFrame(rows, columns=['Username', 'Subscribe Date'])
    df.to_csv(CSV_PATH, index=False)
    print(f"✅ CSV escrito: {len(df)} filas")
    return df

# Subscription check
def check_subscriptions(context: CallbackContext = None):
    print("▶️ Running subscription check…")
    try:
        fetch_subscribers()
        try:
            df_twitch = pd.read_csv(CSV_PATH)
        except pd.errors.EmptyDataError:
            print("⚠️ CSV vacío, saltando check")
            return
        df_twitch['Subscribe Date'] = pd.to_datetime(df_twitch['Subscribe Date'])
        ws_map = sh.worksheet(MAPPING_SHEET)
        df_map = pd.DataFrame(ws_map.get_all_records())
        df_map.columns = df_map.columns.str.strip().str.upper()
        df_map.rename(columns={'NOMBRE EN TWITCH':'Username','NOMBRE EN TELEGRAM':'Telegram Username'}, inplace=True)
        df = pd.merge(df_twitch, df_map, on='Username', how='inner')
        if df.empty:
            print("⚠️ No matches found")
            return
        df['Expire Date'] = df['Subscribe Date'] + timedelta(days=30)
        now = datetime.now(timezone.utc)
        # Update sheet
        try:
            ws_data = sh.worksheet(TWITCHDATA_SHEET)
            ws_data.clear()
        except gspread.exceptions.WorksheetNotFound:
            ws_data = sh.add_worksheet(title=TWITCHDATA_SHEET, rows="1000", cols="20")
        df_upload = df.copy()
        df_upload['Subscribe Date'] = df_upload['Subscribe Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df_upload['Expire Date'] = df_upload['Expire Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        ws_data.update([df_upload.columns.tolist()] + df_upload.values.tolist())
        print("✅ TwitchData actualizado")
        # Send alerts
        sent=0
        for _, row in df.iterrows():
            exp = datetime.fromisoformat(row['Expire Date'].replace('Z', '+00:00'))
            days_left = (exp - now).days
            tg = row['Telegram Username']
            if days_left <= 0:
                msg = f"❌ @{tg}, SUSCRIPCIÓN CADUCADA"
            elif days_left <= 3:
                msg = f"⚠️ @{tg}, VENCE EN {days_left} DÍAS"
            else:
                continue
            Bot(token=TOKEN).send_message(chat_id=CHAT_ID, text=msg)
            print(f"  Sent @{tg}: {msg}")
            sent += 1
        if sent == 0:
            print("ℹ️ No alerts sent")
    except Exception:
        print("❌ Error en check_subscriptions:")
        traceback.print_exc()

# Command handler

def start(update: Update, context: CallbackContext):
    update.message.reply_text("¡Hola! Bot activo. Revisaré suscripciones.")

# Setup bot
updater = Updater(token=TOKEN)
updater.dispatcher.add_handler(CommandHandler('start', start))

# Initial run
check_subscriptions()

# Schedule daily job
hh, mm = map(int, SCHEDULE_TIME.split(':'))
job_time = dtime(hour=hh, minute=mm)
updater.job_queue.run_daily(check_subscriptions, time=job_time, context=None, days=(0,1,2,3,4,5,6), timezone=pytz.UTC)
print(f"⏰ Scheduled daily check at {SCHEDULE_TIME} UTC with pytz")

# Start polling
print("🤖 Bot polling…")
updater.start_polling()
updater.idle()
