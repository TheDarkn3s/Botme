import os
import json
import base64
import time
import traceback
from datetime import datetime, timedelta, timezone

import pandas as pd
import schedule
import requests
from telegram import Bot
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# — Debug: Mostrar todas las variables de entorno —
print("🔍 ENV KEYS:", list(os.environ.keys()))

# Cargar .env local si existe
load_dotenv()
print("✅ .env cargado (si existe)")

# Cargar variables de entorno esenciales
try:
    TOKEN = os.environ['TELEGRAM_TOKEN']
    CHAT_ID = os.environ['TELEGRAM_CHAT_ID']
    SPREADSHEET_URL = os.environ['SPREADSHEET_URL']
    print("🔑 TELEGRAM_TOKEN, TELEGRAM_CHAT_ID y SPREADSHEET_URL cargados")

    TWITCH_CLIENT_ID = os.environ['TWITCH_CLIENT_ID']
    TWITCH_OAUTH_TOKEN = os.environ['TWITCH_OAUTH_TOKEN']
    TWITCH_BROADCASTER_ID = os.environ['TWITCH_BROADCASTER_ID']
    print("🎮 TWITCH_CLIENT_ID, TWITCH_OAUTH_TOKEN y TWITCH_BROADCASTER_ID cargados")

    # Cargar credenciales de Google Sheet codificadas en Base64
    B64 = os.environ.get('GOOGLE_SERVICE_ACCOUNT_JSON_B64')
    if not B64:
        raise KeyError('GOOGLE_SERVICE_ACCOUNT_JSON_B64')
    print("🔐 GOOGLE_SERVICE_ACCOUNT_JSON_B64 cargado")

    MAPPING_SHEET = os.getenv('MAPPING_SHEET_NAME', 'Mapping')
    TWITCHDATA_SHEET = os.getenv('TWITCHDATA_SHEET_NAME', 'TwitchData')
    SCHEDULE_TIME = os.getenv('SCHEDULE_TIME', '00:00')
    print(f"📋 Mapping='{MAPPING_SHEET}', TwitchData='{TWITCHDATA_SHEET}', Schedule='{SCHEDULE_TIME}'")
except KeyError as e:
    print(f"❌ Falta variable de entorno: {e}")
    raise

# Inicializar Bot de Telegram
echo = Bot(token=TOKEN)
print("🤖 Bot de Telegram inicializado")

# Autenticación con Google Sheets usando Base64
try:
    creds_json = base64.b64decode(B64).decode('utf-8')
    service_info = json.loads(creds_json)
    creds = Credentials.from_service_account_info(
        service_info,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    gc = gspread.authorize(creds)
    sh = gc.open_by_url(SPREADSHEET_URL)
    print("✅ Autenticado en Google Sheets")
except Exception:
    print("❌ Error autenticando en Google Sheets:")
    traceback.print_exc()
    raise

CSV_PATH = 'subscriber-list.csv'


def fetch_subscribers():
    print("🌐 Iniciando fetch_subscribers()")
    url = 'https://api.twitch.tv/helix/subscriptions'
    headers = {
        'Client-ID': TWITCH_CLIENT_ID,
        'Authorization': f'Bearer {TWITCH_OAUTH_TOKEN}'
    }
    params = {'broadcaster_id': TWITCH_BROADCASTER_ID, 'first': 100}
    all_data = []
    try:
        while True:
            resp = requests.get(url, headers=headers, params=params).json()
            data = resp.get('data', [])
            print(f"  📦 Recibidos {len(data)} registros")
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
            date_str = sub.get('created_at') or sub.get('gifted_at')
            if not date_str:
                date_str = datetime.now(timezone.utc).isoformat()
            rows.append({
                'Username': sub.get('user_name', ''),
                'Subscribe Date': date_str
            })

        df = pd.DataFrame(rows)
        df.to_csv(CSV_PATH, index=False)
        print(f"✅ {len(df)} suscriptores escritos en {CSV_PATH}")
    except Exception:
        print("❌ Error en fetch_subscribers():")
        traceback.print_exc()
        raise


def check_subscriptions():
    print("▶️ Iniciando check_subscriptions()")
    try:
        fetch_subscribers()

        print(f"🔍 Leyendo Mapping sheet '{MAPPING_SHEET}'")
        ws_map = sh.worksheet(MAPPING_SHEET)
        df_map = pd.DataFrame(ws_map.get_all_records())
        print(f"  🗺️ Mapping filas={len(df_map)}, cols={df_map.shape[1]}")
        df_map.columns = df_map.columns.str.strip().str.upper()
        df_map.rename(columns={
            'NOMBRE EN TWITCH': 'Username',
            'NOMBRE EN TELEGRAM': 'Telegram Username'
        }, inplace=True)

        print(f"🔍 Leyendo CSV '{CSV_PATH}'")
        df_twitch = pd.read_csv(CSV_PATH)
        print(f"  🐼 CSV filas={len(df_twitch)}, cols={df_twitch.shape[1]}")
        df_twitch['Subscribe Date'] = pd.to_datetime(df_twitch['Subscribe Date'])

        df = pd.merge(df_twitch, df_map, on='Username', how='inner')
        print(f"🔗 Merge filas={len(df)}")
        if df.empty:
            print("⚠️ No hay coincidencias entre CSV y Mapping.")
            return

        df['Expire Date'] = df['Subscribe Date'] + timedelta(days=30)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        print("⏳ Fechas de expiración calculadas")

        # Convertir fechas a string ISO
        df['Subscribe Date'] = df['Subscribe Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        df['Expire Date']   = df['Expire Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

        print(f"🔄 Actualizando sheet '{TWITCHDATA_SHEET}'")
        try:
            ws_data = sh.worksheet(TWITCHDATA_SHEET)
            ws_data.clear()
            print("  🗑️ Hoja limpia")
        except gspread.exceptions.WorksheetNotFound:
            ws_data = sh.add_worksheet(title=TWITCHDATA_SHEET, rows="1000", cols="20")
            print("  ➕ Hoja creada")
        ws_data.update([df.columns.tolist()] + df.astype(str).values.tolist())
        print("✅ TwitchData actualizado")

        print("✉️ Enviando alertas")
        sent = 0
        for _, row in df.iterrows():
            exp = datetime.fromisoformat(row['Expire Date'])
            days_left = (exp - now).days
            tg_user = row['Telegram Username']
            if days_left <= 0:
                text = f"❌ @{tg_user}, SUSCRIPCIÓN CADUCADA"
            elif days_left <= 3:
                text = f"⚠️ @{tg_user}, VENCE EN {days_left} DÍAS"
            else:
                continue
            bot.send_message(chat_id=CHAT_ID, text=text)
            print(f"  ✅ Mensaje enviado a @{tg_user}: '{text}'")
            sent += 1
        if sent == 0:
            print("  ℹ️ Ninguna suscripción a punto de expirar")
    except Exception:
        print("❌ Error en check_subscriptions():")
        traceback.print_exc()


if __name__ == "__main__":
    print("🚀 Bot arrancando")
    check_subscriptions()
    print(f"⏰ Programando tarea diaria a las {SCHEDULE_TIME} UTC")
    schedule.every().day.at(SCHEDULE_TIME).do(check_subscriptions)
    while True:
        schedule.run_pending()
        time.sleep(30)
