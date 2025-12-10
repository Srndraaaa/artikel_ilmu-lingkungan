import os
import mysql.connector
import requests
import pytz
from datetime import datetime

# --- KONFIGURASI ---
# Mengambil kunci rahasia dari settingan GitHub (bukan ditulis disini)
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = "AQI" # Default nama DB di Aiven biasanya ini

# Setting API OpenWeather (Ganti dengan API KEY ASLI Anda disini gapapa, atau pakai Secrets juga lebih bagus)
API_KEY = "e3aef5cb30672427a9a263705ea84312"
CITY = "Semarang" 

def ambil_data():
    # 1. Request Data ke OpenWeather
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat=-6.9903988&lon=110.4229104&appid={API_KEY}"
    # (Pastikan lat/lon sesuai kota Anda)
    response = requests.get(url)
    data = response.json()

    # Cek jika error
    if response.status_code != 200:
        print("Gagal ambil data API")
        return

    # 2. Siapkan Waktu WIB
    wib = pytz.timezone('Asia/Jakarta')
    waktu_skrg = datetime.now(wib).strftime('%Y-%m-%d %H:%M:%S')

    # 3. Koneksi ke Database Aiven
    try:
        mydb = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            database=DB_NAME
        )
        cursor = mydb.cursor()

        # Pastikan tabel sudah dibuat. Kalau belum, script ini akan error.
        # Tips: Create table manual dulu lewat terminal Aiven atau script terpisah.
        
        sql = """INSERT INTO tb_kualitas_udara 
                 (city_name, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3, timestamp)
                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        comps = data['list'][0]['components']
        val = (
            CITY,
            data['list'][0]['main']['aqi'],
            comps['co'], comps['no'], comps['no2'], comps['o3'], 
            comps['so2'], comps['pm2_5'], comps['pm10'], comps['nh3'],
            waktu_skrg
        )

        cursor.execute(sql, val)
        mydb.commit()
        print(f"SUKSES! Data {CITY} disimpan jam {waktu_skrg}")
        
    except Exception as e:
        print(f"Error Database: {e}")

if __name__ == "__main__":

    ambil_data()
