import pandas as pd
from kafka import KafkaProducer
import time
import json


def send_to_kafka_dailyByDaily(producer, topic, dataframe):
    messages = []
    for index, row in dataframe.iterrows():
        message = row.to_json().encode('utf-8')
        messages.append(message)
    
    # Invia tutti i messaggi insieme
    for message in messages:
        producer.send(topic, message)
        #time.sleep(0.1)  # Sleep per 10 millisecondi tra i messaggi per ridurre la velocità di invio

        #time.sleep(0.01)  # Sleep per 10 millisecondi tra i messaggi per ridurre la velocità di invio

    
    producer.flush()
    time.sleep(20) # Sleep per 20 secondi tra i messaggi per ridurre la velocità di invio e simulare la distanza tra i giorni
   

# Percorso del file CSV
csv_file = './raw_data_medium-utv_sorted.csv'

# Specifica i tipi di dati per ogni colonna come stringhe
dtype_dict = {
    'date': str,
    'serial_number': str,
    'model': str,
    'failure': str,
    'vault_id': str,
    's1_read_error_rate': str,
    's2_throughput_performance': str,
    's3_spin_up_time': str,
    's4_start_stop_count': str,
    's5_reallocated_sector_count': str,
    's7_seek_error_rate': str,
    's8_seek_time_performance': str,
    's9_power_on_hours': str,
    's10_spin_retry_count': str,
    's12_power_cycle_count': str,
    's173_wear_leveling_count': str,
    's174_unexpected_power_loss_count': str,
    's183_sata_downshift_count': str,
    's187_reported_uncorrectable_errors': str,
    's188_command_timeout': str,
    's189_high_fly_writes': str,
    's190_airflow_temperature_cel': str,
    's191_g_sense_error_rate': str,
    's192_power_off_retract_count': str,
    's193_load_unload_cycle_count': str,
    's194_temperature_celsius': str,
    's195_hardware_ecc_recovered': str,
    's196_reallocated_event_count': str,
    's197_current_pending_sector': str,
    's198_offline_uncorrectable': str,
    's199_udma_crc_error_count': str,
    's200_multi_zone_error_rate': str,
    's220_disk_shift': str,
    's222_loaded_hours': str,
    's223_load_retry_count': str,
    's226_load_in_time': str,
    's240_head_flying_hours': str,
    's241_total_lbas_written': str,
    's242_total_lbas_read': str
}

print("Inizio l'elaborazione del file CSV...")
# Leggi il CSV con tutti i campi come stringhe
df = pd.read_csv(csv_file, dtype=dtype_dict, low_memory=False)

print("Conversione dei tipi di dati...")
try:
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce')
except Exception as e:
    print(f"Errore nella conversione della colonna 'date': {e}")

# Filtra le righe con date valide
df = df.dropna(subset=['date'])


print("Invio dei dati a Kafka...")
# Crea un produttore Kafka
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    request_timeout_ms=120000,  # Aumenta il timeout della richiesta se necessario
    acks=0,  # Disabilita l'ack
    batch_size=16384,  # Riduci la dimensione del batch (default è 16384 byte)
    max_block_ms=60000  # Aumenta il tempo massimo di blocco a 60 secondi
)

# start_date = pd.to_datetime('2023-04-22T00:00:00.000000')
# end_date = pd.to_datetime('2023-04-25T00:00:00.000000')
# df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)]

print("Ottenimento dei giorni unici...")
# Ottiene l'elenco dei giorni unici
unique_days = df['date'].dt.date.unique()
#unique_days = df_filtered['date'].dt.date.unique()

# Scrive i giorni unici su un file di testo
with open('unique_days.txt', 'w') as file:
    for day in unique_days:
        file.write(f"{day}\n")

print("Preparo dati a Kafka per ciascun giorno...")
# Crea dataframe, uno per ciascun giorno
day_dataframes = [df[df['date'].dt.date == day] for day in unique_days]
#day_dataframes = [df_filtered[df_filtered['date'].dt.date == day] for day in unique_days[:4]]

print("Invio dei dati a Kafka...")
# Invia i dataframe a Kafka, uno per volta
topic = 'my-topic'
# for daily_df in day_dataframes:
#     send_to_kafka(producer, topic, daily_df)

for daily_df in day_dataframes:
    send_to_kafka_dailyByDaily(producer, topic, daily_df)

# Chiude il produttore Kafka
producer.close()