import pandas as pd
from kafka import KafkaProducer
import time
import json

# Funzione per inviare i dati a Kafka
def send_to_kafka(producer, topic, dataframe):
    for index, row in dataframe.iterrows():
        #print(f"Invio della riga {index}...")
        message = row.to_json().encode('utf-8')
        #print(f"Message: {message}")
        producer.send(topic, message)
        producer.flush()
        time.sleep(0.0000000001)  # Sleep per simulare lo streaming lento

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
    time.sleep(10)
    # batch_size = 1000
    # num_batches = (len(dataframe) + batch_size - 1) // batch_size

    # for batch_num in range(num_batches):
    #     start_index = batch_num * batch_size
    #     end_index = min(start_index + batch_size, len(dataframe))
    #     batch_df = dataframe.iloc[start_index:end_index]
    #     message = batch_df.to_json(orient='records').encode('utf-8')
    #     producer.send(topic, message)
    #     producer.flush()
    #     time.sleep(0.000000001)  # Sleep per simulare lo streaming lento

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

#Aggiungi righe con date specifiche e valori nulli
# new_dates = ['2023-04-24T00:00:00.000000', '2023-04-25T00:00:00.000000']
# new_rows = []
# for new_date in new_dates:
#     #new_row = {col: None for col in df.columns}
#     new_row = {col: '0' for col in df.columns}  # Imposta tutti i campi a '0'
#     new_row['date'] = pd.to_datetime(new_date, format='%Y-%m-%dT%H:%M:%S.%f', errors='coerce')
#     new_rows.append(new_row)
    #df = df.append(new_row, ignore_index=True)

# new_df = pd.DataFrame(new_rows)
# df = pd.concat([df, new_df], ignore_index=True)

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