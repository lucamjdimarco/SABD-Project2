# SABD-Project2
## Build dei JAR delle query

Per creare i JAR da inserire in Flink per la creazione del job:
- Muoversi nella cartella di interesse, come ```Query1```, ```Query2``` o ```Query3```;
- Creare il pacchetto JAR con l'ausilio di Maven, con il seguente comando (richiede Maven installato sulla macchina in cui si compila il sorgente):

```bash
  mvn clean package
```
A questo punto il JAR sarà nella cartella ```target``` della cartella di interesse.

## Modifica del CSV con tuple di controllo
Per la gestione corretta delle finestre di Flink, si è reso necessario di apportare modifiche al dataset: nello specifico l'inserimento di alcune tuple di controllo. Per eseguire il setup corretto, nella macchina host eseguire il file python denominato ```concat.py``` nella stessa cartella dove presente il dataset. Verranno inserite delle tuple fittizie utili al controllo delle finestre finali.

## Run del cluster Docker
Una volta creato il JAR della query di interesse, muoversi all'interno della cartella principale del progetto, dove presente il ```docker-compose.yml```. Per lanciare il cluster Docker eseguire il comando: ```docker-compose up -d```. 

L'ambiente richiede circa 1 minuto di attesa per il setup completo. Lo start del cluster avvierà automaticamente anche il file python denominato ```ingestion.py``` all'interno del container Python per la simulazione dell'inoltro streaming del dataset. Il topic su Kafka verrà creato automaticamente. 

## Caricamente dei JAR in Flink

Durante l'attesa di setup del cluster, è possibile caricare il JAR in Flink. Di seguito vengono elencati i comandi per ogni JAR di ogni query:

- Query1:
```bash
  docker cp Query1/target/Query1-1.0-SNAPSHOT.jar flink-jobmanager:/job.jar
```

- Query2:
```bash
  docker cp Query2/target/Query2-1.0-SNAPSHOT.jar flink-jobmanager:/job.jar
```

- Query3:
```bash
  docker cp Query3/target/Query3-1.0-SNAPSHOT.jar flink-jobmanager:/job.jar
```

Successivamemte, atteso il minuto di setup, è possibile avviare il Job con il seguente comando:
```bash
  docker exec -it flink-jobmanager flink run /job.jar
```
Se presenta errori come il topic non creato, attendere ancora qualche secondo e riprovare il comando indicato precedentemente, il setup dell'ambiente varia da macchina host a macchina host.

## Interazione con i componenti del sistema
Successivamente al tempo di setup del cluster è possibile interagire correttamente con i componenti del sistema, nello specifico:

- Flink: UI Web accedibile all'indirizzo ```localhost:8081```
- Kafdrop UI Web accedibile all'indirizzo ```localhost:9000```

## Chiusura del cluster e interruzione
Per eseguire la chiusura e l'eliminazione del cluster, eseguire il comando ```docker-compose down```.

## Recupero dei risultati
Per il recupero dei risultati a seguito dell'elaborazione di Flink eseguire i seguenti comandi:
- Query1:
```bash
  docker cp flink-taskmanager:/opt/flink/output_1_day.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/output_3_days.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/output_23_days.csv ./
```

- Query2:
```bash
  docker cp flink-taskmanager:/opt/flink/daily_top10.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/three_day_top10.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/all_day_top10.csv ./
```

- Query3:
```bash
  docker cp flink-taskmanager:/opt/flink/one_day_stats.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/three_day_stats.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/all_time_stats.csv ./
```

## Recupero delle metriche
Per il recupero delle metriche di latenza a seguito dell'elaborazione di Flink eseguire i seguenti comandi:
- Query1:
```bash
  docker cp flink-taskmanager:/opt/flink/latency_1_day.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/latency_3_days.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/latency_23_days.csv ./
```

- Query2:
```bash
  docker cp flink-taskmanager:/opt/flink/daily_latency.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/three_day_latency.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/all_days_latency.csv ./
```

- Query3:
```bash
  docker cp flink-taskmanager:/opt/flink/daily_latency.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/three_day_latency.csv ./
```
```bash
  docker cp flink-taskmanager:/opt/flink/all_time_latency.csv ./
```
