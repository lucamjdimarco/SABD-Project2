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
Per la gestione corretta delle finestre di Flink, si è reso necessario di apportare modifiche al dataset: nello specifico l'inserimento di alcune tuple di controllo. Per eseguire il setup corretto, nella macchina host eseguire il file python denominato ```concat.py``` nella stessa cartella dove presente il dataset. Verranno inserite delle tuple fittizie utili al controllo delle finestre iniziali e finali.

## Run del cluster Docker
Una volta creato il JAR della query di interesse, muoversi all'interno della cartella principale del progetto, dove presente il ```docker-compose.yml```. Per lanciare il cluster Docker eseguire il comando: ```docker-compose up -d```. 

L'ambiente richiede circa 1 minuto di attesa per il setup completo. Lo start del cluster avvierà automaticamente anche il file python denominato ```ingestion.py``` all'interno del container Python per la simulazione dell'inoltro streaming del dataset. Il topic su Kafka verrà creato automaticamente. 

## Caricamente dei JAR in Flink

Durante l'attesa di setup del cluster, è possibile caricare il JAR in Flink. Di seguito vengono elencati i comandi per ogni JAR di ogni query:

- Query1: ```docker cp FlinkKafka/target/FlinkKafka-1.0-SNAPSHOT.jar flink-jobmanager:/job.jar```

- Query2: ```docker cp Query2/target/Query2-1.0-SNAPSHOT.jar flink-jobmanager:/job.jar ```

- Query3: ```docker cp Query3/target/Query3-1.0-SNAPSHOT.jar flink-jobmanager:/job.jar```

Successivamemte, atteso il minuto di setup, è possibile avviare il Job con il seguente comando:
```bash
  docker exec -it flink-jobmanager flink run /job.jar
```

## Interazione con i componenti del sistema
Successivamente al tempo di setup del cluster è possibile interagire correttamente con i componenti del sistema, nello specifico:

- Flink: UI Web accedibile all'indirizzo ```localhost:8081```
- Kafdrop UI Web accedibile all'indirizzo ```localhost:9000```
