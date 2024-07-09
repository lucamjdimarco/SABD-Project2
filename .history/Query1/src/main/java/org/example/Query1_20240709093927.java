package org.example;

import org.apache.commons.text.ExtendedMessageFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.runtime.webmonitor.stats.Statistics;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.units.qual.s;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class Query1 {

    private static LocalDateTime parseDate(String dateString) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        return LocalDateTime.parse(dateString, formatter);
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("my-topic")
                .setGroupId("flink-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        var src = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        var dataStream = src
                .map((MapFunction<String, Message>) Message::create)
                .name("Map to Message")
                .setParallelism(2)
                .filter(Objects::nonNull)
                .name("Filter non-null messages")
                .setParallelism(2)
                .filter(message -> {
                    int vaultId = Integer.parseInt(message.getVaultId());
                    return vaultId >= 1000 && vaultId <= 1020;
                })
                .name("Filter by vaultId")
                .setParallelism(2)
                .map((MapFunction<Message, Tuple2<Long, Message>>) value ->
                        Tuple2.of(value.getDate(), value))
                .returns(Types.TUPLE(Types.LONG, Types.GENERIC(Message.class)))
                .name("Map to Tuple2")
                .setParallelism(2)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<Long, Message>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<Long, Message>>) (element, recordTimestamp) -> element.f0)
                );

        // Finestra di 1 giorno
        processWindow(dataStream, Time.days(1), "output_1_day.csv", "latency_1_day.csv");

        // Finestra di 3 giorni
        processWindow(dataStream, Time.days(3), "output_3_days.csv", "latency_3_days.csv");

        // Finestra di 23 giorni
        processWindow(dataStream, Time.days(23), "output_23_days.csv", "latency_23_days.csv");

        env.execute("Kafka Connector Demo");
    }

    private static void processWindow(DataStream<Tuple2<Long, Message>> dataStream, Time windowTime, String outputPath, String latencyPath) {

        Time offset;
        if (outputPath.equals("output_1_day.csv")) {
            offset = Time.days(0);
        } else if (outputPath.equals("output_3_days.csv")) {
            offset = Time.days(2);
        } else if (outputPath.equals("output_23_days.csv")) {
            offset = Time.days(13);
        } else {
            offset = Time.days(0);
        }

        dataStream
                .windowAll(TumblingEventTimeWindows.of(windowTime, offset))
                .process(new VaultStatisticsProcessAllWindowFunction(latencyPath)) 
                .name("Process All Window " + windowTime.toString())
                .setParallelism(1)
                .writeUsingOutputFormat(new CustomCsvOutputFormat(outputPath))
                .name("Write to CSV File " + windowTime.toString());
    }

    public static class Message {
        private String serial_number;
        private String model;
        private String failure;
        private String vault_id;
        private Double s9_power_on_hours;
        private String s194_temperature_celsius;
        private Long date;
        private Long ingressTimestamp;
        private Long processingEndTime;

        public Message(Long date, String serial_number, String model, String failure, String vault_id, Double s9_power_on_hours, String s194_temperature_celsius ) {
            this.serial_number = serial_number;
            this.model = model;
            this.failure = failure;
            this.vault_id = vault_id;
            this.s9_power_on_hours = s9_power_on_hours;
            this.s194_temperature_celsius = s194_temperature_celsius;
            this.date = date;
            this.ingressTimestamp = System.currentTimeMillis();
        }

        public static Message create(String rawMessage) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                JsonNode root = mapper.readTree(rawMessage);

                Long date = root.path("date").asLong();
                String serialNumber = root.path("serial_number").asText();
                String model = root.path("model").asText();
                String failure = root.path("failure").asText();
                String vaultId = root.path("vault_id").asText();
                double s9PowerOnHours = root.path("s9_power_on_hours").asDouble();
                String s194TemperatureCelsius = root.path("s194_temperature_celsius").asText();

                return new Message(date, serialNumber, model, failure, vaultId, s9PowerOnHours, s194TemperatureCelsius);
            } catch (Exception e) {
                System.out.println("Errore di deserializzazione del JSON: " + e.getMessage());
                return null;
            }
        }

        public String getSerialNumber() {
            return serial_number;
        }

        public String getModel() {
            return model;
        }

        public String getFailure() {
            return failure;
        }

        public String getVaultId() {
            return vault_id;
        }

        public Double getS9PowerOnHours() {
            return s9_power_on_hours;
        }

        public String getS194TemperatureCelsius() {
            return s194_temperature_celsius;
        }

        public Long getDate() {
            return date;
        }

        public Long getIngressTimestamp() {
            return ingressTimestamp;
        }

        public Long getProcessingEndTime() {
            return processingEndTime;
        }

        public void setProcessingEndTime(Long processingEndTime) {
            this.processingEndTime = processingEndTime;
        }
    }

    public static class VaultStatisticsProcessAllWindowFunction extends ProcessAllWindowFunction<
            Tuple2<Long, Message>, String, TimeWindow> {

        private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        private static final ObjectMapper objectMapper = new ObjectMapper(); // ObjectMapper per calcolare la dimensione dei messaggi


        private static LocalDateTime millisToDateTime(long millis) {
            Instant instant = Instant.ofEpochMilli(millis);
            return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        }

        private final String latencyPath;

        public VaultStatisticsProcessAllWindowFunction(String latencyPath) {
            this.latencyPath = latencyPath;
        }

        @Override
        public void process(Context context, Iterable<Tuple2<Long, Message>> elements, Collector<String> out) throws Exception {
            Map<String, Statistics> statisticsMap = new HashMap<>();
            long startTime = System.nanoTime(); //inizializza il cronometro per il calcolo della latenza
            int messageCount = 0;
            long totalBytes = 0;
            Path latencyFilePath = Paths.get(this.latencyPath);
                    try {
                        Files.createFile(latencyFilePath);
                    } catch (FileAlreadyExistsException e) {
                        
                    }

            for (Tuple2<Long, Message> element : elements) {
                messageCount++;
                String vaultId = element.f1.getVaultId();
                String temperatureStr = element.f1.getS194TemperatureCelsius();

                 // Calcolo dimensione in byte del messaggio
                String messageJson = objectMapper.writeValueAsString(element.f1);
                totalBytes += messageJson.getBytes().length;

                try {
                    if (temperatureStr != null && !temperatureStr.isEmpty() && !temperatureStr.equals("0.0")) {
                        double temperature = Double.parseDouble(temperatureStr);
                        Statistics stats = statisticsMap.getOrDefault(vaultId, new Statistics());
                        stats.count++;
                        double delta = temperature - stats.mean;
                        stats.mean += delta / stats.count;
                        double delta2 = temperature - stats.mean;
                        stats.sumOfSquares += delta * delta2;
                        statisticsMap.put(vaultId, stats);
                    }
                    element.f1.setProcessingEndTime(System.currentTimeMillis());
                    //long latency = element.f1.getProcessingEndTime() - element.f1.getIngressTimestamp();
                    //String latencyRecord = millisToDateTime(context.window().getStart()).format(FORMATTER) + "," + latency + "\n";
                    //Files.write(latencyFilePath, latencyRecord.getBytes(), StandardOpenOption.APPEND);
                } catch (NumberFormatException e) {
                    System.err.println("Errore durante il parsing della temperatura: " + temperatureStr);
                }
            }

            long windowStart = context.window().getStart();
            LocalDateTime dateTime = millisToDateTime(windowStart);
            String formattedDate = dateTime.format(FORMATTER);

            long windowEnd = context.window().getEnd();
            LocalDateTime dateTimeEnd = millisToDateTime(windowEnd);
            String formattedDateEnd = dateTimeEnd.format(FORMATTER);

            for (Map.Entry<String, Statistics> entry : statisticsMap.entrySet()) {
                String vaultId = entry.getKey();
                Statistics stats = entry.getValue();
                double variance = stats.count > 1 ? stats.sumOfSquares / (stats.count - 1) : 0.0;
                double stddev = Math.sqrt(variance);

                String result = String.format("%s, %s, %s, %d, %.2f, %.2f",
                        formattedDate, formattedDateEnd, vaultId, stats.count, stats.mean, stddev);
                out.collect(result);
            }

            long endTime = System.nanoTime();
            long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime); // Calcolo della durata in millisecondi
            // Calcolo del throughput in messaggi al secondo
            double throughputMessages = (double) messageCount / (duration / 1000.0);

            // Calcolo del throughput in byte al secondo
            double throughputBytes = (double) totalBytes / (duration / 1000.0);
            // Registrazione della durata e del throughput
            String durationRecord = String.format("Window: %s to %s, Duration: %d ms, Throughput: %.2f messages/sec, %.2f bytes/sec\n",
                formattedDate, formattedDateEnd, duration, throughputMessages, throughputBytes);
            Files.write(latencyFilePath, durationRecord.getBytes(), StandardOpenOption.APPEND);

        }
    }

    public static class Statistics {
        public int count = 0;
        public double sumOfSquares = 0.0;
        public double mean = 0.0;
    }

    public static class CustomCsvOutputFormat implements OutputFormat<String> {
        private final String filePath;

        public CustomCsvOutputFormat(String filePath) {
            this.filePath = filePath;
        }

        @Override
        public void configure(Configuration parameters) {
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            File file = new File(filePath);
            if (taskNumber == 0 && file.exists()) {
                file.delete();
            }
            if (!file.exists()) {
                file.createNewFile();
            }
        }

        @Override
        public void writeRecord(String record) throws IOException {
            Files.write(Paths.get(filePath), (record + "\n").getBytes(), StandardOpenOption.APPEND);
        }

        @Override
        public void close() throws IOException {
        }
    }

    // public static class LatencyCalculatorAndLoggerMapFunction extends RichMapFunction<Tuple2<Long, Message>, Tuple2<Long, Message>> {
    //     private final String latencyFilePath;
    
    //     public LatencyCalculatorAndLoggerMapFunction(String latencyFilePath) {
    //         this.latencyFilePath = latencyFilePath;
    //     }
    
    //     @Override
    //     public void open(Configuration parameters) throws Exception {
    //         java.nio.file.Path latencyPath = java.nio.file.Paths.get(latencyFilePath);
    //         if (!java.nio.file.Files.exists(latencyPath)) {
    //             java.nio.file.Files.createFile(latencyPath);
    //         }
    //     }
    
    //     @Override
    //     public Tuple2<Long, Message> map(Tuple2<Long, Message> value) throws Exception {
    //         long currentTime = System.currentTimeMillis();
    //         long latency = currentTime - value.f1.getIngressTimestamp();
    //         String latencyRecord = currentTime + "," + latency + "\n";  // Usa il timestamp corrente
    //         java.nio.file.Files.write(java.nio.file.Paths.get(latencyFilePath), latencyRecord.getBytes(), java.nio.file.StandardOpenOption.APPEND);
    //         return value;
    //     }
    // }
    

}
