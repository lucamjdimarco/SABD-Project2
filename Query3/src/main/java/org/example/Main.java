package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tdunning.math.stats.TDigest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Define Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "my-topic",
                new SimpleStringSchema(),
                getKafkaProperties()
        );

        // Configure watermark strategy based on the date field
        WatermarkStrategy<Message> watermarkStrategy = WatermarkStrategy
                .<Message>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((SerializableTimestampAssigner<Message>) (event, timestamp) -> event.getDate());

        DataStream<String> rawStream = env.addSource(consumer);

        // Parse the raw JSON records into Message objects
        DataStream<Message> messageStream = rawStream
                //.map(Message::create)
                .map(new IngressTimestampMapFunction())
                .filter(message -> message != null && message.getVaultId() != null)
                .assignTimestampsAndWatermarks(watermarkStrategy);

        // Filter the stream for vault IDs between 1090 and 1120
        DataStream<Message> filteredStream = messageStream
                .filter(message -> {
                    try {
                        int vaultId = Integer.parseInt(message.getVaultId());
                        return vaultId >= 1090 && vaultId <= 1120;
                    } catch (NumberFormatException e) {
                        return false;
                    }
                });

        // Calculate statistics in 1-day, 3-day, and full dataset windows
        DataStream<String> oneDayStats = calculateStats(filteredStream, Time.days(1), "daily_latency.csv");
        DataStream<String> threeDayStats = calculateStats(filteredStream, Time.days(3), "three_day_latency.csv");
        DataStream<String> allTimeStats = calculateStats(filteredStream, Time.days(23), "all_time_latency.csv");

        // Write results to CSV files
        oneDayStats.addSink(new OutputFormatSinkFunction<>(new CustomCsvOutputFormat("one_day_stats.csv")));
        threeDayStats.addSink(new OutputFormatSinkFunction<>(new CustomCsvOutputFormat("three_day_stats.csv")));
        allTimeStats.addSink(new OutputFormatSinkFunction<>(new CustomCsvOutputFormat("all_time_stats.csv")));

        env.execute("Disk Failure Statistics");
    }

    public static DataStream<String> calculateStats(DataStream<Message> stream, Time windowSize, String latencyFilePath) {
        Time offset;
        if (latencyFilePath.equals("daily_latency.csv")) {
            offset = Time.days(0);
        } else if (latencyFilePath.equals("three_day_latency.csv")) {
            offset = Time.days(2);
        } else if (latencyFilePath.equals("all_time_latency.csv")) {
            offset = Time.days(13);
        } else {
            offset = Time.days(0);
        }
        
        return stream
                .windowAll(TumblingEventTimeWindows.of(windowSize, offset))
                .apply(new CalculateStatsAllWindowFunction(latencyFilePath));
    }

    public static class CalculateStatsAllWindowFunction extends RichAllWindowFunction<Message, String, TimeWindow> {
        private transient MapState<String, Double> latestState;

        private final String latencyFilePath;
        private static final ObjectMapper objectMapper = new ObjectMapper();

        public CalculateStatsAllWindowFunction(String latencyFilePath) {
            this.latencyFilePath = latencyFilePath;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Double> descriptor = new MapStateDescriptor<>(
                    "latestState",
                    String.class,
                    Double.class
            );
            latestState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void apply(TimeWindow window, Iterable<Message> values, Collector<String> out) throws Exception {
            Map<String, TDigest> tdigestMap = new HashMap<>();
            Map<String, Double> minMap = new HashMap<>();
            Map<String, Double> maxMap = new HashMap<>();
            Map<String, Integer> countMap = new HashMap<>();
            int messageCount = 0;
            long totalBytes = 0;

            Path latencyPath = Paths.get(latencyFilePath);
            try {
                Files.createFile(latencyPath);
            } catch (FileAlreadyExistsException e) {

            }
            long startTime = System.nanoTime(); // Inizio misurazione tempo
            

            for (Message message : values) {
                messageCount++;
                String messageJson = objectMapper.writeValueAsString(message);
                totalBytes += messageJson.getBytes().length;

                String vaultId = message.getVaultId();
                double hours = message.getPowerOnHours();

                latestState.put(message.getSerialNumber(), hours);

                TDigest tDigest = tdigestMap.computeIfAbsent(vaultId, k -> TDigest.createMergingDigest(100));
                tDigest.add(hours);

                minMap.put(vaultId, Math.min(minMap.getOrDefault(vaultId, Double.MAX_VALUE), hours));
                maxMap.put(vaultId, Math.max(maxMap.getOrDefault(vaultId, Double.MIN_VALUE), hours));
                countMap.put(vaultId, countMap.getOrDefault(vaultId, 0) + 1);
            }

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
            String windowStart = formatter.format(Instant.ofEpochMilli(window.getStart()));
            String windowEnd = formatter.format(Instant.ofEpochMilli(window.getEnd()));

            for (String vaultId : tdigestMap.keySet()) {
                TDigest tDigest = tdigestMap.get(vaultId);
                double min = minMap.get(vaultId);
                double max = maxMap.get(vaultId);
                int count = countMap.get(vaultId);

                double p25 = tDigest.quantile(0.25);
                double p50 = tDigest.quantile(0.5);
                double p75 = tDigest.quantile(0.75);

                out.collect(String.format("%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d",
                        windowStart, windowEnd, vaultId, min, p25, p50, p75, max, count));
            }

            long endTime = System.nanoTime(); // Fine misurazione tempo
            long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime); // Durata in millisecondi
            // Calcolo del throughput in messaggi al secondo
            double throughputMessages = (double) messageCount / (duration / 1000.0);

            // Calcolo del throughput in byte al secondo
            double throughputBytes = (double) totalBytes / (duration / 1000.0);
            String durationRecord = "Window: " + windowStart + " to " + windowEnd + ", Duration: " + duration + " ms, Throughput:" + throughputMessages+ " message/sec, "+ throughputBytes +" bytes/sec\n";
            Files.write(latencyPath, durationRecord.getBytes(), StandardOpenOption.APPEND);

        }
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

    private static Properties getKafkaProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("group.id", "flink-group");
        return properties;
    }

    public static class Message {
        private Long date;
        private String serial_number;
        private String model;
        private String failure;
        private String vault_id;
        private double s9_power_on_hours;
        private long ingressTimestamp;
        private Long processingEndTime;


        public Message(Long date, String serial_number, String model, String failure, String vault_id, double s9_power_on_hours) {
            this.date = date;
            this.serial_number = serial_number;
            this.model = model;
            this.failure = failure;
            this.vault_id = vault_id;
            this.s9_power_on_hours = s9_power_on_hours;
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

                return new Message(date, serialNumber, model, failure, vaultId, s9PowerOnHours);
            } catch (Exception e) {
                System.out.println("Errore di deserializzazione del JSON: " + e.getMessage());
                return null;
            }
        }

        public Long getDate() {
            return date;
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

        public double getPowerOnHours() {
            return s9_power_on_hours;
        }
    }

     public static class IngressTimestampMapFunction extends RichMapFunction<String, Message> {
        @Override
        public Message map(String value) throws Exception {
            Message message = Message.create(value);
            if (message != null) {
                message.ingressTimestamp = System.currentTimeMillis();
            }
            return message;
        }
    }
}
