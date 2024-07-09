package org.example;

import org.apache.commons.text.ExtendedMessageFormat;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.FileSystem.WriteMode;
//import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.webmonitor.stats.Statistics;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
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
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.naming.Context;

import javax.naming.Context;


public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "my-topic",
                new SimpleStringSchema(),
                getKafkaProperties()
        );

        // Configure watermark strategy
        WatermarkStrategy<Message> watermarkStrategy = WatermarkStrategy
                .<Message>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, timestamp) -> event.getDate());

        DataStream<String> rawStream = env.addSource(consumer);

        // Parse the raw JSON records into a Tuple3<String, Integer, List<Message>>
        DataStream<Message> messageStream = rawStream
                .map(new IngressTimestampMapFunction())
                //.map(Message::create)
                .filter(message -> message != null && "1".equals(message.getFailure()))  // Filter only messages with failure
                .assignTimestampsAndWatermarks(watermarkStrategy);

        
        DataStream<Tuple3<String, Integer, List<Message>>> failureStream = messageStream
                .map(message -> Tuple3.of(message.getVaultId(), 1, List.of(message)))
                .returns(Types.TUPLE(Types.STRING, Types.INT, Types.LIST(Types.GENERIC(Message.class))));

        // Calculate top 10 vaults with highest failures in 1-day window
        DataStream<String> dailyTop10 = calculateTop10(failureStream, Time.days(1), "daily_latency.csv");

        // Calculate top 10 vaults with highest failures in 3-day window
        DataStream<String> threeDayTop10 = calculateTop10(failureStream, Time.days(3), "three_day_latency.csv");

        DataStream<String> allDayTop10 = calculateTop10(failureStream, Time.days(23), "all_days_latency.csv");

        // Write results to CSV files
        dailyTop10.addSink(new OutputFormatSinkFunction<>(new CustomCsvOutputFormat("daily_top10.csv")));
        threeDayTop10.addSink(new OutputFormatSinkFunction<>(new CustomCsvOutputFormat("three_day_top10.csv")));
        allDayTop10.addSink(new OutputFormatSinkFunction<>(new CustomCsvOutputFormat("all_day_top10.csv")));

        env.execute("Disk Failure Ranking");
    }

    public static DataStream<String> calculateTop10(DataStream<Tuple3<String, Integer, List<Message>>> failureStream, Time windowSize, String latencyFilePath) {
        Time offset;
        if (latencyFilePath.equals("daily_latency.csv")) {
            offset = Time.days(0);
        } else if (latencyFilePath.equals("three_day_latency.csv")) {
            offset = Time.days(2);
        } else if (latencyFilePath.equals("all_days_latency.csv")) {
            offset = Time.days(13);
        } else {
            offset = Time.days(0);
        }

        return failureStream
                .windowAll(TumblingEventTimeWindows.of(windowSize, offset))
                .apply(new AllWindowFunction<Tuple3<String, Integer, List<Message>>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Tuple3<String, Integer, List<Message>>> values, Collector<String> out) throws Exception {

                        long startTime = System.nanoTime(); // Inizio misurazione tempo
                        ObjectMapper objectMapper = new ObjectMapper(); // ObjectMapper per calcolare la dimensione dei messaggi
                        int messageCount = 0;
                        long totalBytes = 0;
                        Path latencyPath = Paths.get(latencyFilePath);
                        try {
                            Files.createFile(latencyPath);
                        } catch (FileAlreadyExistsException e) {
                        }
                        // Group by vault ID and aggregate the number of failures
                        Map<String, Tuple3<String, Integer, List<Message>>> groupedFailures = StreamSupport.stream(values.spliterator(), false)
                                .collect(Collectors.toMap(
                                        tuple -> tuple.f0,
                                        tuple -> tuple,
                                        (tuple1, tuple2) -> {
                                            int newFailures = (Integer) tuple1.f1 + (Integer) tuple2.f1;
                                            List<Message> combinedMessages = Stream.concat(tuple1.f2.stream(), tuple2.f2.stream())
                                                    .collect(Collectors.toList());
                                            return Tuple3.of(tuple1.f0, newFailures, combinedMessages);
                                        }
                                ));

                        List<Tuple3<String, Integer, List<Message>>> sortedFailures = groupedFailures.values().stream()
                                .sorted((a, b) -> Integer.compare(b.f1, a.f1))
                                .limit(10)
                                .collect(Collectors.toList());

                        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());

                        // Convert start and end time of the window to readable format
                        String windowStart = formatter.format(Instant.ofEpochMilli(window.getStart()));
                        String windowEnd = formatter.format(Instant.ofEpochMilli(window.getEnd()));

                        StringBuilder resultBuilder = new StringBuilder();
                        resultBuilder.append("Window Start,Window End,Vault ID,Failures,Disk Details\n");

                        for (Tuple3<String, Integer, List<Message>> failure : sortedFailures) {
                            for (Message msg : failure.f2) {
                                messageCount++;
                                String messageJson = objectMapper.writeValueAsString(msg);
                                totalBytes += messageJson.getBytes().length;
                                msg.setProcessingEndTime(System.currentTimeMillis());
                                long latency = msg.getProcessingEndTime() - msg.getIngressTimestamp();
                                resultBuilder.append(windowStart)
                                        .append(",")
                                        .append(windowEnd)
                                        .append(",")
                                        .append(failure.f0)
                                        .append(",")
                                        .append(failure.f1)
                                        .append(",")
                                        .append(failure.f2.stream()
                                                .map(m -> m.getModel() + "-" + m.getSerialNumber())
                                                .collect(Collectors.joining(";")))
                                        .append("\n");
                                // Scrivi la latenza nel file di latenza
                                //String latencyRecord = windowStart + ", vault_id:" + failure.f0 + ", " + latency + "\n";
                                //Files.write(latencyPath, latencyRecord.getBytes(), StandardOpenOption.APPEND);
                            }
                        }
                        long endTime = System.nanoTime(); // Fine misurazione tempo
                        long duration = TimeUnit.NANOSECONDS.toMillis(endTime - startTime); // Durata in millisecondi
                        // Calcolo del throughput in messaggi al secondo
                        double throughputMessages = (double) messageCount / (duration / 1000.0);

                        // Calcolo del throughput in byte al secondo
                        double throughputBytes = (double) totalBytes / (duration / 1000.0);
                        String durationRecord = "Window: " + windowStart + " to " + windowEnd + ", Duration: " + duration + " ms, Throughput:" + throughputMessages+ " message/sec, "+ throughputBytes +" bytes/sec\n";
                        Files.write(latencyPath, durationRecord.getBytes(), StandardOpenOption.APPEND);

                        String result = resultBuilder.toString();
                        out.collect(result);
                    }
                });
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
        private Long ingressTimestamp;
        private Long processingEndTime;
        

        public Message(Long date, String serial_number, String model, String failure, String vault_id) {
            this.date = date;
            this.serial_number = serial_number;
            this.model = model;
            this.failure = failure;
            this.vault_id = vault_id;
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

                return new Message(date, serialNumber, model, failure, vaultId);
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

