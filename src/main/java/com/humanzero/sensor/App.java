package com.humanzero.sensor;

import com.humanzero.sensor.receivers.PacketReceiver;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.Properties;


public class App {

    private static final int BATCH_INTERVAL_SECONDS = 60;
    private static String kafkaTopic = "alerts";
    private static String kafkaMessage = "You have a new alert!";
    private static Properties kafkaProps = new Properties();
    private static long minLimit = 1073741824;
    private static long maxLimit = 1024;

    private static void initKafkaProps() {
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", StringSerializer.class);
        kafkaProps.put("value.serializer", StringSerializer.class);
        kafkaProps.put("group.max.session.timeout.ms", 300001);
        kafkaProps.put("heartbeat.interval.ms", 300001);
    }


    public static void main(String[] args) throws InterruptedException {

        Logger.getRootLogger().setLevel(Level.WARN);

        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("traffic-sensor");

        JavaStreamingContext streamingContext =
                new JavaStreamingContext(conf, Durations.seconds(BATCH_INTERVAL_SECONDS));

        initKafkaProps();

        Producer<String, String> kafkaProducer = new KafkaProducer<>(kafkaProps);

        JavaDStream<Long> byteBatchCountFlow = streamingContext
                .receiverStream(new PacketReceiver())
                .map(packet -> ArrayUtils.toObject(packet.getRawData()))
                .flatMap(x -> Arrays.asList(x).iterator())
                .count();

        byteBatchCountFlow.print();

        byteBatchCountFlow.foreachRDD(rdd -> {
            if (rdd.count() < minLimit || rdd.count() > maxLimit) {
                kafkaProducer.send(new ProducerRecord<>(kafkaTopic, kafkaMessage));
                kafkaProducer.close();
            }
        });

        streamingContext.start();
        streamingContext.awaitTermination();

    }
}
