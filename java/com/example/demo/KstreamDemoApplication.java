package com.example.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class KstreamDemoApplication {

    /*
        events in favorite-car-topic topic (key is null, value is comma separated 'user','favorite-car')
        brook,audi
        rahul,bmw
        mercy,toyota
        brook,honda   [note, favorite cars for a user could change]
    */
    static final String FAVORITE_CAR_INPUT_TOPIC = "favorite-car-input";

    // store user -> car as key -> value pairs. compacted topic.
    static final String USER_AND_FAVORITE_CAR_TOPIC = "user-favorite-car";

    static final String FAVORITE_CAR_COUNT_OUTPUT_TOPIC = "favorite-car-count-output";
    static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    static final Serde<String> stringSerde = Serdes.String();
    static final Serde<Long> longSerde = Serdes.Long();


    public static void main(String[] args) {
        SpringApplication.run(KstreamDemoApplication.class, args);

        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-cars");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // disable caching to understand the steps involved in transformation -- just for development
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        final Topology topology = getTopology();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // clean local state prior to starting the processing topology
        streams.cleanUp();
        streams.start();

        // shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();

        // construct KStream from the input topic of favorite-car-input
        // transforming the value such as brook,car to k, v pairs like brook -> bmw
        final KStream<String, String> input = builder.stream(FAVORITE_CAR_INPUT_TOPIC);

        final KStream<String, String> usersAndCars = input
                // validate comma is part of the stream before splitting it
                .filter((k, v) -> v.contains(","))
                // select a key to be used as a user id
                .selectKey((k, v) -> v.split(",")[0].toLowerCase())
                // get the favorite car from the value
                .mapValues(v -> v.split(",")[1].toLowerCase())
                // filter undesired cars
                .filter((k, v) -> Arrays.asList("bmw", "honda", "toyota").contains(v));

        usersAndCars.to(USER_AND_FAVORITE_CAR_TOPIC);

        // read user-favorite-car topic as a KTable
        KTable<String, String> usersAndCarsTable = builder.table(USER_AND_FAVORITE_CAR_TOPIC);

        // count occurrences of cars
        KTable<String, Long> favoriteCarsCount =  usersAndCarsTable
                // group by car within the KTable
                .groupBy((k, v) -> new KeyValue<>(v, v))
                .count();

        // convert the `KTable<String, Long>` into a KStream<String, Long> and write to the output topic
        favoriteCarsCount.toStream().to(FAVORITE_CAR_COUNT_OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        return builder.build();
    }

}

