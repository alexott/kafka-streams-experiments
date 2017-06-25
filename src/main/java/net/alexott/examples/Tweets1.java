package net.alexott.examples;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import com.eneco.trading.kafka.connect.twitter.TwitterStatus;

import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class Tweets1 {
    public static void main(String[] args) {
        final Serde<String> stringSerde = Serdes.String();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put("schema.registry.url", "http://localhost:8081");
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final SpecificAvroSerde<TwitterStatus> tweetsSerde = new SpecificAvroSerde<>();
        final Map<String, String> serdeConfig = Collections.singletonMap(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");
        tweetsSerde.configure(serdeConfig, false);

        // props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        // JsonTimestampExtractor.class);
        // setting offset reset to earliest so that we can re-run the demo code
        // with the same pre-loaded data
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, TwitterStatus> tweets = builder.stream(stringSerde, tweetsSerde,
                "tweets");
        tweets.map((key, value) -> new KeyValue<>(value.getUser().getScreenName(),
                value.getText())).to(stringSerde, stringSerde, "tweets-text");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
    }
}
