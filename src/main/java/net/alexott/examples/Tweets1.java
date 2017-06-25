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

import com.eneco.trading.kafka.connect.twitter.Tweet;

import io.confluent.examples.streams.utils.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;

public class Tweets1 {
    public static void main(String[] args) {
        final Serde<String> stringSerde = Serdes.String();
        final Serde<byte[]> bytesSerde = Serdes.ByteArray();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
        // "true");
        // props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        // "http://localhost:8081");

        final SpecificAvroSerde<Tweet> tweetsSerde = new SpecificAvroSerde<>();
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
        KStream<byte[], Tweet> tweets = builder.stream(bytesSerde, tweetsSerde, "tweets");
        // tweets.foreach((key, value) -> System.out.println("key='"
        // + (key == null ? "null" : DatatypeConverter.printHexBinary(key))
        // + "', value='"
        // + (value == null ? "null" : DatatypeConverter.printHexBinary(value))
        // + "'"));
        // tweets.foreach((key, value) -> System.out.println(value));
        tweets.map((key, value) -> new KeyValue<String, String>(
                value.getUser().getScreenName(), value.getText()))
                .to(stringSerde, stringSerde, "tweets-text");
        // tweets.map((key, value) -> new
        // KeyValue<>(value.getUser().getScreenName(),
        // value.getText())).to(stringSerde, stringSerde, "tweets-text");

        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close()));
    }
}
