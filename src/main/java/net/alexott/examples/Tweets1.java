package net.alexott.examples;

import java.util.Properties;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import com.eneco.trading.kafka.connect.twitter.TwitterStatus;

import io.confluent.examples.streams.utils.SpecificAvroDeserializer;
import io.confluent.examples.streams.utils.SpecificAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;


public class Tweets1 
{
    @SuppressWarnings({ "deprecation" })
    public static void main( String[] args )
    {
        final Serializer<String> stringSerializer = new StringSerializer();
        final Deserializer<String> stringDeserializer = new StringDeserializer();
        final Serializer<Long> longSerializer = new LongSerializer();
        final Deserializer<Long> longDeserializer = new LongDeserializer();
        
        Properties props = new Properties();
        props.put(StreamsConfig.JOB_ID_CONFIG, "tweets1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        props.put(StreamsConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamsConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamsConfig.VALUE_SERIALIZER_CLASS_CONFIG, SpecificAvroSerializer.class);
        props.put(StreamsConfig.VALUE_DESERIALIZER_CLASS_CONFIG, SpecificAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        
//        props.put(StreamsConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, TwitterStatus> tweets = builder.stream("tweets2");
        KStream<String, Long> hashCounts = tweets.map((key, value) -> new KeyValue<>(value.getUser().getScreenName(), value))
                .countByKey(stringSerializer, longSerializer, stringDeserializer, longDeserializer, "Counts")
                .toStream();
        hashCounts.to("tweets-count", stringSerializer, longSerializer);
        
        KafkaStreams streams = new KafkaStreams(builder, props);
        streams.start();
    }
}
