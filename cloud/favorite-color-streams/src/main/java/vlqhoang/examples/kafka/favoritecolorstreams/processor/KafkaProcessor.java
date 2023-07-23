package vlqhoang.examples.kafka.favoritecolorstreams.processor;

import lombok.extern.log4j.Log4j2;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.function.Function;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

@Configuration
@Log4j2
public class KafkaProcessor {

    private static final List<String> ALLOW_COLORS = List.of("red", "green", "blue");

    @Bean
    public Function<KStream<String, String>, KStream<String, String>> extractUserKeyProcessor(){
        return kStream ->  kStream
                .filter((key, value) -> value.contains(",")) // ensure input text has "," to split
                .selectKey((key, value) -> value.split(",")[0].toLowerCase()) // extract user id to <key>
                .mapValues((value) -> value.split(",")[1].toLowerCase()) //extract color to <value>
                .filter((userId, color) -> ALLOW_COLORS.contains(color))
                .peek((userId, color) -> log.debug("Outputting message with key: {}, value: {}", userId, color));
    }

    @Bean
    public Function<KTable<String, String>, KStream<String, Long>> countUserFavoriteColorProcessor() {

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        return userKeyAndColorInputStream -> userKeyAndColorInputStream
                .groupBy((userId, color) -> new KeyValue<>(color, color))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(stringSerde)
                        .withValueSerde(longSerde))
                .toStream();
    }
}
