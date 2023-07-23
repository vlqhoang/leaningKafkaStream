package vlqhoang.examples.kafka.favoritecolorstreamscore.processor;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FavoriteColorProcessor {

    private static final Serde<String> STRING_SERDE = Serdes.String();

    private static final Serde<Long> LONG_SERDE = Serdes.Long();

    private static final List<String> ALLOW_COLORS = List.of("red", "green", "blue");

    @Value(value = "${kafka.topic.input}")
    private String inputTopic;

    @Value(value = "${kafka.topic.intermediary}")
    private String intermediaryTopic;

    @Value(value = "${kafka.topic.output}")
    private String outputTopic;

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        final KStream<String, String> textStream = streamsBuilder
                .stream(inputTopic, Consumed.with(STRING_SERDE, STRING_SERDE));

        KStream<String, String> usersAndColorStream = textStream
                .filter((key, value) -> value.contains(",")) // ensure input text has "," to split
                .selectKey((key, value) -> value.split(",")[0].toLowerCase()) // extract user id to <key>
                .mapValues((value) -> value.split(",")[1].toLowerCase()) //extract color to <value>
                .filter((userId, color) -> ALLOW_COLORS.contains(color));

        usersAndColorStream.to(intermediaryTopic);

        final KTable<String, String> usersAndColoursTable = streamsBuilder.table(intermediaryTopic);

        KTable<String, Long> countFavoriteColorsTable = usersAndColoursTable
                .groupBy((userId, color) -> new KeyValue<>(color, color))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("CountsByColours")
                        .withKeySerde(STRING_SERDE)
                        .withValueSerde(LONG_SERDE));

        countFavoriteColorsTable.toStream().to(outputTopic, Produced.with(STRING_SERDE, LONG_SERDE));

    }
}
