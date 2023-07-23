package vlqhoang.examples.kafka.favoritecolorstreamscore.controller;

import lombok.AllArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
@AllArgsConstructor
public class FavoriteColorRestController {

    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/count/{color}")
    public Long getWordCount(@PathVariable String color) {
        KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        ReadOnlyKeyValueStore<String, Long> counts = kafkaStreams
                .store(StoreQueryParameters.fromNameAndType("CountsByColours", QueryableStoreTypes.keyValueStore()));
        return counts.get(color);
    }
}
