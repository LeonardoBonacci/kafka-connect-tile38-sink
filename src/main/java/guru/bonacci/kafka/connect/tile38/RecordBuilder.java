package guru.bonacci.kafka.connect.tile38;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.kafka.connect.sink.SinkRecord;

import guru.bonacci.kafka.connect.tile38.writer.RecordConverter;
import guru.bonacci.kafka.connect.tile38.writer.Tile38Record;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class RecordBuilder {

	private final RecordConverter converter = new RecordConverter();
	private final Set<String> topics = newHashSet();
	private final List<SinkRecord> sinkRecords = newArrayList(); 

	
    RecordBuilder withTopics(Set<String> topics) {
        this.topics.addAll(topics);
        return this;
    }

    RecordBuilder withSinkRecords(Collection<SinkRecord> sinkRecords) {
        this.sinkRecords.addAll(sinkRecords);
        return this;
    }

    Stream<Tile38Record> build() { 
    	// Should not happen but just in case
    	if (topics.isEmpty()) {
			return Stream.empty();
    	}
    	
        return sinkRecords.stream().filter(record -> {
        	boolean isConfigured = topics.contains(record.topic());
            if (!isConfigured) {
                log.warn("Topic {} not configured", record.topic());
            } 
            return isConfigured;
        }).map(converter::convert);
    }
}