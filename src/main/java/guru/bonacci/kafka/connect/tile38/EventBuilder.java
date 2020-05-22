package guru.bonacci.kafka.connect.tile38;

import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.transformValues;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.testcontainers.shaded.com.google.common.collect.Lists.newArrayList;
import static org.testcontainers.shaded.com.google.common.collect.Sets.newHashSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;

import com.google.common.collect.ImmutableMap;

import lombok.extern.slf4j.Slf4j;

@Slf4j
class EventBuilder {

	private Set<String> topics = newHashSet();
	private List<SinkRecord> sinkRecords = newArrayList(); 


    EventBuilder withTopics(Set<String> topics) {
        this.topics.addAll(topics);
        return this;
    }

    EventBuilder withSinkRecords(Collection<SinkRecord> sinkRecords) {
        this.sinkRecords.addAll(sinkRecords);
        return this;
    }

    Map<String, List<InternalSinkRecord>> build() { 
    	if (topics.isEmpty() || sinkRecords.isEmpty()) {
			return ImmutableMap.of();
    	}
    	
    	// Group by topic
        Map<String, List<SinkRecord>> byTopic = sinkRecords.stream()
        		.collect(groupingBy(SinkRecord::topic));

    	// Keep only the configured topics..
        Map<String, List<SinkRecord>> recordsByTopic = filterKeys(byTopic, topic -> {
        	boolean isConfigured = topics.contains(topic);
            if (!isConfigured) {
                log.debug("Topic {} not configured", topic);
            } 
            return isConfigured;
        });

    	// ..and convert the records to internal sink records
        Map<String, List<InternalSinkRecord>> interalsByTopic = 
        		transformValues(recordsByTopic, sinkRecords -> {
	        return sinkRecords.stream()
	                .map(DataConverter::toInternalSinkRecord)
	                .collect(toList());
	        }
        );

        return interalsByTopic;
    }
}