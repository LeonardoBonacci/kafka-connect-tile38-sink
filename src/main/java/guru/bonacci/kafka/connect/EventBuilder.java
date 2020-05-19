package guru.bonacci.kafka.connect;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.sink.SinkRecord;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventBuilder {

	private Set<String> topics;
	private Collection<SinkRecord> sinkRecords; 


    EventBuilder withTopics(Set<String> topics) {
        this.topics = topics;
        return this;
    }

    EventBuilder withSinkRecords(Collection<SinkRecord> sinkRecords) {
        this.sinkRecords = sinkRecords;
        return this;
    }

    Map<String, List<InternalSinkRecord>> build() { 
        Map<String, List<SinkRecord>> byTopic = sinkRecords.stream()
        		.collect(groupingBy(SinkRecord::topic));

        Map<String, List<SinkRecord>> srByTopic = Maps.filterKeys(byTopic, topic -> {
        	boolean isValidTopic = topics.contains(topic);
            if (!isValidTopic) {
                log.debug("Topic {} not present", topic);
            } 
            return isValidTopic;
        });

        Map<String, List<InternalSinkRecord>> interalByTopic = 
        		Maps.transformValues(srByTopic, sinkRecords -> {
	        return sinkRecords.stream()
	                .map(DataConverter::toInternalSinkRecord)
	                .collect(toList());
	        }
        );

        return interalByTopic;
    }
}