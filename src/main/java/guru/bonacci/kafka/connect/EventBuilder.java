package guru.bonacci.kafka.connect;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.sink.SinkRecord;

import guru.bonacci.kafka.connect.utils.ConverterFunctions;

public class EventBuilder {

	@SuppressWarnings("unused")
	private String topic;
	private Collection<SinkRecord> sinkRecords; 


    EventBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    EventBuilder withSinkRecords(Collection<SinkRecord> sinkRecords) {
        this.sinkRecords = sinkRecords;
        return this;
    }

    List<InternalSinkRecord> build() { 
        return this.sinkRecords.stream()
                .map(ConverterFunctions::InternalSinkRecord)
                .collect(Collectors.toList());
    }

}