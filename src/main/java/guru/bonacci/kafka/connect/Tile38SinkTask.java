package guru.bonacci.kafka.connect;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38SinkTask extends SinkTask {

	private Tile38Service service;

	
	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		service = new Tile38Service(null, new Tile38SinkConnectorConfig(map));
	}

	@Override
	public void put(Collection<SinkRecord> collection) {
		try {
			log.info("Going well, another {} incoming ", collection.size());
			Collection<String> recordsAsStrings = collection.stream().
					map(r -> String.valueOf(r.value()))
					.collect(Collectors.toList());
			service.process(recordsAsStrings);
		} catch (Exception e) {
			log.error("Error while processing records");
			log.error(e.toString());
		}
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
		log.trace("Flushing the queue");
	}

	@Override
	public void stop() {
		service.closeClient();
	}
}
