package guru.bonacci.kafka.connect.tile38;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import guru.bonacci.kafka.connect.tile38.writer.Tile38Record;
import guru.bonacci.kafka.connect.tile38.writer.Tile38Writer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Tile38SinkTask extends SinkTask {

	private Tile38SinkConnectorConfig config;
	private Tile38Writer service;

	
	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Starting Tile38SinkTask");

		this.config = new Tile38SinkConnectorConfig(props);
		this.service = new Tile38Writer(config);
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		log.debug("Putting {} records to Tile38", records.size());

		if (records.isEmpty()) {
			return;
		}

		Map<String, List<Tile38Record>> data = new EventBuilder()
				.withTopics(config.getTopicsConfig().configuredTopics())
				.withSinkRecords(records)
				.build();

		service.writeData(data);
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		log.debug("Flushing data to Tile38 with the following offsets: {}", offsets);
	}

	@Override
	public void close(Collection<TopicPartition> partitions) {
		log.debug("Closing the task for topic partitions: {}", partitions);
	}

	@Override
	public void stop() {
		log.info("Stopping Tile38SinkTask");
		
		if (service != null) {
			service.close();
		}
	}
}
