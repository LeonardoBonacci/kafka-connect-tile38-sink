package guru.bonacci.kafka.connect;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import guru.bonacci.kafka.connect.service.ElasticService;
import guru.bonacci.kafka.connect.service.ElasticServiceImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticSinkTask extends SinkTask {

	private ElasticService elasticService;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> map) {
		elasticService = new ElasticServiceImpl(null, new ElasticSinkConnectorConfig(map));
	}

	@Override
	public void put(Collection<SinkRecord> collection) {
		try {
			log.error("going well another " + collection.size());
			Collection<String> recordsAsString = collection.stream().
					peek(r -> log.error("record" + r)).map(r -> String.valueOf(r.value()))
					.peek(r -> log.error("record as string" + r)) 
					.collect(Collectors.toList());
			elasticService.process(recordsAsString);
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
		try {
			elasticService.closeClient();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
