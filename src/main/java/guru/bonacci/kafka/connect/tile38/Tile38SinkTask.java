package guru.bonacci.kafka.connect.tile38;

import static guru.bonacci.kafka.connect.tile38.validators.BehaviorOnErrorValues.FAIL;
import static io.lettuce.core.LettuceFutures.awaitAll;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;
import guru.bonacci.kafka.connect.tile38.writer.Tile38Record;
import guru.bonacci.kafka.connect.tile38.writer.Tile38Writer;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38SinkTask extends SinkTask {

	private Tile38SinkConnectorConfig config;
	@Getter private Tile38Writer writer; // for testing

	
	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Starting Tile38SinkTask");

		this.config = new Tile38SinkConnectorConfig(props);
		this.writer = new Tile38Writer(config);
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		log.trace("Putting {} records to Tile38", records.size());

		if (records.isEmpty()) {
			return;
		}

		final Stream<Tile38Record> recordStream = new RecordBuilder()
				.withTopics(config.getTopicsConfig().configuredTopics())
				.withSinkRecords(records)
				.build();

		final RedisFuture<?>[] futures = writer.write(recordStream);
		
		wait(futures);
		log.trace(futures.length + " commands executed");
	}

	private void wait(RedisFuture<?>... futures) {
		// Wait until all commands are executed
		try {
			boolean completed = awaitAll(config.getFlushTimeOut(), MILLISECONDS, futures);
			if (!completed) {
				// Only non-completed tasks can be cancelled
				for (RedisFuture<?> f : futures) {
					f.cancel(true);
				}

				throw new RetriableException(
						String.format("Timeout after %s ms while waiting for operation to complete.", config.getFlushTimeOut()));
			}
		} catch (RedisCommandExecutionException e) {
			log.warn(e.getMessage());
			if (FAIL.equals(config.getBehaviorOnError())) {
				throw new ConnectException(e);
			}
		}
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
		
		if (writer != null) {
			writer.close();
		}
	}
}
