/**
 * 	Copyright 2020 Jeffrey van Helden (aabcehmu@mailfence.com)
 *	
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *	
 *	    http://www.apache.org/licenses/LICENSE-2.0
 *	
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */
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
import io.lettuce.core.RedisException;
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

	// Number of records here depends on the properties:
	// fetch.max.bytes
	// fetch.min.bytes
	// fetch.wait.max.ms
	// max.poll.records
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
		} catch (RedisException e) { //Lettuce wraps ExecutionException and InterruptedException 
			log.warn(e.getMessage());
			throw new RetriableException(e);
		} catch (RuntimeException e) {
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
