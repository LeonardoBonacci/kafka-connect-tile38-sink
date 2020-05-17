package guru.bonacci.kafka.connect;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38Client {

	private final RedisClient client;
	private final RedisCommands<String, String> sync;
	private final String q;

	public Tile38Client(String url, int port, String q) {
		this.client = RedisClient.create(String.format("redis://%s:%d", url, port));
		StatefulRedisConnection<String, String> connection = client.connect();
		this.sync = connection.sync();

		this.q = q;
	}

	public void write(Collection<Record> events) {
		List<String> terms = Arrays.asList(q.split(" "));
		for (Record record : events) {
			CommandArgs<String, String> cmd = new QueryHelper(q, terms, record.getJson()).generateCommand();
			String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), cmd);
			log.info("tile38 answers {}", resp);
		}
	}

	public void close() {
		client.shutdown();
	}
}
