package guru.bonacci.kafka.connect;

import static java.util.Arrays.asList;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import com.google.gson.Gson;

import io.lettuce.core.RedisClient;
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
	private final Gson gson;
	private final ImmutablePair<String, Set<String>> query;
	
	
	public Tile38Client(String url, int port, String queryString) {
		this.client = RedisClient.create(String.format("redis://%s:%d", url, port));
		this.sync = client.connect().sync();

	    final Set<String> targetSet = new HashSet<>();
		CollectionUtils.addAll(targetSet, queryString.split(" "));
		this.query = new ImmutablePair<>(queryString, targetSet);

		this.gson = new Gson();
	}

	@SuppressWarnings("unchecked")
	public void write(Collection<Record> events) {
		for (Record record : events) {
			Map<String, String> json = gson.fromJson(record.getJson().toString(), Map.class);
			CommandArgs<String, String> cmd = new QueryHelper(query, json).generateCommand();

			String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), cmd);
			log.info("tile38 answers {}", resp);
		}
	}

	public void close() {
		client.shutdown();
	}
}
