package guru.bonacci.kafka.connect;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.beanutils.PropertyUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

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

	public void send(List<Record> records) {
		for (Record record : records) {
			JsonObject json = record.getJson();
			String query = prepareQuery(q, json);

			CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
			Arrays.asList(query.split(" ")).forEach(args::add);
            log.info("dont exec: " + args.toCommandString());

			String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), args);
			log.info("tile38 answers {}", resp);
		}
	}

	String prepareQuery(String query, JsonObject jsonO) {
		Stream<String> events = Arrays.asList(query.split(" ")).stream().filter(s -> s.startsWith(Constants.TOKERATOR));

		Map<String, String> map = new Gson().fromJson(jsonO.toString(), Map.class);

		Map<String, String> parsed = events.collect(Collectors.toMap(Function.identity(), ev -> {
			try {
				String prop = ev.replace(Constants.TOKERATOR, "");
				Object val = PropertyUtils.getProperty(map, prop);
				return val != null ? String.valueOf(val) : ev;
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				// ignore
				return ev;
			}
		}));
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			query = query.replaceAll(entry.getKey(), entry.getValue());
		}

		System.out.println(query);
		return query;
	}

	public void close() {
		client.shutdown();
	}
}
