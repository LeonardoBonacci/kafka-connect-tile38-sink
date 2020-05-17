package guru.bonacci.kafka.connect;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.beanutils.PropertyUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandArgs;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class QueryHelper {

	private final String query;
	private final JsonObject json;

	public CommandArgs<String, String> generateCommand() {
		CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
		Arrays.asList(prepareStatement().split(" ")).forEach(args::add);
	    log.info("dont exec: " + args.toCommandString());
	    return args;
	}

	// default for testing
	@SuppressWarnings("unchecked")
	String prepareStatement() {
		Stream<String> events = Arrays.asList(query.split(" ")).stream().filter(s -> s.startsWith(Constants.TOKERATOR));
		Map<String, String> map = new Gson().fromJson(json.toString(), Map.class);

		Map<String, String> parsed = events.collect(Collectors.toMap(Function.identity(), ev -> {
			try {
				String prop = ev.replace(Constants.TOKERATOR, "");
				Object val = PropertyUtils.getProperty(map, prop);
				return val != null ? String.valueOf(val) : ev;
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				// ignore mismatch
				return ev;
			}
		}));

		String result = query;
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			result = result.replaceAll(entry.getKey(), entry.getValue());
		}

		System.out.println(result);
		return result;
	}


}
