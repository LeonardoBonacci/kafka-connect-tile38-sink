package guru.bonacci.kafka.connect;

import static java.util.Arrays.asList;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.stream.Collectors.toMap;
import static java.util.function.Function.identity;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.beanutils.PropertyUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.lettuce.core.protocol.CommandArgs;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class QueryHelper {

	private final String query;
	private final List<String> queryTerms;
	private final JsonObject json;

	
	public CommandArgs<String, String> generateCommand() {
		CommandArgs<String, String> cmd = new CommandArgs<>(UTF8);
		asList(preparedStatement().split(" ")).forEach(cmd::add);
		
		log.debug(cmd.toCommandString());
	    return cmd;
	}

	// default for testing
	@SuppressWarnings("unchecked")
	String preparedStatement() {
		Stream<String> events = queryTerms.stream().filter(s -> s.startsWith(Constants.TOKERATOR));
		Map<String, String> map = new Gson().fromJson(json.toString(), Map.class);

		Map<String, String> parsed = events.collect(toMap(identity(), ev -> {
			try {
				String prop = ev.replace(Constants.TOKERATOR, "");
				Object val = PropertyUtils.getProperty(map, prop);
				return val != null ? String.valueOf(val) : ev;
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				// ignore mismatch
				return ev;
			}
		}));

		StringBuilder result = new StringBuilder(query);
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			result = replaceAll(result, entry.getKey(), entry.getValue());
		}

		return result.toString();
	}

	// for performance
	private static StringBuilder replaceAll(StringBuilder sb, String find, String replace){
        return new StringBuilder(Pattern.compile(find).matcher(sb).replaceAll(replace));
    }
}
