package guru.bonacci.kafka.connect;

import static guru.bonacci.kafka.connect.StringBuilderUtils.replaceAll;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;

import io.lettuce.core.protocol.CommandArgs;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class QueryHelper {

	// query string with query terms
	private final ImmutablePair<String, Set<String>> query;
	private final Map<String, String> json;

	
	public CommandArgs<String, String> generateCommand() {
		CommandArgs<String, String> cmd = new CommandArgs<>(UTF8);
		asList(preparedStatement().split(" ")).forEach(cmd::add);
		
		log.debug(cmd.toCommandString());
	    return cmd;
	}

	// default for testing
	String preparedStatement() {
		Stream<String> events = query.right.stream().filter(s -> s.startsWith(Constants.TOKERATOR));
		Map<String, String> parsed = events.collect(toMap(identity(), ev -> {
			try {
				String prop = ev.replace(Constants.TOKERATOR, "");
				Object val = PropertyUtils.getProperty(json, prop);
				return val != null ? String.valueOf(val) : ev;
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				// ignore mismatch
				return ev;
			}
		}));

		StringBuilder result = new StringBuilder(query.left);
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			result = replaceAll(result, entry.getKey(), entry.getValue());
		}

		return result.toString();
	}
}
