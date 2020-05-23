package guru.bonacci.kafka.connect.tile38;

import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.tuple.Pair;

import io.lettuce.core.protocol.CommandArgs;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class CommandGenerator {

	// command string with terms
	private final Pair<String, Set<String>> cmd;

	
	CommandArgs<String, String> compile(Map<String, String> json) {
		CommandArgs<String, String> cmd = new CommandArgs<>(UTF8);
		asList(preparedStatement(json).split(" ")).forEach(cmd::add);
		
		log.debug(cmd.toCommandString());
	    return cmd;
	}

	// visible for testing
	String preparedStatement(Map<String, String> json) {
		Stream<String> events = cmd.getRight().stream();
		Map<String, String> parsed = events.collect(toMap(identity(), ev -> {
			try {
				String prop = ev.replace(TOKERATOR, "");
				Object val = PropertyUtils.getProperty(json, prop);
				return val != null ? String.valueOf(val) : ev;
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				// ignore mismatch
				return ev;
			}
		}));

		String result = cmd.getLeft();
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			result = result.replaceAll(entry.getKey(), quoteReplacement(entry.getValue()));
		}

		return result;
	}
	
	static CommandGenerator from(Pair<String, Set<String>> cmd) {
		cmd.getRight().removeIf(s -> !s.startsWith(TOKERATOR));
		return new CommandGenerator(cmd);
	}
}
