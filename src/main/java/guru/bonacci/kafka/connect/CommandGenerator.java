package guru.bonacci.kafka.connect;

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
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CommandGenerator {

	private static final String TOKEN = "event";
	private static final String SEPARATOR = ".";
	public static final String TOKERATOR = TOKEN + SEPARATOR;
	
	
	// command string with terms
	private final ImmutablePair<String, Set<String>> command;

	
	public CommandGenerator(ImmutablePair<String, Set<String>> command) {
		this.command = command;
		this.command.right.removeIf(s -> !s.startsWith(TOKERATOR));
	}
	
	public CommandArgs<String, String> compile(Map<String, String> json) {
		CommandArgs<String, String> cmd = new CommandArgs<>(UTF8);
		asList(preparedStatement(json).split(" ")).forEach(cmd::add);
		
		log.debug(cmd.toCommandString());
	    return cmd;
	}

	// visible for testing
	String preparedStatement(Map<String, String> json) {
		Stream<String> events = command.right.stream();
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

		String result = command.left;
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			result = result.replaceAll(entry.getKey(), entry.getValue());
		}

		return result;
	}
}
