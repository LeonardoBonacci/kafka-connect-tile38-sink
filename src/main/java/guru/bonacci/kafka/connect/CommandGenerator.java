package guru.bonacci.kafka.connect;

import static guru.bonacci.kafka.connect.Constants.TOKERATOR;
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

	// command string with terms
	private final ImmutablePair<String, Set<String>> cmd;

	
	private CommandGenerator(ImmutablePair<String, Set<String>> cmd) {
		this.cmd = cmd;
		this.cmd.right.removeIf(s -> !s.startsWith(TOKERATOR));
	}
	
	CommandArgs<String, String> compile(Map<String, String> json) {
		CommandArgs<String, String> cmd = new CommandArgs<>(UTF8);
		asList(preparedStatement(json).split(" ")).forEach(cmd::add);
		
		log.debug(cmd.toCommandString());
	    return cmd;
	}

	// visible for testing
	String preparedStatement(Map<String, String> json) {
		Stream<String> events = cmd.right.stream();
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

		String result = cmd.left;
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			result = result.replaceAll(entry.getKey(), entry.getValue());
		}

		return result;
	}
	
	static CommandGenerator from(ImmutablePair<String, Set<String>> cmd) {
		return new CommandGenerator(cmd);
	}

}
