package guru.bonacci.kafka.connect.tile38.commands;

import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;
import static io.lettuce.core.codec.StringCodec.UTF8;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.errors.DataException;

import guru.bonacci.kafka.connect.tile38.writer.Tile38Record;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class CommandGenerator {

	private final CommandWrapper cmd;

	
	public Pair<CommandType, CommandArgs<String, String>> compile(Tile38Record record) {
		CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);

		// tombstone message are deleted
		if (record.getValue() == null) {
			cmdArgs.add(cmd.getKey());
			cmdArgs.add(record.getKey());

			log.debug("Compiled to: {}", cmdArgs.toCommandString());
			return Pair.of(CommandType.DEL, cmdArgs);
		} 
			
		asList(preparedStatement(record.getValue()).split(" ")).forEach(cmdArgs::add);
		
		log.debug("Compiled to: {}", cmdArgs.toCommandString());
	    return Pair.of(CommandType.SET, cmdArgs);
	}

	// visible for testing
	String preparedStatement(Map<String, Object> json) {
		Stream<String> events = cmd.getTerms().stream();
		Map<String, String> parsed = events.collect(toMap(identity(), ev -> {
			try {
				String prop = ev.replace(TOKERATOR, "");
				Object val = PropertyUtils.getProperty(json, prop);

				if (val == null)
					throw new IllegalAccessException();
				return String.valueOf(val);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				log.warn("Field mismatch command {}, and sink record {}", ev, json);
				throw new DataException("Field mismatch between command and sink record", e);
			}
		}));

		String result = cmd.getCmdString();
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			result = result.replaceAll(entry.getKey(), quoteReplacement(entry.getValue()));
		}

		return result;
	}
	
	public static CommandGenerator from(CommandWrapper cmd) {
		return new CommandGenerator(cmd);
	}
}
