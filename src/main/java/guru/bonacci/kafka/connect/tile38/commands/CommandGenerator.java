package guru.bonacci.kafka.connect.tile38.commands;

import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static io.lettuce.core.codec.StringCodec.UTF8;
import static java.util.Arrays.asList;
import static java.util.function.Function.identity;
import static java.util.regex.Matcher.quoteReplacement;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.tuple.Triple.of;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.connect.errors.DataException;

import guru.bonacci.kafka.connect.tile38.writer.Tile38Record;
import io.lettuce.core.output.CommandOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Class that facilitates Redis command generation based on sinking records
 */
@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class CommandGenerator {

	private final CommandTemplate cmd;

	/**
	 * Generates a Redis command based on a record's key and/or value.
	 * Sending a command with Lettuce requires three arguments:
	 * 1. CommandType: SET or DEL
	 * 2. CommandOutput: Lettuce forces one to anticipate on the result data type
	 * 3. CommandArgs: The actual command terms
	 */
	public Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> compile(final Tile38Record record) {
		final Triple<CommandType, CommandOutput<String, String, ?>, CommandArgs<String, String>> generatedCmd;
		final CommandArgs<String, String> cmdArgs = new CommandArgs<>(UTF8);

		if (record.getValue() != null) {
			// convert the command string into a CommandArgs object.
			asList(preparedStatement(record.getValue()).split(" ")).forEach(cmdArgs::add);
		    generatedCmd = of(CommandType.SET, new StatusOutput<>(UTF8), cmdArgs);
		    
		} else { 		
			// Tombstone messages are deleted
			// Command format: DEL key id
			cmdArgs.add(cmd.getKey()); //key
			cmdArgs.add(record.getId()); //id

			generatedCmd = of(CommandType.DEL, new IntegerOutput<>(UTF8), cmdArgs);
		}
			
		log.trace("Compiled to: {} {}", generatedCmd.getLeft(), cmdArgs.toCommandString());
	    return generatedCmd;
	}

	/**
	 * A command statement is created by 
	 * substituting the command terms with the records actual values.
	 * 'event.XYZ' corresponds here to a records field name XYZ.
	 * Nesting is supported with dots, as in 'outermost.outer.inner.innermost'
	 */
	// visible for testing
	String preparedStatement(final Map<String, Object> record) {
		// determine for each command term its corresponding record value
		final Map<String, String> parsed = cmd.getTerms().stream()
			.collect(toMap(identity(), term -> {
			try {
				// field name is query term without 'event.'
				String prop = term.replace(TOKERATOR, "");
				
				// given the field name, retrieve the field value from the record
				Object val = PropertyUtils.getProperty(record, prop);

				if (val == null) {
					// record does not contain required field 
					throw new IllegalAccessException();
				}
				return String.valueOf(val);
			} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
				log.warn("Field mismatch command {}, and sink record {}", term, record);
				throw new DataException("Field mismatch between command and sink record", e);
			}
		}));

		// build the command string by replacing the command terms with record values.
		String generatedCmdString = cmd.getCmdString();
		for (Map.Entry<String, String> entry : parsed.entrySet()) {
			// thereby escaping characters
			generatedCmdString = generatedCmdString.replaceAll(entry.getKey(), quoteReplacement(entry.getValue()));
		}

		return generatedCmdString;
	}
	
	public static CommandGenerator from(CommandTemplate cmd) {
		return new CommandGenerator(cmd);
	}
}
