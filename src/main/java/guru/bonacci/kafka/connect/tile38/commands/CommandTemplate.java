package guru.bonacci.kafka.connect.tile38.commands;

import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Sets.newHashSet;
import static guru.bonacci.kafka.connect.tile38.Constants.DEL_RESERVED;
import static guru.bonacci.kafka.connect.tile38.Constants.SET_RESERVED;
import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.strip;

import java.util.List;
import java.util.Set;

import org.apache.kafka.common.config.ConfigException;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Class that facilitates Redis command generation based on sinking records
 */
@Getter 
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor(access = PRIVATE)
public class CommandTemplate {

	// The command string as in the connector configuration
	// example: foo event.id FIELD route event.rou POINT event.lat event.lon 
	private final String cmdString;
	
	// The first term of the command string indicates the command's key field 
	// example: foo 
	private final String key;
	
	// All command terms starting with 'event.'
	// These terms are substituted in dynamic command generation
	// example: {event.id, event.rou, event.lat, event.lon}
	private final Set<String> terms;
	
	/**
	 * Command format:
	 * SET key id [FIELD name value ...] [EX seconds] [NX|XX] (OBJECT
	 * geojson)|(POINT lat lon [z])|(BOUNDS minlat minlon maxlat maxlon)|(HASH
	 * geohash)|(STRING value)
	 * 
	 * 'Compile time' verification only on the first term, it being a valid key.
	 * Otherwise invalid commands arise 'run time'.
	 */
	public static CommandTemplate from(String cmdString) {
		if (isBlank(cmdString)) {
			throw new ConfigException("Command cannot be blank");
		}
		
		String trimmedCmdString = strip(cmdString);
		// List to keep order
		List<String> cmdTerms = copyOf(trimmedCmdString.split(" "));

		String key = cmdTerms.get(0);
	    if (key.startsWith(TOKERATOR))
	    	throw new ConfigException("Command {} cannot start with 'event.'. The first command word is the key. Check the docs: {}", 
	    			cmdString, "https://tile38.com/commands/set/");

	    if (of(SET_RESERVED, DEL_RESERVED).contains(key.toUpperCase()))
	    	throw new ConfigException("SET and DEL are reserved words and can be ommitted. Command {} cannot start with either one.", cmdString);
	    	
	    // no more need for order
	    Set<String> terms = newHashSet(cmdTerms);
		// remove all command terms that do not start with 'event.'
		terms.removeIf(s -> !s.startsWith(TOKERATOR));

	    return new CommandTemplate(trimmedCmdString, key, terms);
	}
}
