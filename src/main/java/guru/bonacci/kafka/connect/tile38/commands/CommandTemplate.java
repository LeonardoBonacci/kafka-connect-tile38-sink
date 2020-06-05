/**
 * 	Copyright 2020 Jeffrey van Helden (aabcehmu@mailfence.com)
 *	
 *	Licensed under the Apache License, Version 2.0 (the "License");
 *	you may not use this file except in compliance with the License.
 *	You may obtain a copy of the License at
 *	
 *	    http://www.apache.org/licenses/LICENSE-2.0
 *	
 *	Unless required by applicable law or agreed to in writing, software
 *	distributed under the License is distributed on an "AS IS" BASIS,
 *	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *	See the License for the specific language governing permissions and
 *	limitations under the License.
 */
package guru.bonacci.kafka.connect.tile38.commands;

import static com.google.common.collect.Sets.newHashSet;
import static guru.bonacci.kafka.connect.tile38.Constants.SET_TERM;
import static guru.bonacci.kafka.connect.tile38.Constants.TOKERATOR;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.strip;

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
	 */
	public static CommandTemplate from(String cmdString) {
		if (isBlank(cmdString)) {
			throw new ConfigException("Command cannot be empty");
		}

		// remove excessive spaces and strip the SET term from the command string
		final String[] setAndCmdString = strip(cmdString.replaceAll("[ ]+", " ")).split(" ", 2);
		
		if (!SET_TERM.equalsIgnoreCase(setAndCmdString[0])) 
	    	throw new ConfigException(String.format("Only SET commands are supported. Configured command '%s' starts with '%s'", cmdString, setAndCmdString[0]));

		if (setAndCmdString.length < 2) 
	    	throw new ConfigException(String.format("No key defined in command '%s'", cmdString));

		final String cmdStringWithoutSet = setAndCmdString[1];

		// strip the key from the command string
		final String[] keyAndCmdString = cmdStringWithoutSet.split(" ", 2);
		if (keyAndCmdString.length < 2) 
	    	throw new ConfigException(String.format("No id defined in command '%s'", cmdString));
		
		final Set<String> terms = newHashSet(cmdStringWithoutSet.split(" "));
		// remove all command terms that do not start with 'event.'
		terms.removeIf(s -> !s.startsWith(TOKERATOR));

	    return new CommandTemplate(cmdStringWithoutSet, keyAndCmdString[0], terms);
	}
}
