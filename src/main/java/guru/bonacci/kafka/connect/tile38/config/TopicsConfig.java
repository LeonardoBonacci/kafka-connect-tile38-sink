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
package guru.bonacci.kafka.connect.tile38.config;

import static guru.bonacci.kafka.connect.tile38.Constants.COMMAND_PREFIX;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;

import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * Topics configured to have a Redis command.
 */
@Getter
@ToString
@RequiredArgsConstructor(access = PRIVATE)
public class TopicsConfig {

	private final Map<String, String> cmdsByTopic;

	
	public Set<String> configuredTopics() {
		return cmdsByTopic.keySet();
	}

	public static TopicsConfig from(Map<String, String> config) {
		// Filter and remove the prefix from the topic config keys
		return new TopicsConfig(config.entrySet().stream()
				.filter(prop -> prop.getKey().startsWith(COMMAND_PREFIX))
				.collect(toMap(k -> k.getKey().replace(COMMAND_PREFIX, ""), 
			                   v -> v.getValue())));
	}
}
