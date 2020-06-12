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

import static com.google.common.collect.Maps.filterKeys;
import static com.google.common.collect.Maps.immutableEntry;
import static guru.bonacci.kafka.connect.tile38.Constants.EXPIRE_SUFFIX;
import static java.lang.Integer.valueOf;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;
import static lombok.AccessLevel.PRIVATE;
import static org.apache.commons.lang3.StringUtils.removeEnd;

import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import org.apache.kafka.common.config.ConfigException;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import guru.bonacci.kafka.connect.tile38.config.TopicsConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Wrapper class to access CommandTemplates by topic
 */
@Slf4j
@RequiredArgsConstructor(access = PRIVATE)
public class CommandTemplates {

	private final Map<String, CommandTemplate> cmds;

	
	public Stream<String> configuredTopics() {
		return cmds.keySet().stream();
	}

	public CommandTemplate templateForTopic(String topic) {
		return cmds.get(topic);
	}
	
	public static CommandTemplates from(TopicsConfig topics) {
		final Map<String, String> cmdsByTopic = topics.getCmdsByTopic();
		log.info("Creating command template data structure for {}", cmdsByTopic);
		
		// split the map in commands and expiration configs
		final Predicate<String> isExpire = key -> key.endsWith(EXPIRE_SUFFIX);
		final Map<String, String> expires = filterKeys(cmdsByTopic, Predicates.and(isExpire));
		final Map<String, String> cmds = filterKeys(cmdsByTopic, Predicates.not(isExpire));

		// create templates from command strings
		// in -> key: topic name - value: command string
		final Map<String, CommandTemplate> cmdTemplates = 
				cmds.entrySet().stream().map(cmdForTopic -> {
					CommandTemplate cmd = CommandTemplate.from(cmdForTopic.getValue());
				    return immutableEntry(cmdForTopic.getKey(), cmd);
				})
				.collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

		// add the expiration times to the templates
		for (Entry<String, String> expire : expires.entrySet()) {
			String expireTopicKey = removeEnd(expire.getKey(), EXPIRE_SUFFIX);
			try {
				cmdTemplates.computeIfAbsent(expireTopicKey, s -> { throw new IllegalArgumentException(); });
				cmdTemplates.get(expireTopicKey).setExpirationInSec(valueOf(expire.getValue()));
			} catch (IllegalArgumentException e) {
				throw new ConfigException(
						format("Error expire config for topic '%s'. Invalid value '%s'. Check the docs!", expireTopicKey, expire.getValue()));
			}	
		}
		
		// out -> key: topic name - value: command template
		return new CommandTemplates(cmdTemplates);
	}
}
