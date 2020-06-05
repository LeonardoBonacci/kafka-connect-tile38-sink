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

import static lombok.AccessLevel.PRIVATE;

import java.util.Map;

import com.google.common.collect.ImmutableMap;

import lombok.RequiredArgsConstructor;

/**
 * Wrapper class to access CommandGenerators by topic
 */
@RequiredArgsConstructor(access = PRIVATE)
public class CommandGenerators {

	private final Map<String, CommandGenerator> cmds;
	

	public CommandGenerator generatorForTopic(String topic) {
		return cmds.get(topic);
	}
	
	public static CommandGenerators from(Map<String, CommandGenerator> cmds) {
		if (cmds == null)
			cmds = ImmutableMap.of();

		return new CommandGenerators(cmds);
	}
}
