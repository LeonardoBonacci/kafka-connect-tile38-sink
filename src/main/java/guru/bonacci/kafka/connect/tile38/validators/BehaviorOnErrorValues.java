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
package guru.bonacci.kafka.connect.tile38.validators;

import java.util.Locale;

import org.apache.kafka.common.config.ConfigDef;

public enum BehaviorOnErrorValues {

	LOG, FAIL;

	public static final BehaviorOnErrorValues DEFAULT = FAIL;

	// Want values for "behavior.on.null.values" property to be case-insensitive
	public static final ConfigDef.Validator VALIDATOR = new ConfigDef.Validator() {
		private final ConfigDef.ValidString validator = ConfigDef.ValidString.in(names());

		@Override
		public void ensureValid(String name, Object value) {
			if (value instanceof String) {
				value = ((String) value).toLowerCase(Locale.ROOT);
			}
			validator.ensureValid(name, value);
		}

		@Override
		public String toString() {
			return validator.toString();
		}

	};

	public static String[] names() {
		BehaviorOnErrorValues[] behaviors = values();
		String[] result = new String[behaviors.length];

		for (int i = 0; i < behaviors.length; i++) {
			result[i] = behaviors[i].toString();
		}

		return result;
	}

	public static BehaviorOnErrorValues forValue(String value) {
		return valueOf(value.toUpperCase(Locale.ROOT));
	}

	@Override
	public String toString() {
		return name().toLowerCase(Locale.ROOT);
	}
}
