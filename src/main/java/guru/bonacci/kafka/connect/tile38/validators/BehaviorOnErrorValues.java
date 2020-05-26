package guru.bonacci.kafka.connect.tile38.validators;

import java.util.Locale;

import org.apache.kafka.common.config.ConfigDef;

public enum BehaviorOnErrorValues {

	IGNORE, FAIL;

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

		// Overridden here so that ConfigDef.toEnrichedRst shows possible values
		// correctly
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
