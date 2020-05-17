package guru.bonacci.kafka.connect.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import guru.bonacci.kafka.connect.Constants;

public class ObjectTypeValidator implements ConfigDef.Validator {

	@Override
	public void ensureValid(String name, Object value) {
		String objectType = (String) value;
//		if (!Constants.POINT_LABEL.equals(objectType)) {
//			throw new ConfigException(name, value, "Object type " + objectType + " not supported");
//		}
	}
}
