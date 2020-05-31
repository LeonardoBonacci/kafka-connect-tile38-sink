package guru.bonacci.kafka.connect.tile38.transforms;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import guru.bonacci.kafka.connect.tile38.writer.RecordConverter;
import lombok.extern.slf4j.Slf4j;

/**
 * Single Message Transformation that takes CDC records and converts them to use
 * the Simple structure for writing.
 */
@Slf4j
public class RemoveWhiteSpaces<R extends ConnectRecord<SinkRecord>> implements Transformation<SinkRecord> {

	private static final String FIELD_CONFIG = "field";

	public static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field to remove white spaces from.");


	private String fieldName;

	
	@Override
	public final void configure(Map<String, ?> props) {
		final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(FIELD_CONFIG);
    }

	//TODO applies same to records from all topics now
	@Override
	public final SinkRecord apply(SinkRecord record) {
		// Leave tombstone records untouched
		if (record.value() == null) {
			return record;
		}

		final Map<String, Object> value = new RecordConverter().convertValue(record);
		try {
			log.debug("record {}", value);
			// given the field name, retrieve the field value from the record
			// allows for nesting
			Object oldValue = PropertyUtils.getProperty(value, fieldName);

			if (oldValue == null) {
				// record does not contain required field 
				throw new IllegalAccessException();
			}

			log.debug("old value {}", oldValue);
			String newValue = String.valueOf(oldValue).replaceAll("\\s+", "");

			log.debug("new value {}", newValue);
			// allows for nesting
			PropertyUtils.setProperty(value, fieldName, newValue);
			
			return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), value, record.timestamp());
		} catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
			throw new IllegalArgumentException("Unknown field: " + fieldName);
		}
	}	

	@Override
	public final void close() {
	}

	@Override
	public final ConfigDef config() {
		return CONFIG_DEF;
	}
}