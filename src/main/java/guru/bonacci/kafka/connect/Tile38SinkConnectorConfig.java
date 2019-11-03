package guru.bonacci.kafka.connect;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import guru.bonacci.kafka.connect.validators.ObjectTypeValidator;

public class Tile38SinkConnectorConfig extends AbstractConfig {

	public static final String TILE38_URL = "tile38.url";
	private static final String TILE38_URL_DOC = "Tile38 URL to connect.";
	public static final String TILE38_PORT = "tile38.port";
	private static final String TILE38_PORT_DOC = "TILE38 port to connect.";
	public static final String KEY = "key.name";
	private static final String KEY_DOC = "The key (name) of your record.";
	public static final String OBJECT_TYPE = "object.type";
	private static final String OBJECT_TYPE_DOC = "Type of the Tile38 record you want to work sink.";
	public static final String OPTIONAL_FIELD_NAME = "optional.field.name";
	private static final String OPTIONAL_FIELD_NAME_DOC = "Double precision floating point field to sink.";

	
	public Tile38SinkConnectorConfig(ConfigDef config, Map<String, String> parsedConfig) {
		super(config, parsedConfig);
	}

	public Tile38SinkConnectorConfig(Map<String, String> parsedConfig) {
		this(conf(), parsedConfig);
	}

	public static ConfigDef conf() {
		return new ConfigDef()
				.define(TILE38_URL, Type.STRING, Importance.HIGH, TILE38_URL_DOC)
				.define(TILE38_PORT, Type.INT, Importance.HIGH, TILE38_PORT_DOC)
				.define(KEY, Type.STRING, Importance.HIGH, KEY_DOC)
				.define(OBJECT_TYPE, Type.STRING, Constants.POINT_LABEL, new ObjectTypeValidator(), Importance.MEDIUM, OBJECT_TYPE_DOC)
				.define(OPTIONAL_FIELD_NAME, Type.STRING, null, Importance.LOW, OPTIONAL_FIELD_NAME_DOC);
	}

	public String getTile38Url() {
		return this.getString(TILE38_URL);
	}

	public Integer getTile38Port() {
		return this.getInt(TILE38_PORT);
	}
	
	public String getKey() {
		return this.getString(KEY);
	}

	public String getObjectType() {
		return this.getString(OBJECT_TYPE);
	}
	
	public String getOptionalFieldName() {
		return this.getString(OPTIONAL_FIELD_NAME);
	}
}
