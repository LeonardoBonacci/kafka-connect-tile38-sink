package guru.bonacci.kafka.connect;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class Tile38SinkConnectorConfig extends AbstractConfig {

	public static final String TILE38_URL = "tile38.url";
	private static final String TILE38_URL_DOC = "Tile38 URL to connect.";
	public static final String TILE38_PORT = "tile38.port";
	private static final String TILE38_PORT_DOC = "Tile38 port to connect.";

	Topics topics;
	CommandTemplates cmdTemplates;
	
	public Tile38SinkConnectorConfig(Map<String, String> props) {
		this(conf(), props);
	}

	public Tile38SinkConnectorConfig(ConfigDef config, Map<String, String> props) {
		super(config, props);
		topics = Topics.from(props); 
		cmdTemplates = CommandTemplates.from(topics);
		validateAllTopics(props);
	}


	private void validateAllTopics(Map<String, String> props) {
//		 List<String> topics = Arrays.asList(
//				 props.get("topics")
//	                    .split(","));
    }
	
	public static ConfigDef conf() {
		return new ConfigDef()
				.define(TILE38_URL, Type.STRING, Importance.HIGH, TILE38_URL_DOC)
				.define(TILE38_PORT, Type.INT, Importance.HIGH, TILE38_PORT_DOC);
	}

	public String getTile38Url() {
		return this.getString(TILE38_URL);
	}

	public Integer getTile38Port() {
		return this.getInt(TILE38_PORT);
	}
}
