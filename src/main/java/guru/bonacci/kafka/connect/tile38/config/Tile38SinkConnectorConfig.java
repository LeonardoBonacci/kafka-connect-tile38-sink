package guru.bonacci.kafka.connect.tile38.config;

import static com.google.common.collect.Sets.symmetricDifference;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;

import org.apache.kafka.common.config.ConfigException;

import lombok.Getter;


public class Tile38SinkConnectorConfig extends AbstractConfig {

	public static final String TILE38_HOST = "tile38.host";
	private static final String TILE38_HOST_DOC = "Tile38 server host.";
	public static final String TILE38_PORT = "tile38.port";
	private static final String TILE38_PORT_DOC = "Tile38 server host port number.";

	@Getter	TopicsConfig topicsConfig;
	@Getter	CommandTemplates cmdTemplates;

	
	public Tile38SinkConnectorConfig(Map<String, String> props) {
		this(conf(), props);
	}

	public Tile38SinkConnectorConfig(ConfigDef config, Map<String, String> props) {
		super(config, props);
		
		topicsConfig = TopicsConfig.from(props); 
		cmdTemplates = CommandTemplates.from(topicsConfig);

		validateConfiguredTopics(props);
	}

	private void validateConfiguredTopics(Map<String, String> props) {
		 Set<String> topics = props.containsKey(TOPICS_CONFIG)
				? stream((props.get(TOPICS_CONFIG).trim()).split(",")).map(String::trim).collect(toSet()) 
				: emptySet();
				 
        Set<String> configuredTopics = this.topicsConfig.configuredTopics();

        if (!symmetricDifference(topics, configuredTopics).isEmpty()) {
            throw new ConfigException(format("There is a mismatch between topics defined into the property 'topics' %s and configured topics %s", 
            		topics, configuredTopics));
        }
    }
	
	public static ConfigDef conf() {
		return new ConfigDef()
				.define(TILE38_HOST, Type.STRING, "localhost", Importance.HIGH, TILE38_HOST_DOC)
				.define(TILE38_PORT, Type.INT, 9851, Importance.HIGH, TILE38_PORT_DOC);
	}

	public String getHost() {
		return this.getString(TILE38_HOST);
	}

	public Integer getPort() {
		return this.getInt(TILE38_PORT);
	}
}
