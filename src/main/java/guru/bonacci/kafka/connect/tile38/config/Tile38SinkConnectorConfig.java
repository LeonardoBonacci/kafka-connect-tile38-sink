package guru.bonacci.kafka.connect.tile38.config;

import static com.google.common.collect.Sets.symmetricDifference;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_CONFIG;
import static org.apache.kafka.connect.sink.SinkTask.TOPICS_REGEX_CONFIG;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;

import guru.bonacci.kafka.connect.tile38.commands.CommandTemplates;
import guru.bonacci.kafka.connect.tile38.validators.BehaviorOnErrorValues;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.SocketOptions;
import lombok.Getter;


public class Tile38SinkConnectorConfig extends AbstractConfig {

	public static final String TILE38_HOST = "tile38.host";
	private static final String TILE38_HOST_DOC = "Tile38 server host.";
	public static final String TILE38_PORT = "tile38.port";
	private static final String TILE38_PORT_DOC = "Tile38 server host port number.";

	public static final String FLUSH_TIMEOUT = "flush.timeout.ms";
	private static final String FLUSH_TIMEOUT_DOC = "The timeout in milliseconds to use for periodic flushing.";

	public static final String BEHAVIOR_ON_ERROR = "behavior.on.error";
	private static final String BEHAVIOR_ON_ERROR_DOC = "Error handling behavior setting. Valid options are 'LOG' and 'FAIL'.";

	public static final String SOCKET_TCP_NO_DELAY = "socket.tcp.no.delay.enabled";
	private static final String SOCKET_TCP_NO_DELAY_DOC = "Use TCP-no-delay.";

	public static final String SOCKET_KEEP_ALIVE = "socket.keep.alive.enabled";
	private static final String SOCKET_KEEP_ALIVE_DOC = "Enable keepalive.";

	public static final String SOCKET_CONNECT_TIMEOUT = "socket.connect.timeout.ms";
	private static final String SOCKET_CONNECT_TIMEOUT_DOC = "Wait ms before socket timeout.";

	public static final String REQUEST_QUEUE_SIZE = "request.queue.size";
	private static final String REQUEST_QUEUE_SIZE_DOC = "Max number of queued requests.";

	public static final String AUTO_RECONNECT = "auto.reconnect.enabled";
	private static final String AUTO_RECONNECT_DOC = "Lets the Redis client reconnect automatically.";


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
		 if (props.containsKey(TOPICS_REGEX_CONFIG)) {
            throw new ConfigException("'topics.regex' no supported, comma separated 'topics' instead");
		 }

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
				.define(TILE38_PORT, Type.INT, 9851, Importance.HIGH, TILE38_PORT_DOC)
				.define(BEHAVIOR_ON_ERROR, Type.STRING, BehaviorOnErrorValues.DEFAULT.toString(), BehaviorOnErrorValues.VALIDATOR, Importance.MEDIUM, BEHAVIOR_ON_ERROR_DOC)
				.define(FLUSH_TIMEOUT, Type.INT, 10000, Importance.LOW, FLUSH_TIMEOUT_DOC)
				.define(SOCKET_TCP_NO_DELAY, Type.BOOLEAN, SocketOptions.DEFAULT_SO_NO_DELAY, Importance.LOW, SOCKET_TCP_NO_DELAY_DOC)
				.define(SOCKET_KEEP_ALIVE, Type.BOOLEAN, SocketOptions.DEFAULT_SO_KEEPALIVE, Importance.LOW, SOCKET_KEEP_ALIVE_DOC)
				.define(SOCKET_CONNECT_TIMEOUT, Type.LONG, SocketOptions.DEFAULT_CONNECT_TIMEOUT_DURATION.toMillis(), Importance.LOW, SOCKET_CONNECT_TIMEOUT_DOC)
				.define(REQUEST_QUEUE_SIZE, Type.INT, ClientOptions.DEFAULT_REQUEST_QUEUE_SIZE, Importance.LOW, REQUEST_QUEUE_SIZE_DOC)
				.define(AUTO_RECONNECT, Type.BOOLEAN, ClientOptions.DEFAULT_AUTO_RECONNECT, Importance.LOW, AUTO_RECONNECT_DOC);
	}
	
	public String getHost() {
		return this.getString(TILE38_HOST);
	}

	public Integer getPort() {
		return this.getInt(TILE38_PORT);
	}
	
	public Integer getFlushTimeOut() {
		return this.getInt(FLUSH_TIMEOUT);
	}

	public Boolean getTcpNoDelay() {
		return this.getBoolean(SOCKET_TCP_NO_DELAY);
	}

	public Boolean getKeepAliveEnabled() {
		return this.getBoolean(SOCKET_KEEP_ALIVE);
	}
	
	public Long getConnectTimeout() {
		return this.getLong(SOCKET_CONNECT_TIMEOUT);
	}

	public Integer getRequestQueueSize() {
		return this.getInt(REQUEST_QUEUE_SIZE);
	}

	public Boolean getAutoReconnectEnabled() {
		return this.getBoolean(AUTO_RECONNECT);
	}
	
	public BehaviorOnErrorValues getBehaviorOnError() {
		return BehaviorOnErrorValues.forValue(this.getString(BEHAVIOR_ON_ERROR));
	}
}
