package guru.bonacci.kafka.connect.tile38;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;

import guru.bonacci.kafka.connect.tile38.config.Tile38SinkConnectorConfig;

public class Tile38SinkConnector extends SinkConnector {

	private Map<String, String> configProperties;

	@Override
	public String version() {
		return Version.getVersion();
	}

	@Override
	public void start(Map<String, String> props) {
		try {
			configProperties = props;
			new Tile38SinkConnectorConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException("Couldn't start Tile38SinkConnector due to configuration error", e);
		}
	}

	@Override
	public Class<? extends Task> taskClass() {
		return Tile38SinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>();
		taskProps.putAll(configProperties);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(taskProps);
		}
		return taskConfigs;
	}

	@Override
	public void stop() throws ConnectException {
	}

	@Override
	public ConfigDef config() {
		return Tile38SinkConnectorConfig.conf();
	}
}
