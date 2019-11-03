package guru.bonacci.kafka.connect.client;

import static guru.bonacci.kafka.connect.client.JsonReader.readDouble;
import static guru.bonacci.kafka.connect.client.JsonReader.readString;

import java.util.List;
import org.apache.commons.lang3.StringUtils;
import com.google.gson.JsonObject;
import guru.bonacci.kafka.connect.Constants;
import guru.bonacci.kafka.connect.Record;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38ClientImpl implements Tile38Client {

	private final RedisClient client;
	private final RedisCommands<String, String> sync;
	
	
    public Tile38ClientImpl(String url, int port) {
        client = RedisClient.create(String.format("redis://%s:%d", url, port));
        StatefulRedisConnection<String, String> connection = client.connect();
        sync = connection.sync();
    }

	/**
	 * SET key id [FIELD name value ...] [EX seconds] [NX|XX] (OBJECT
	 * geojson)|(POINT lat lon [z])|(BOUNDS minlat minlon maxlat maxlon)|(HASH
	 * geohash)|(STRING value)
	 */
    public void send(List<Record> records, String key, String objectType, String optionalFieldName) {
    	for (Record record : records) {
            JsonObject json = record.getJson();
            String id = readString(json, Constants.ID);
            Double lat = readDouble(json, Constants.LATITUDE);
            Double lon = readDouble(json, Constants.LONGITUDE);
            //TODO add elevation: int?
            
            CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
					            .add(key) 
					            .add(id);

            if (StringUtils.isNotBlank(optionalFieldName)) {
                //TODO support multiple fields 
	            args.add(Constants.FIELD_LABEL)
	            	.add(optionalFieldName) 
	            	.add(readDouble(json, optionalFieldName)); 
            }

            args.add(objectType) 
            	.add(lat) 
				.add(lon);

	        String resp = sync.dispatch(CommandType.SET, new StatusOutput<>(StringCodec.UTF8), args);
            log.info("tile38's response {}", resp);
        }
    }

    public void close() {
        client.shutdown();
    }
}
