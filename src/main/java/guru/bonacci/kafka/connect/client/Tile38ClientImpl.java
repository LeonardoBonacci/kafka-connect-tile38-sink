package guru.bonacci.kafka.connect.client;

import java.util.List;

import com.google.gson.JsonObject;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.codec.StringCodec;
import com.lambdaworks.redis.output.StatusOutput;
import com.lambdaworks.redis.protocol.CommandArgs;
import com.lambdaworks.redis.protocol.CommandType;

import guru.bonacci.kafka.connect.Constants;
import guru.bonacci.kafka.connect.Record;
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
            String id = JsonReader.readString(json, Constants.ID);
            Double lat = JsonReader.readDouble(json, Constants.LATITUDE);
            Double lon = JsonReader.readDouble(json, Constants.LONGITUDE);
            //TODO add elevation
            
            //TODO make optional and support multiple fields 
            Double f1 = JsonReader.readDouble(json, optionalFieldName);

	        String resp = sync.dispatch(CommandType.SET,
	                    new StatusOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8)
	                            .add(key) 
	                            .add(id)
	                            .add(Constants.FIELD_LABEL) 
	                            .add(optionalFieldName) 
	                            .add(f1) 
	                            .add(objectType) 
	                            .add(lat) 
	                            .add(lon));
            log.info("tile38's response {}", resp);
        }
    }

    public void close() {
        client.shutdown();
    }
}
