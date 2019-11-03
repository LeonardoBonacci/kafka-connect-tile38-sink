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
    public void send(List<Record> records, String key, String objectType) {
    	for (Record record : records) {
            JsonObject dataAsObject = record.getData();
            String id = dataAsObject.get(Constants.DATA_ID).getAsString();
            
	        String resp = sync.dispatch(CommandType.SET,
	                    new StatusOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8)
	                            .add(key) 
	                            .add(id)
	                            .add("FIELD") // optional extra fields 
	                            .add("route") // extra field name
	                            .add(66) // extra field value - must be numeric
	                            .add(objectType) 
	                            .add(33.01) // lat 
	                            .add(-115.03)); // lon
            log.info("tile38's response {}", resp);
        }
    }

    public void close() {
        client.shutdown();
    }
}
