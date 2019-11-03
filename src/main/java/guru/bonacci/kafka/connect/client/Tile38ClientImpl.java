package guru.bonacci.kafka.connect.client;

import java.util.List;

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
        client = RedisClient.create("redis://tile38:9851");
        StatefulRedisConnection<String, String> connection = client.connect();
        sync = connection.sync();
    }

    public void send(List<Record> records, String collectionName) {
    	for (Record bulkItem : records) {
            JsonObject dataAsObject = bulkItem.getData();
            String id = dataAsObject.get(Constants.DATA_ID).getAsString();
            
	        String resp = sync.dispatch(CommandType.SET,
	                    new StatusOutput<>(StringCodec.UTF8), new CommandArgs<>(StringCodec.UTF8)
	                            .add("trains") // internal collection name
	                            .add(id)
//	                            .add("FIELDS") // optional extra fields 
//	                            .add("route") // extra field name
//	                            .add(66) // extra field value - must be numeric
	                            .add("POINT") // point
	                            .add(33.01) // lat 
	                            .add(-115.03)); // lon
            log.info(resp);
        }
   
    }

    public void close() {
        client.shutdown();
    }
}
