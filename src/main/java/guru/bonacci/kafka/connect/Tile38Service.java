package guru.bonacci.kafka.connect;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38Service {

	private Gson gson;
    private Tile38Client client;
    
    public Tile38Service(Tile38Client client, Tile38SinkConnectorConfig config) {

        prepareJsonConverters();

        if (client == null) {
            try {
                client = new Tile38Client(config.getTile38Url(), config.getTile38Port(), config.getKey());
            } catch (RuntimeException e) {
                log.error("Could not connect to host, why? {}", e.toString());
            }
        }

        this.client = client;
    }

    public void process(Collection<String> recordsAsString){
        List<Record> recordList = new ArrayList<>();

        recordsAsString.forEach(recordStr -> {
            try {
                JsonObject recordAsJson = gson.fromJson(recordStr, JsonObject.class);
                recordList.add(new Record(recordAsJson));
            }
            catch (JsonSyntaxException e) {
                log.error("Cannot deserialize json string, which is : " + recordStr);
            }
            catch (Exception e) {
                log.error("Cannot process data, which is : " + recordStr);
            }
        });

        try {
            client.send(recordList);
        }
        catch (Exception e) {
            log.error("Something went wrong ", e);
        }
    }

    public void closeClient() {
        client.close();
    }

    private void prepareJsonConverters() {
        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }
}
