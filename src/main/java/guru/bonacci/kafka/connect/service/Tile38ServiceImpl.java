package guru.bonacci.kafka.connect.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import guru.bonacci.kafka.connect.Record;
import guru.bonacci.kafka.connect.Tile38SinkConnectorConfig;
import guru.bonacci.kafka.connect.client.Tile38Client;
import guru.bonacci.kafka.connect.client.Tile38ClientImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Tile38ServiceImpl implements Tile38Service {

	private Gson gson;
    private String key;
    private String objectType;
    private String optionalField;
    private Tile38Client client;
    
    public Tile38ServiceImpl(Tile38Client client, Tile38SinkConnectorConfig config) {
        key = config.getKey();
        objectType = config.getObjectType();
        optionalField = config.getOptionalFieldName();

        prepareJsonConverters();

        if (client == null) {
            try {
                client = new Tile38ClientImpl(config.getTile38Url(), config.getTile38Port());
            } catch (RuntimeException e) {
                log.error("Could not connect to host, exception stacktrace: {}", e.toString());
            }
        }

        this.client = client;
    }

    @Override
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
            client.send(recordList, key, objectType, optionalField);
        }
        catch (Exception e) {
            log.error("Something went wrong ", e);
        }
    }

    @Override
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
