package guru.bonacci.kafka.connect.service;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.json.JsonConverter;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import guru.bonacci.kafka.connect.ElasticSinkConnectorConfig;
import guru.bonacci.kafka.connect.Record;
import guru.bonacci.kafka.connect.client.ElasticClient;
import guru.bonacci.kafka.connect.client.ElasticClientImpl;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticServiceImpl implements ElasticService {

	private Gson gson;
    private String indexName;
    private String typeName;
    private ElasticClient elasticClient;

    public ElasticServiceImpl(ElasticClient elasticClient, ElasticSinkConnectorConfig config) {
        indexName = config.getIndexName();
        typeName = config.getTypeName();

        prepareJsonConverters();

        if(elasticClient == null) {
            try {
                elasticClient = new ElasticClientImpl(config.getElasticUrl(), config.getElasticPort());
            } catch (UnknownHostException e) {
                log.error("The host is unknown, exception stacktrace: " + e.toString());
            }
        }

        this.elasticClient = elasticClient;
    }

    @Override
    public void process(Collection<String> recordsAsString){
        List<Record> recordList = new ArrayList<>();

        recordsAsString.forEach(recordStr -> {
            try {
                JsonObject recordAsJson = gson.fromJson(recordStr, JsonObject.class);
                log.info("another one " + recordAsJson);
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
            elasticClient.bulkSend(recordList, indexName, typeName);
        }
        catch (Exception e) {
            log.error("Something failed, here is the error:");
            log.error(e.toString());
        }
    }

    @Override
    public void closeClient() throws IOException {
        elasticClient.close();
    }

    private void prepareJsonConverters() {
        JsonConverter converter = new JsonConverter();
        converter.configure(Collections.singletonMap("schemas.enable", "false"), false);
	
        gson = new GsonBuilder()
                .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
                .setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
                .create();
    }
}
