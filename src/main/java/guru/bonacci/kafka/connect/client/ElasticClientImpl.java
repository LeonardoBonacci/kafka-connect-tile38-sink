package guru.bonacci.kafka.connect.client;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

import com.google.gson.JsonObject;

import guru.bonacci.kafka.connect.Constants;
import guru.bonacci.kafka.connect.Record;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ElasticClientImpl implements ElasticClient {

	private JestClient client;

    public ElasticClientImpl(String url, int port) throws UnknownHostException {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(String.format("http://%s:%s", url, port))
                .multiThreaded(true)
                .build());

        client = factory.getObject();
    }


    public void bulkSend(List<Record> records, String index, String type) {
        Bulk.Builder bulkBuilder = new Bulk.Builder()
                .defaultIndex(index)
                .defaultType(type);

        for (Record bulkItem : records) {
            JsonObject dataAsObject = bulkItem.getData();
            String id = dataAsObject.get(Constants.DATA_ID).getAsString();
            bulkBuilder.addAction(new Index.Builder(dataAsObject).id(id).build());
        }

        try {
            BulkResult execute = client.execute(bulkBuilder.build());
            String errorMessage = execute.getErrorMessage();

            if(errorMessage != null) {
                log.error(errorMessage);
            }
        } catch (IOException e) {
            log.error(e.toString());
        }
    }

    public void close() throws IOException {
        client.shutdownClient();
    }
}
