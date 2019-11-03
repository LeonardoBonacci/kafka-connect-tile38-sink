package guru.bonacci.kafka.connect.client;

import java.io.IOException;
import java.util.List;

import guru.bonacci.kafka.connect.Record;

public interface ElasticClient {
    void bulkSend(List<Record> records, String index, String type) throws IOException;

    void close() throws IOException;
}
