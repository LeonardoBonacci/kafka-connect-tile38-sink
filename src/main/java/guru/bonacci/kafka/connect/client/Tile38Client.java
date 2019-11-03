package guru.bonacci.kafka.connect.client;

import java.util.List;

import guru.bonacci.kafka.connect.Record;

public interface Tile38Client {

	void send(List<Record> records, String key, String objectType);
    void close();
}
