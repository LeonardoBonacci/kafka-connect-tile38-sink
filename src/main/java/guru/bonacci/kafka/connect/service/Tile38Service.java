package guru.bonacci.kafka.connect.service;

import java.util.Collection;

public interface Tile38Service {

	void process(Collection<String> recordsAsStrings);
    void closeClient();
}
