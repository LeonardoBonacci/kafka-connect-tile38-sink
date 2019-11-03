package guru.bonacci.kafka.connect.service;

import java.util.Collection;

public interface Tile38Service {

	void process(Collection<String> recordsAsString);
    
	void closeClient();
}
