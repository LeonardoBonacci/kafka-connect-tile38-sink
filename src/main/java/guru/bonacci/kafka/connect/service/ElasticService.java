package guru.bonacci.kafka.connect.service;

import java.io.IOException;
import java.util.Collection;

public interface ElasticService {
    void process(Collection<String> recordsAsString);
    void closeClient() throws IOException;
}
