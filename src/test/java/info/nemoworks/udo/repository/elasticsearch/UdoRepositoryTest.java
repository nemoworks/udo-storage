package info.nemoworks.udo.repository.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import info.nemoworks.udo.model.Udo;
import info.nemoworks.udo.model.UdoSchema;
import info.nemoworks.udo.storage.UdoPersistException;

@Import(ElasticsearchConfig.class)
@SpringBootTest(classes = { info.nemoworks.udo.repository.elasticsearch.UdoWrapperRepository.class })
@Testcontainers
public class UdoRepositoryTest {

    public static class FixedElasticsearchContainer extends ElasticsearchContainer {
        public FixedElasticsearchContainer() {
            super();
        }

        public FixedElasticsearchContainer configurePort() {
            super.addFixedExposedPort(9200, 9200);
            super.addFixedExposedPort(9300, 9300);
            return this;
        }
    }

    @Autowired
    UdoWrapperRepository repository;

    @Container
    private static final FixedElasticsearchContainer es = new FixedElasticsearchContainer().configurePort();

    @Test
    public void assertContainerRunning() {
        assertTrue(es.isRunning());
    }

    @Test
    public void insertOneUdo() throws JsonParseException, IOException, UdoPersistException {

        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode actualObj = mapper.readTree(jsonString).deepCopy();

        UdoSchema schema = new UdoSchema(actualObj);
        schema.setId("schema-1");

        Udo udo = new Udo(schema, actualObj);
        udo.setId("udo-1");
        System.out.println(udo.toJsonObject());
        assertNotNull(udo);

        repository.saveUdo(udo);

        System.out.print(repository.findAllUdos());
    }

}
