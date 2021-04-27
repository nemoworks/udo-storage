package info.nemoworks.udo.repository.elasticsearch;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import info.nemoworks.udo.model.Udo;
import info.nemoworks.udo.model.UdoSchema;

@RunWith(SpringRunner.class)
@SpringBootTest
@Testcontainers
public class UdoRepositoryTest {

    @Autowired
    UdoRepository repository;

    @Container
    public ElasticsearchContainer es = new ElasticsearchContainer().withExposedPorts(9200);

    @BeforeEach
    public void setUp() {

    }

    @Test
    public void insertOneUdo() throws JsonParseException, IOException {

        String jsonString = "{\"k1\":\"v1\",\"k2\":\"v2\"}";

        ObjectMapper mapper = new ObjectMapper();
        JsonNode actualObj = mapper.readTree(jsonString);

        UdoSchema schema = new UdoSchema(actualObj);
        schema.setId("schema-1");

        Udo udo = new Udo(schema, actualObj);
        udo.setId("udo-1");
        repository.save(udo);
    }

}
