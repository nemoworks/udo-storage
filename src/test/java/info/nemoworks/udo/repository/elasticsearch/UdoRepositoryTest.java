package info.nemoworks.udo.repository.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
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
    public void insertOneUdo() throws UdoPersistException {

        String jsonString = "{\n" +
                "  \"type\": \"object\",\n" +
                "  \"title\": \"VirtualMachineInstance\",\n" +
                "  \"properties\": {\n" +
                "    \"CPU\": {\n" +
                "      \"properties\": {\n" +
                "        \"cores\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"sockets\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"threads\": {\n" +
                "          \"type\": \"integer\"\n" +
                "        },\n" +
                "        \"model\": {\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        \"features\": {\n" +
                "          \"items\": {\n" +
                "            \"type\": \"embedded\",\n" +
                "            \"typeName\": \"CPUFeature\"\n" +
                "          },\n" +
                "          \"type\": \"array\"\n" +
                "        },\n" +
                "        \"dedicatedCpuPlacement\": {\n" +
                "          \"type\": \"boolean\"\n" +
                "        },\n" +
                "        \"isolateEmulatorThread\": {\n" +
                "          \"type\": \"boolean\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"additionalProperties\": false,\n" +
                "      \"title\": \"CPU\",\n" +
                "      \"type\": \"object\"\n" +
                "    },\n" +
                "    \"CPUFeature\": {\n" +
                "      \"required\": [\n" +
                "        \"name\"\n" +
                "      ],\n" +
                "      \"properties\": {\n" +
                "        \"name\": {\n" +
                "          \"type\": \"string\"\n" +
                "        },\n" +
                "        \"policy\": {\n" +
                "          \"type\": \"string\"\n" +
                "        }\n" +
                "      },\n" +
                "      \"additionalProperties\": false,\n" +
                "      \"title\": \"CPUFeature\",\n" +
                "      \"type\": \"object\"\n" +
                "    }\n" +
                "  }\n" +
                "}";

        JsonObject data = new Gson().fromJson(jsonString,JsonObject.class);
        UdoSchema schema = new UdoSchema(data);
        schema.setId("schema-1");

        Udo udo = new Udo(schema, data);
        assertNotNull(udo);
        Udo udo1 = repository.saveUdo(udo);
        System.out.println(udo1.toJsonObject());

        Udo udoById = repository.findUdoById(udo1.getId());

        System.out.println(udoById.toJsonObject());
    }

}
