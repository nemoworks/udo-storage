package info.nemoworks.udo.repository.elasticsearch;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import info.nemoworks.udo.model.Udo;
import info.nemoworks.udo.model.UdoType;
import info.nemoworks.udo.storage.UdoNotExistException;
import info.nemoworks.udo.storage.UdoPersistException;

@Import(ElasticsearchConfig.class)
@SpringBootTest(classes = { info.nemoworks.udo.repository.elasticsearch.UdoWrapperRepository.class })
@Testcontainers
public class UdoRepositoryTest {

    private static final Logger logger = LoggerFactory.getLogger(UdoRepositoryTest.class);

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
    public void insetOneSchema() throws UdoPersistException, UdoNotExistException {
        String jsonString = "{'id': 1001, "
                + "'firstName': 'Lokesh',"
                + "'lastName': 'Gupta',"
                + "'email': 'howtodoinjava@gmail.com'}";
        JsonObject data = new Gson().fromJson(jsonString,JsonObject.class);
        UdoType type = new UdoType(data);
        UdoType udoType = repository.saveType(type);
        System.out.println(udoType.toJsonObject());
    }

    @Test
    public void testGetTypeById() throws UdoPersistException {
        String jsonString = "{'id': 1001, "
                + "'firstName': 'Lokesh',"
                + "'lastName': 'Gupta',"
                + "'email': 'howtodoinjava@gmail.com'}";
        JsonObject data = new Gson().fromJson(jsonString,JsonObject.class);
        UdoType schema = new UdoType(data);
        UdoType udoSchema = repository.saveType(schema);
        UdoType schemaById = repository.findTypeById(udoSchema.getId());
        System.out.println(schemaById.toJsonObject());
    }

    @Test
    public void testFindAllTypes() throws UdoPersistException {
        String jsonString = "{'id': 1001, "
                + "'firstName': 'Lokesh',"
                + "'lastName': 'Gupta',"
                + "'email': 'howtodoinjava@gmail.com'}";
        JsonObject data = new Gson().fromJson(jsonString,JsonObject.class);
        UdoType type = new UdoType(data);
        UdoType udoSchema = repository.saveType(type);
        repository.findAllTypes().forEach(udotype1 -> {
            System.out.println(udotype1.toJsonObject());
        });
    }

    @Test
    public void testDeleteTypeById() throws UdoPersistException, UdoNotExistException {
        String jsonString = "{'id': 1001, "
                + "'firstName': 'Lokesh',"
                + "'lastName': 'Gupta',"
                + "'email': 'howtodoinjava@gmail.com'}";
        JsonObject data = new Gson().fromJson(jsonString,JsonObject.class);
        UdoType schema = new UdoType(data);
        UdoType udoSchema = repository.saveType(schema);
        repository.deleteTypeById(udoSchema.getId());
        logger.info("number of schemas: "+String.valueOf(repository.findAllTypes().size()));
    }


    @Test
    public void insertOneUdo() throws UdoPersistException, UdoNotExistException {

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
        UdoType type = new UdoType(data);
        type.setId("schema-1");

        Udo udo = new Udo(type, data);
        assertNotNull(udo);
        Udo udo1 = repository.saveUdo(udo);
        System.out.println(udo1.toJsonObject());

        Udo udoById = repository.findUdoById(udo1.getId());

        System.out.println(udoById.toJsonObject());

        repository.deleteTypeById(udo1.getId());
    }

}
