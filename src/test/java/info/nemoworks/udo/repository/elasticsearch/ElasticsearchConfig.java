package info.nemoworks.udo.repository.elasticsearch;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.repository.config.EnableElasticsearchRepositories;

@TestConfiguration
@EnableElasticsearchRepositories(basePackages = "info.nemoworks.udo.repository.elasticsearch")
@ComponentScan(basePackages = { "info.nemoworks.udo.repository.elasticsearch" })
public class ElasticsearchConfig {

    @Bean
    RestHighLevelClient client() {
        ClientConfiguration clientConfiguration = ClientConfiguration.builder().connectedTo("localhost:9200").build();

        return RestClients.create(clientConfiguration).rest();
    }

    @Bean
    public ElasticsearchOperations elasticsearchTemplate() {
        return new ElasticsearchRestTemplate(client());
    }
}
