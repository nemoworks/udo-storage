package info.nemoworks.udo.repository.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import info.nemoworks.udo.model.Udo;
import lombok.Data;

@Document(indexName = "udo")
@Data
public class UdoDocument {

    @Id
    private String id;

    @Field
    private JsonNode schema;

    @Field
    private JsonNode data;

    public UdoDocument(Udo udo) {

        this.id = udo.getId();
        this.schema = udo.getSchema().toJsonObject();
        this.data = udo.getData();

    }

}
