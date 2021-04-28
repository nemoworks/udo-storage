package info.nemoworks.udo.repository.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import info.nemoworks.udo.model.Udo;
import lombok.AllArgsConstructor;
import lombok.Data;

@Document(indexName = "udo")
@Data
@AllArgsConstructor
public class UdoDocument {

    @Id
    private String id;

    @Field
    private JsonNode schema;

    @Field
    private JsonNode data;

    public Udo toUdo(){
        return new Udo(this.getId(), this.getSchema(), this.getData());
    }

}
