package info.nemoworks.udo.repository.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import info.nemoworks.udo.model.Udo;
import info.nemoworks.udo.model.UdoSchema;
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

    public Udo toUdo() {
        Udo udo = new Udo(new UdoSchema(this.getSchema()), this.getData());
        udo.setId(this.getId());

        return udo;
    }

}
