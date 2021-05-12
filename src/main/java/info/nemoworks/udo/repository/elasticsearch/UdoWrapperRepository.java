package info.nemoworks.udo.repository.elasticsearch;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.internal.LinkedTreeMap;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

import info.nemoworks.udo.model.Udo;
import info.nemoworks.udo.model.UdoSchema;
import info.nemoworks.udo.storage.UdoNotExistException;
import info.nemoworks.udo.storage.UdoPersistException;
import info.nemoworks.udo.storage.UdoRepository;

@Component
public class UdoWrapperRepository implements UdoRepository {

    private final RestHighLevelClient client;

    private final Gson gson;

    private static final String INDEX_UDO = "udo";
    private static final String INDEX_SCHEMA = "udoschema";

    public UdoWrapperRepository(RestHighLevelClient client, Gson gson) {
        this.client = client;
        this.gson = gson;
    }

    @Override
    public void deleteSchemaById(String id) throws UdoNotExistException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_SCHEMA);
        deleteRequest.id(id);
        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            if (!deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)) {
                throw new UdoNotExistException("delete udoSchema failed.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void deleteUdoById(String id) throws UdoNotExistException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_UDO);
        deleteRequest.id(id);
        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            if (!deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)) {
                throw new UdoNotExistException("delete udo failed.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public List<UdoSchema> findAllSchemas() {
        List<UdoSchema> udoSchemaList = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(INDEX_SCHEMA);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
            response.getHits().forEach(hit -> {
                UdoSchema udoSchema = gson.fromJson(gson.toJson(hit.getSourceAsMap()), UdoSchema.class);
                udoSchema.setId(hit.getId());
                udoSchemaList.add(udoSchema);
            });
            return udoSchemaList;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public UdoSchema findSchemaById(String id) {
        GetRequest getRequest = new GetRequest(INDEX_SCHEMA);
        getRequest.id(id);
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> source = getResponse.getSource();
            return gson.fromJson(gson.toJson(source), UdoSchema.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Udo findUdoById(String id) {
        GetRequest getRequest = new GetRequest(INDEX_UDO);
        getRequest.id(id);
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> source = getResponse.getSource();
            return gson.fromJson(gson.toJson(source), Udo.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Udo> findUdosBySchema(UdoSchema schema) {
        List<Udo> udoSchemaList = new ArrayList<>();
        String schemaId = schema.getId();
        SearchRequest searchRequest = new SearchRequest(INDEX_UDO);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("schema.id", schemaId));

        return null;
    }

    @Override
    public UdoSchema saveSchema(UdoSchema udoSchema) throws UdoPersistException {
        JsonObject schema = udoSchema.toJsonObject();
//        Gson gson = new Gson();
        HashMap<String, LinkedTreeMap> hashMap = gson.fromJson(schema.toString(), HashMap.class);
        IndexRequest request = new IndexRequest(INDEX_SCHEMA);
        request.source(hashMap);
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
            if (response.getResult().equals(DocWriteResponse.Result.CREATED)) {
                udoSchema.setId(response.getId());
                return udoSchema;
            } else throw new UdoPersistException("index udoSchema failed.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Udo saveUdo(Udo udo) throws UdoPersistException {
        JsonObject object = udo.toJsonObject();
//        Gson gson = new Gson();
        HashMap<String, LinkedTreeMap> hashMap = gson.fromJson(object.toString(), HashMap.class);
        IndexRequest request = new IndexRequest(INDEX_UDO);
        request.source(hashMap);
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
            if (response.getResult().equals(DocWriteResponse.Result.CREATED)) {
                udo.setId(response.getId());
                return udo;
            } else throw new UdoPersistException("index udo failed.");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Udo sync(Udo udo) throws UdoPersistException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Udo> findAllUdos() {
        List<Udo> udoList = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(INDEX_UDO);
        SearchResponse response = null;
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
            response.getHits().forEach(hit -> {
                Udo udo = gson.fromJson(gson.toJson(hit.getSourceAsMap()), Udo.class);
                udo.setId(hit.getId());
                udoList.add(udo);
            });
            return udoList;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
