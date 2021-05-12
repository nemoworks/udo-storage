package info.nemoworks.udo.repository.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
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
    private static final String INDEX_SCHEMA = "udoSchema";

    public UdoWrapperRepository(RestHighLevelClient client, Gson gson) {
        this.client = client;
        this.gson = gson;
    }

    @Override
    public void deleteSchemaById(String id) throws UdoNotExistException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_SCHEMA);
        deleteRequest.id(id);
        DeleteResponse deleteResponse = null;
        try{
            deleteResponse = client.delete(deleteRequest,RequestOptions.DEFAULT);
            if(!deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)){
                throw new UdoNotExistException("delete udoSchema failed.");
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    @Override
    public void deleteUdoById(String id) throws UdoNotExistException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_UDO);
        deleteRequest.id(id);
        DeleteResponse deleteResponse = null;
        try{
            deleteResponse = client.delete(deleteRequest,RequestOptions.DEFAULT);
            if(!deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)){
                throw new UdoNotExistException("delete udo failed.");
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    @Override
    public List<UdoSchema> findAllSchemas() {
        // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
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
            if(response.getResult().equals(DocWriteResponse.Result.CREATED)){
                udoSchema.setId(response.getId());
                return udoSchema;
            }else throw new UdoPersistException("index udoSchema failed.");
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
            if(response.getResult().equals(DocWriteResponse.Result.CREATED)){
                udo.setId(response.getId());
                return udo;
            }else throw new UdoPersistException("index udo failed.");
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
    public List<Udo> findAllUdos(){
        return null;
    }

}
