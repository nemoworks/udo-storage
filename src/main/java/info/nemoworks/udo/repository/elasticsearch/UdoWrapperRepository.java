package info.nemoworks.udo.repository.elasticsearch;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.internal.LinkedTreeMap;
import info.nemoworks.udo.model.Udo;
import info.nemoworks.udo.model.UdoType;
import info.nemoworks.udo.storage.UdoNotExistException;
import info.nemoworks.udo.storage.UdoPersistException;
import info.nemoworks.udo.storage.UdoRepository;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Component;

@Component
public class UdoWrapperRepository implements UdoRepository {

    private final RestHighLevelClient client;

    private final Gson gson;

    private static final String INDEX_UDO = "udo";

    private static final String INDEX_TYPE = "udotype";

    private static final String UDO_TYPE_ID_LOC = "type.id";

    public UdoWrapperRepository(RestHighLevelClient client, Gson gson) {
        this.client = client;
        this.gson = gson;
    }

    @Override
    public void deleteTypeById(String id) throws UdoNotExistException {
        DeleteRequest deleteRequest = new DeleteRequest(INDEX_TYPE);
        deleteRequest.id(id);
        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            if (!deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)) {
                throw new UdoNotExistException("delete udoType failed.");
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
    public List<UdoType> findAllTypes() {
        List<UdoType> udoTypeList = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(INDEX_TYPE);
        SearchResponse response = null;
//        searchRequest.setBatchedReduceSize(20);
        try {
            response = client.search(searchRequest, RequestOptions.DEFAULT);
            response.getHits().forEach(hit -> {
                UdoType udoType = gson.fromJson(gson.toJson(hit.getSourceAsMap()), UdoType.class);
                udoType.setId(hit.getId());
                udoTypeList.add(udoType);
            });
            return udoTypeList;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public UdoType findTypeById(String id) {
        GetRequest getRequest = new GetRequest(INDEX_TYPE);
        getRequest.id(id);
        GetResponse getResponse = null;
        try {
            getResponse = client.get(getRequest, RequestOptions.DEFAULT);
            Map<String, Object> source = getResponse.getSource();
            UdoType udoType = gson.fromJson(gson.toJson(source), UdoType.class);
            udoType.setId(id);
            return udoType;
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
            Udo udo = gson.fromJson(gson.toJson(source), Udo.class);
            udo.setId(id);
            return udo;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

//    @Override
//    public Udo findUdoByUri(String uri) throws UdoNotExistException {
//        return null;
//    }

    @Override
    public List<Udo> findUdosByType(UdoType udoType) {
        String typeId = udoType.getId();
        return this.findUdosByTypeId(typeId);
    }

    @Override
    public List<Udo> findUdosByTypeId(String udoTypeId) {
        List<Udo> udoList = new ArrayList<>();
        SearchRequest searchRequest = new SearchRequest(INDEX_UDO);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery(UDO_TYPE_ID_LOC, udoTypeId));
        searchSourceBuilder.size(1000);
        searchRequest.source(searchSourceBuilder);
        try {
            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
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


    @Override
    public UdoType saveType(UdoType udoType) throws UdoPersistException {
        JsonObject type = udoType.toJsonObject();
        HashMap<String, LinkedTreeMap> hashMap = gson.fromJson(type.toString(), HashMap.class);
        IndexRequest request = new IndexRequest(INDEX_TYPE);
        if (udoType.getId() != null) {
            request.id(udoType.getId());
        }
        request.source(hashMap);
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
            if (response.getResult().equals(DocWriteResponse.Result.CREATED)) {
                udoType.setId(response.getId());
                return udoType;
            } else if (response.getResult().equals(DocWriteResponse.Result.UPDATED)) {
                return udoType;
            } else {
                throw new UdoPersistException("index udoType failed.");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Udo saveUdo(Udo udo) throws UdoPersistException {
        JsonObject object = udo.toJsonObject();
        HashMap<String, LinkedTreeMap> hashMap = gson.fromJson(object.toString(), HashMap.class);
        IndexRequest request = new IndexRequest(INDEX_UDO);
        if (udo.getId() != null) {
            request.id(udo.getId());
        }
        request.source(hashMap);
        IndexResponse response = null;
        try {
            response = client.index(request, RequestOptions.DEFAULT);
            if (response.getResult().equals(DocWriteResponse.Result.CREATED)) {
                udo.setId(response.getId());
                return udo;
            } else if (response.getResult().equals(DocWriteResponse.Result.UPDATED)) {
                return udo;
            } else {
                throw new UdoPersistException("index udo failed.");
            }
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
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(1000);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = null;

//        searchRequest.setBatchedReduceSize(20);
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
