package info.nemoworks.udo.repository.elasticsearch;


import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;


@Repository
public interface UdoRepository extends ElasticsearchRepository<UdoDocument, String> {
    
}
