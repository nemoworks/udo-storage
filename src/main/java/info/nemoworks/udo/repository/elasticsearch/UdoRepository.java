package info.nemoworks.udo.repository.elasticsearch;


import org.springframework.boot.autoconfigure.data.web.SpringDataWebProperties.Pageable;
import org.springframework.data.domain.Page;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import info.nemoworks.udo.model.Udo;


@Repository
public interface UdoRepository extends ElasticsearchRepository<Udo, String> {
    
}
