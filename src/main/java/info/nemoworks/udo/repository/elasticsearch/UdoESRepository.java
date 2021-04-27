package info.nemoworks.udo.repository.elasticsearch;


import org.h2.mvstore.Page;
import org.springframework.boot.autoconfigure.data.web.SpringDataWebProperties.Pageable;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import info.nemoworks.udo.model.Udo;


@Repository
public interface UdoESRepository extends ElasticsearchRepository<Udo, String> {

    Page<Udo> findById(String id, Pageable pageable);
}
