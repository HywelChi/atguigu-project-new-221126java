package com.atguigu.es.dao;

import com.atguigu.es.entity.Product;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProductDao extends ElasticsearchRepository<Product,Long> {
    List<Product> findByPriceBetween(Double price1, Double price2);
}