package com.atguigu.es;

import com.atguigu.es.dao.ProductDao;
import com.atguigu.es.entity.Product;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Sort;
import org.springframework.data.elasticsearch.core.ElasticsearchRestTemplate;

import java.util.List;
import java.util.Optional;

/**
 * @BelongsProject: atguigu-project-new-221126java
 * @BelongsPackage: com.atguigu.es
 * @Author: Hywel
 * @CreateTime: 2023-03-21  21:25
 * @Description: TODO
 * @Version: 1.0
 */
@SpringBootTest
public class ElasticSearchTest {
    @Autowired
    private ElasticsearchRestTemplate elasticsearchRestTemplate;

    @Autowired
    private ProductDao productDao;

    /**
     * 测试创建
     */
    @Test
    public void testInit(){

    }

    /**
     * 保存
     */
    @Test
    public void testSave(){
        Product product = new Product();
        product.setId(4L);
        product.setProductName("iphone4");
        product.setPrice(4444D);
        product.setStore(44);
        productDao.save(product);
    }

    /**
     * 测试根据Id查找
     */
    @Test
    public void testFindById(){
        Optional<Product> optional = productDao.findById(1L);
        Product product = optional.orElse(new Product());
        System.out.println("product = " + product);
    }
    
    /**
     * 排序
     */
    @Test
    public void testSort(){
        List<Product> productList = (List<Product>) productDao.findAll(Sort.by(Sort.Direction.DESC, "price"));
        productList.forEach(System.out::println);

    }

    /**
     * 自定义
     */
    @Test
    public void testDIYMethod(){
       List<Product> productList =  productDao.findByPriceBetween(2000D,4000D);

       productList.forEach(System.out::println);

    }
}
