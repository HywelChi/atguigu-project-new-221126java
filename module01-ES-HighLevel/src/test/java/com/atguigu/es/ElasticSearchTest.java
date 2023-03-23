package com.atguigu.es;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.es.entity.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

@SpringBootTest
public class ElasticSearchTest {

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    @Test
    public void testRestHighLevelClient() throws Exception {
        System.out.println("restHighLevelClient = " + restHighLevelClient);
    }

    public static final String INDEX = "product";

    //创建索引
    @Test
    public void createIndex() {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX);
        try {
            createIndexRequest.mapping("{\n" +
                    "    \"properties\": {\n" +
                    "      \"name\": {\n" +
                    "        \"type\": \"keyword\",\n" +
                    "        \"index\": true,\n" +
                    "        \"store\": true\n" +
                    "      },\n" +
                    "      \"age\": {\n" +
                    "        \"type\": \"integer\",\n" +
                    "        \"index\": true,\n" +
                    "        \"store\": true\n" +
                    "      },\n" +
                    "      \"remark\": {\n" +
                    "        \"type\": \"text\",\n" +
                    "        \"index\": true,\n" +
                    "        \"store\": true,\n" +
                    "        \"analyzer\": \"ik_max_word\",\n" +
                    "        \"search_analyzer\": \"ik_smart\"\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }", XContentType.JSON);
            CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            System.out.println(createIndexResponse.isAcknowledged());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 查看索引
     */
    @Test
    public void testGetIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX);

        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);

        Map<String, MappingMetadata> mappings = getIndexResponse.getMappings();
        Set<String> mappingsKeySet = mappings.keySet();
        for (String key : mappingsKeySet) {
            MappingMetadata mappingMetadata = mappings.get(key);
            System.out.println("key = " + key);
            System.out.println("mappingMetadata = " + mappingMetadata.sourceAsMap());
        }

        Map<String, Settings> settings = getIndexResponse.getSettings();
        Set<String> settingsKeySet = settings.keySet();
        for (String key : settingsKeySet) {
            Settings setting = settings.get(key);
            System.out.println("key = " + key);
            System.out.println("setting = " + setting);
        }
    }

    /**
     * 删除索引
     */
    @Test
    public void testDeleteIndex() {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(INDEX);

        try {
            AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
            System.out.println(acknowledgedResponse.isAcknowledged());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建文档
     */
    @Test
    public void testCreateDocument() {
        IndexRequest indexRequest = new IndexRequest(INDEX);
        indexRequest.id("1");
        User user = new User();
        user.setAge(18);
        user.setName("Boruto");
        user.setRemark("漩涡博人");
        indexRequest.source(JSONObject.toJSONString(user), XContentType.JSON);

        try {
            IndexResponse index = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println(index.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 修改文档
     */
    @Test
    public void testUpdateDocument() throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(INDEX, "1");
        User user = new User();
        user.setName("博人");
        user.setRemark("ninja");
        updateRequest.doc(JSONObject.toJSONString(user), XContentType.JSON);
        UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, RequestOptions.DEFAULT);
        System.out.println(updateResponse.getResult());
    }

    /**
     * 根据Id查询
     */
    @Test
    public void testGetDoc() throws IOException {
        GetRequest request = new GetRequest(INDEX, "1");
        GetResponse response = restHighLevelClient.get(request, RequestOptions.DEFAULT);
        System.out.println(response.getSourceAsString());
    }

    /**
     * 批量操作
     */
    @Test
    public void testBulkDocument() throws IOException {
        BulkRequest request = new BulkRequest();
        User user = new User();
        for (int i = 0; i < 10; i++) {
            user.setAge(18 + i);
            user.setName("博人" + i);
            user.setRemark("ninja" + i);
            request.add(new IndexRequest(INDEX).id(String.valueOf(10 + i)).source(JSONObject.toJSONString(user), XContentType.JSON));
        }

        BulkResponse response = restHighLevelClient.bulk(request, RequestOptions.DEFAULT);
        for (BulkItemResponse itemResponse : response.getItems()) {
            System.out.println(itemResponse.isFailed());
        }

    }

    //删除文档
    @Test
    public void deleteDocument() {
        DeleteRequest request = new DeleteRequest(INDEX, "11");
        try {
            DeleteResponse response = restHighLevelClient.delete(request, RequestOptions.DEFAULT);
            System.out.println(response.getResult());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
