package com.atguigu.es;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @BelongsProject: atguigu-project-new-221126java
 * @BelongsPackage: com.atguigu.es
 * @Author: Hywel
 * @CreateTime: 2023-03-22  20:26
 * @Description: TODO
 * @Version: 1.0
 */
@SpringBootTest
public class ESTest {
    @Autowired
    private RestHighLevelClient restHighLevelClient;

    public static final String INDEX = "product";

    /**
     *
     */
    @Test
    public void testGetIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest(INDEX);

        GetIndexResponse getIndexResponse = restHighLevelClient.indices().get(getIndexRequest, RequestOptions.DEFAULT);

        Map<String, MappingMetadata> mappings = getIndexResponse.getMappings();

        Set<String> strings = mappings.keySet();

        for (String key : strings) {
            MappingMetadata mappingMetadata = mappings.get(key);
            System.out.println("key = " + key);
            System.out.println("mappingMetadata = " + mappingMetadata.sourceAsMap());
        }

        Map<String, Settings> settings = getIndexResponse.getSettings();

        Set<String> keySet = settings.keySet();

        for (String s : keySet) {
            System.out.println(s);
            Settings settings1 = settings.get(s);
            System.out.println("settings1 = " + settings1);
        }
    }

    /**
     *
     */
    @Test
    public void testDeleteIndex() throws IOException {
        DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(INDEX);

        AcknowledgedResponse acknowledgedResponse = restHighLevelClient.indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
    }

    public static final String INDEX_NAME_SONG = "db_song";
    public static final String INDEX_NAME_HR = "db_hr";

    /**
     * 查询文档
     */
    @Test
    public void testGetDoc() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME_SONG);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchQueryBuilder("song_name", "apple"));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        float maxScore = hits.getMaxScore();
        System.out.println("maxScore = " + maxScore);
        TotalHits totalHits = hits.getTotalHits();
        System.out.println("totalHits = " + totalHits);
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            String id = searchHit.getId();
            System.out.println("id = " + id);
            float score = searchHit.getScore();
            System.out.println("score = " + score);
            Map<String, Object> sourceAsMap = searchHit.getSourceAsMap();
            Set<Map.Entry<String, Object>> entries = sourceAsMap.entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
    }

    /**
     * 高亮
     */
    @Test
    public void testHighLight() throws IOException {
        SearchRequest searchRequest = new SearchRequest();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(new MatchQueryBuilder("song_name", "apple"));

        searchRequest.source(searchSourceBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");
        highlightBuilder.field("song_name");

        searchSourceBuilder.highlighter(highlightBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();

        SearchHit[] searchHits = hits.getHits();

        for (SearchHit searchHit : searchHits) {
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            Set<Map.Entry<String, HighlightField>> entries = highlightFields.entrySet();
            for (Map.Entry<String, HighlightField> entry : entries) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }

    }

    /**
     *
     */
    @Test
    public void testAggs() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("agg_dept").field("department");

        searchSourceBuilder.aggregation(termsAggregationBuilder);

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Map<String, Aggregation> stringAggregationMap = aggregations.asMap();
        ParsedTerms parsedTerms = (ParsedTerms) stringAggregationMap.get("agg_dept");
        List<? extends Terms.Bucket> buckets = parsedTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            long docCount = bucket.getDocCount();
            System.out.println("docCount = " + docCount);
            Object key = bucket.getKey();
            System.out.println("key = " + key);
        }

    }

}
