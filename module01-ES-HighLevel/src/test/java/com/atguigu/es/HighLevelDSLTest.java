package com.atguigu.es;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
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
 * @CreateTime: 2023-03-21  18:57
 * @Description: TODO
 * @Version: 1.0
 */
@SpringBootTest
public class HighLevelDSLTest {
    public static final String INDEX_NAME_SONG = "db_song";
    public static final String INDEX_NAME_HR = "db_hr";

    @Autowired
    private RestHighLevelClient restHighLevelClient;

    /**
     * 查询文档
     */
    @Test
    public void testMatchQuery() throws Exception {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME_SONG);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        searchSourceBuilder.query(new MatchQueryBuilder("song_name", "apple"));

        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        System.out.println("hits.getTotalHits() = " + hits.getTotalHits());

        SearchHit[] searchHits = hits.getHits();

        for (SearchHit searchHit : searchHits) {
            System.out.println("searchHit.getId() = " + searchHit.getId());
            System.out.println("searchHit.getScore() = " + searchHit.getScore());

            Set<Map.Entry<String, Object>> entries = searchHit.getSourceAsMap().entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
        }
    }

    /**
     * 高亮查询
     */
    @Test
    public void testHighlightSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME_SONG);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchQueryBuilder("song_name", "apple"));
        searchRequest.source(searchSourceBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field("song_name");
        highlightBuilder.preTags("<span style='color:red'>");
        highlightBuilder.postTags("</span>");

        searchSourceBuilder.highlighter(highlightBuilder);
        System.out.println(searchSourceBuilder);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println("searchResponse.getTook() = " + searchResponse.getTook());
        SearchHits hits = searchResponse.getHits();
        System.out.println("hits.getMaxScore() = " + hits.getMaxScore());
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit searchHit : searchHits) {
            Set<Map.Entry<String, Object>> entries = searchHit.getSourceAsMap().entrySet();
            for (Map.Entry<String, Object> entry : entries) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }
            Map<String, HighlightField> highlightFields = searchHit.getHighlightFields();
            Set<Map.Entry<String, HighlightField>> entrySet = highlightFields.entrySet();
            for (Map.Entry<String, HighlightField> stringHighlightFieldEntry : entrySet) {
                System.out.println(stringHighlightFieldEntry.getKey());
                System.out.println(stringHighlightFieldEntry.getValue());
            }
        }
    }

    /**
     * 聚合
     */
    @Test
    public void testAggregation() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME_HR);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        TermsAggregationBuilder termsAggregationBuilder = AggregationBuilders.terms("agg_dept").field("department");

        searchSourceBuilder.aggregation(termsAggregationBuilder);

        searchRequest.source(searchSourceBuilder);

        System.out.println(searchRequest);

        SearchResponse searchResponse = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = searchResponse.getAggregations();

        Map<String, Aggregation> stringAggregationMap = aggregations.asMap();

        ParsedTerms aggregation = (ParsedTerms) stringAggregationMap.get("agg_dept");

        List<? extends Terms.Bucket> buckets = aggregation.getBuckets();

        for (Terms.Bucket bucket : buckets) {
            System.out.println(bucket.getKey() + ":" + bucket.getDocCount());

        }
    }
}
