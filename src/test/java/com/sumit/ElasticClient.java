package com.sumit;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sumit on 11/13/15.
 */
public class ElasticClient {
    private static Client client;

    private static Client getTransportClient() {
        if (client != null)
            return client;
        try {
            client = TransportClient.builder().build()
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return client;

    }

    public static Map<String, Object> getJsonDocument(String title, String content, Date postDate,
                                                      String[] tags, String author) {

        Map<String, Object> jsonDocument = new HashMap<String, Object>();

        jsonDocument.put("title", title);
        jsonDocument.put("content", content);
        jsonDocument.put("postDate", postDate);
        jsonDocument.put("tags", tags);
        jsonDocument.put("author", author);

        return jsonDocument;
    }

    public static void indexData(Map<String, Object> document) {
        client = getTransportClient();
        IndexRequestBuilder indexRequestBuilder = client.prepareIndex("articles", "article");
        indexRequestBuilder.setSource(document);
        indexRequestBuilder.execute().actionGet();
    }

    public static void search() {
        Client client = getTransportClient();
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("articles");
        SearchResponse searchResponse = searchRequestBuilder.setQuery(getSearchQuery())
                .execute()
                .actionGet();
        SearchHit[] results = searchResponse.getHits().getHits();
        System.out.println("Total-Hits="+searchResponse.getHits().getTotalHits());
        for (SearchHit hit : results) {
            System.out.println("------------------------------");
            Map<String,Object> result = hit.getSource();
            System.out.println(result);
        }


    }

    private static QueryBuilder getSearchQuery() {
        //TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("content", "scala");
        BoolQueryBuilder builder = QueryBuilders.boolQuery();
        BoolQueryBuilder filter = QueryBuilders.boolQuery().
                filter(builder.should(QueryBuilders.termQuery("content", "elastic")).should(QueryBuilders.termQuery("content", "scala"))
                        /*must(QueryBuilders.termQuery("content", "elastic")).
                        must(QueryBuilders.termQuery("author", "sengar"))
                        .should()*/
                        );
        String s = filter.toString();
        System.out.println("query="+s);

        return filter;
    }

    private static BoolQueryBuilder getOrClause(BoolQueryBuilder queryBuilder){
        BoolQueryBuilder orClause = queryBuilder.should(QueryBuilders.termQuery("content", "elastic")).should(QueryBuilders.termQuery("content", "scala"));
        return orClause;
    }

    public static void main(String[] args) {
        //indexData(getJsonDocument("Elastic", "Elastic is cooler", new Date(), new String[]{"elastic"}, "Sengar"));
        search();
        client.close();

    }
}
