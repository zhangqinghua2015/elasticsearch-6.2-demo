package com.zqh.elasticsearch.service;

import com.zqh.elasticsearch.factory.ClientFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @discription:
 * @date: 2019/09/26 11:31
 */
public class ElasticsearchService {

    private static Logger logger = LogManager.getLogger(ElasticsearchService.class);

    public void createIndex(String indexName, String indexVersion, String type) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("properties")
                    .startObject("title").field("type", "keyword").endObject()
                    .startObject("content").field("type", "keyword").endObject()
                    .endObject()
                .endObject();
        System.out.println(xContentBuilder.string());

        CreateIndexRequest request = new CreateIndexRequest(indexName + "_" + indexVersion)
                .settings(Settings.builder()
                        .put("index.number_of_shards", 2)
                        .put("index.number_of_replicas", 1)
                )
                .alias(new Alias(indexName))
//                .mapping(type, , XContentType.JSON)
                .mapping(type, xContentBuilder)
                /**Timeout to wait for the all the nodes to acknowledge the index creation as a String*/
//                .timeout(TimeValue.timeValueMinutes(2))
                /**Timeout to wait for the all the nodes to acknowledge the index creation as a TimeValue*/
//                .timeout("2m")
                /**Timeout to connect to the master node as a TimeValue*/
//                .masterNodeTimeout(TimeValue.timeValueMinutes(1))
                /**Timeout to connect to the master node as a String*/
//                .masterNodeTimeout("1m")
                /**The number of active shard copies to wait for before the create index API returns a response, as an int.*/
//                .waitForActiveShards(2)
                /**The number of active shard copies to wait for before the create index API returns a response, as an ActiveShardCount.*/
//                .waitForActiveShards(ActiveShardCount.DEFAULT)
                ;

        /** Synchronous Execution*/
        CreateIndexResponse createIndexResponse = ClientFactory.getRestHighLevelClient().indices().create(request);
        /** Asynchronous Execution*/
        /*client.indices().createAsync(request, new ActionListener<CreateIndexResponse>() {
            @Override
            public void onResponse(CreateIndexResponse createIndexResponse) {

            }

            @Override
            public void onFailure(Exception e) {

            }
        });*/
        /** Indicates whether all of the nodes have acknowledged the request*/
        boolean acknowledged = createIndexResponse.isAcknowledged();
        /** Indicates whether all of the nodes have acknowledged the request*/
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
    }


    public void openIndex(String indexName) throws IOException {
        OpenIndexRequest request = new OpenIndexRequest(indexName)
                .indicesOptions(IndicesOptions.strictExpandOpen())
                ;
        ClientFactory.getRestHighLevelClient().indices().open(request);
    }

    public void closeIndex(String indexName) throws IOException {
        CloseIndexRequest request = new CloseIndexRequest(indexName)
                .indicesOptions(IndicesOptions.lenientExpandOpen())
                ;
        ClientFactory.getRestHighLevelClient().indices().close(request);
    }

    /**
     *
     * @param indexName 索引名，不能是别名
     * @throws IOException
     */
    public void deleteIndex(String indexName) throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest(indexName)
                ;
        ClientFactory.getRestHighLevelClient().indices().delete(request);
    }

    public void getDoc(String indexName, String indexType, String documentId) throws IOException {

        /**Configure source inclusion for specific fields*/
        String[] includes = new String[]{"title", "*tent"};
        String[] excludes = Strings.EMPTY_ARRAY;

        /**Configure source exclusion for specific fields*/
        /*String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = new String[]{"message"};*/

        GetRequest request = new GetRequest(indexName, indexType, documentId)
                /**Disable source retrieval, enabled by default*/
                .fetchSourceContext(new FetchSourceContext(false))
                .fetchSourceContext(new FetchSourceContext(true, includes, excludes))
                /**Configure retrieval for specific stored fields (requires fields to be stored separately in the mappings)*/
                .storedFields("title")
                ;

        try {
            GetResponse getResponse = ClientFactory.getRestHighLevelClient().get(request);
            /**Retrieve the message stored field (requires the field to be stored separately in the mappings)*/
//            String title = getResponse.getField("title").getValue();

            String index = getResponse.getIndex();
            System.out.println(index);
            String type = getResponse.getType();
            System.out.println(type);
            String id = getResponse.getId();
            System.out.println(id);
            if (getResponse.isExists()) {
                long version = getResponse.getVersion();
                System.out.println(version);
                /**Retrieve the document as a String*/
                String sourceAsString = getResponse.getSourceAsString();
                System.out.println(sourceAsString);
                /**Retrieve the document as a Map<String, Object>*/
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                /**Retrieve the document as a byte[]*/
                byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            } else {
                /**Handle the scenario where the document was not found.
                 * Note that although the returned response has 404 status code,
                 * a valid GetResponse is returned rather than an exception thrown.
                 * Such response does not hold any source document and its isExists method returns false.*/
                System.out.println("document was not found.");
            }
        } catch (ElasticsearchException e) {
            e.printStackTrace();
            if (e.status() == RestStatus.NOT_FOUND) {
                /**Handle the exception thrown because the index does not exist*/
            } else if (e.status() == RestStatus.CONFLICT) {
                /**The raised exception indicates that a version conflict error was returned*/
            }
        }
    }

    public void addDoc(String indexName, String indexType, String docId) throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("title", "ElasticSearch的JavaAPI创建Mapping");
        jsonMap.put("content", "ElasticSearch的JavaAPI创建Mapping内容 ");
        jsonMap.put("message", "trying out Elasticsearch");
        IndexRequest indexRequest = new IndexRequest(indexName, indexType, docId)
                .source(jsonMap)
//                .opType(DocWriteRequest.OpType.CREATE)
                ;

        /*IndexRequest indexRequest = new IndexRequest(indexName, indexType, docId)
                .source("title", "ElasticSearch的JavaAPI创建Mapping",
                        "content", "ElasticSearch的JavaAPI创建Mapping内容"),
                        "message", "trying out Elasticsearch");*/

        try {
            IndexResponse indexResponse = ClientFactory.getRestHighLevelClient().index(indexRequest);
            String index = indexResponse.getIndex();
            System.out.println(index);
            String type = indexResponse.getType();
            System.out.println(type);
            String id = indexResponse.getId();
            System.out.println(id);
            long version = indexResponse.getVersion();
            System.out.println(version);
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("create");
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("update");
            }
            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                System.out.println(shardInfo.getSuccessful());
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    System.out.println(reason);
                }
            }
        } catch (ElasticsearchException e) {
            e.printStackTrace();
            if (e.status() == RestStatus.CONFLICT) {

            }
        }
    }

    public void updDoc(String indexName, String indexType, String docId) throws IOException {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("title", "ElasticSearch的JavaAPI创建Mapping upd");
        jsonMap.put("content", "ElasticSearch的JavaAPI创建Mapping内容 upd");
        UpdateRequest updateRequest = new UpdateRequest(indexName, indexType, docId)
                .doc(jsonMap);

        UpdateResponse updateResponse = ClientFactory.getRestHighLevelClient().update(updateRequest);
        long version = updateResponse.getVersion();
        System.out.println(version);
        System.out.println(updateResponse.getResult());
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.DELETED) {

        } else if (updateResponse.getResult() == DocWriteResponse.Result.NOOP) {

        }
    }

    public void delDoc(String indexName, String indexType, String docId) throws IOException {
        DeleteRequest request = new DeleteRequest(indexName, indexType, docId);
        DeleteResponse deleteResponse = ClientFactory.getRestHighLevelClient().delete(request);
    }

    public void bulkAddDoc(String indexName, String indexType, String docId) throws IOException, InterruptedException {
        BulkRequest request = new BulkRequest()
                .add(new DeleteRequest(indexName, indexType, docId))
                .add(new IndexRequest(indexName, indexType, docId).source(XContentType.JSON, "title", "title1", "content", "content1"))
                .add(new UpdateRequest(indexName, indexType, docId).doc(XContentType.JSON, "title", "title upd"));

        BulkResponse bulkResponse = ClientFactory.getRestHighLevelClient().bulk(request);
        for (BulkItemResponse bulkItemResponse : bulkResponse) {
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();

            if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                    || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                IndexResponse indexResponse = (IndexResponse) itemResponse;
                System.out.println(indexResponse.toString());

            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                System.out.println(updateResponse.toString());

            } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                System.out.println(deleteResponse.toString());
            }
        }

        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                int numberOfActions = request.numberOfActions();
                logger.debug("Executing bulk [{}] with {} requests", executionId, numberOfActions);
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (response.hasFailures()) {
                    logger.warn("Bulk [{}] executed with failures", executionId);
                } else {
                    logger.debug("Bulk [{}] completed in {} milliseconds", executionId, response.getTook().getMillis());
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                logger.error("Failed to execute bulk", failure);
            }
        };
        BulkProcessor bulkProcessor = BulkProcessor.builder(ClientFactory.getRestHighLevelClient()::bulkAsync, listener)
                .setBulkActions(500)
                .setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB))
                .setConcurrentRequests(3)
                .setFlushInterval(TimeValue.timeValueSeconds(10L))
                .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
                .build();

        bulkProcessor.add(new DeleteRequest(indexName, indexType, docId))
                .add(new IndexRequest(indexName, indexType, docId).source(XContentType.JSON, "title", "title1", "content", "content1"))
                .add(new UpdateRequest(indexName, indexType, docId).doc(XContentType.JSON, "title", "title upd"));
//        bulkProcessor.flush();
//        bulkProcessor.awaitClose(3, TimeUnit.MINUTES);
    }

    public void searchDocs(String indexName, String field, String value) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
//                .query(QueryBuilders.matchAllQuery())
                .query(QueryBuilders.termQuery(field, value))
                .from(0)
                .size(1)
                .timeout(TimeValue.timeValueSeconds(10));

        SearchRequest searchRequest = new SearchRequest(indexName)
                .source(searchSourceBuilder);
        SearchResponse searchResponse = ClientFactory.getRestHighLevelClient().search(searchRequest);
        System.out.println(searchResponse.toString());



        String[] includeFields = new String[] {"title", "user", "innerObject.*"};
        String[] excludeFields = new String[] {"_type"};
        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery(field, value)
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);
        searchSourceBuilder = new SearchSourceBuilder().query(matchQueryBuilder)
                .fetchSource(false)
//                .fetchSource(includeFields, excludeFields)
                .sort(new ScoreSortBuilder().order(SortOrder.DESC)) // Sort descending by _score
                .sort(new FieldSortBuilder("_id").order(SortOrder.ASC)); // Also sort ascending by _id field
        searchResponse = ClientFactory.getRestHighLevelClient().search(new SearchRequest().indices(indexName).source(searchSourceBuilder));
        System.out.println(searchResponse.toString());


        /** Highlighting */
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("title");
        highlightTitle.highlighterType("plain");
        highlightBuilder.field(highlightTitle);
        HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("content");
        highlightBuilder.field(highlightUser);
        searchSourceBuilder = new SearchSourceBuilder()
                .highlighter(highlightBuilder)
                .query(QueryBuilders.matchQuery(field, value));
        searchResponse = ClientFactory.getRestHighLevelClient().search(new SearchRequest().indices(indexName).source(searchSourceBuilder));
        System.out.println(searchResponse.toString());

    }

}
