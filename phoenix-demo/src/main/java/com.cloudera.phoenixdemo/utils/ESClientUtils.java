package com.cloudera.phoenixdemo.utils;

import com.cloudera.phoenixdemo.constant.IMChatConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * @author Charles
 * @package com.hdjt.bigdata.util
 * @classname ESClientUtils
 * @description Elasticsearch工具类
 * @date 2019-5-6 11:58
 */
public class ESClientUtils {
    protected static Logger logger = LoggerFactory.getLogger(ESClientUtils.class);

    public static RestHighLevelClient getClient(){
        RestHighLevelClient client = null;
        List<Node> nodes = new ArrayList<Node>();
        try {
            String clusterName = "data-lakes";
            String nodeStr = "bigdata-dev-190:9200,bigdata-dev-191:9200,bigdata-dev-192:9200,bigdata-dev-193:9200,bigdata-dev-194:9200";
            if (StringUtils.isNotBlank(nodeStr)) {
                String[] hosts = nodeStr.split(",");
                for (String host : hosts) {
                    nodes.add(new Node(HttpHost.create(host)));
                }
                new RestHighLevelClient(
                        RestClient.builder(nodes.toArray(new Node[nodes.size()]))
                );
            }else {
                throw new Exception("未配置节点信息");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return client;
    }

    public static void main(String[] args) {
        String indexName = IMChatConstant.COMMUNITY_IM_CHAT_INDEX;
        String id = "1";

        try {
            GetResponse response = getResponse(indexName, id);
            System.out.println(response);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description: 查询数据, 精确查询
     */
    public static GetResponse getResponse(String indexName, String id) throws Exception {
        GetRequest request = new GetRequest(indexName, id);

        String[] includes = new String[]{"message", "*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext =
                new FetchSourceContext(true, includes, excludes);
        request.fetchSourceContext(fetchSourceContext);

        request.storedFields("message");
        GetResponse getResponse = getClient().get(request, RequestOptions.DEFAULT);
        String message = getResponse.getField("message").getValue();

        System.out.println(message);
        return getResponse;
    }





    /**
     * @Description: 查询数据, 返回Source ， 与getQueryResponse搭配使用
     */
//    public static Map<String, Object> getQueryResSource(GetResponse response) throws Exception {
//        setUp();
//        //查询添加的索引
//        // Index name
//        String _index = response.getIndex();
//        // Type name
//        String _type = response.getType();
//        // Document ID (generated or not)
//        String _id = response.getId();
//        // Version (if it's the first time you index this document, you will get: 1)
//        long _version = response.getVersion();
//        Map<String, GetField> fields = response.getFields();
//        for (Entry<String, GetField> fieldsEach : fields.entrySet()) {
//            System.out.println("fieldsEach.key:" + fieldsEach.getKey() + " " + "fieldsEach.value:" + fieldsEach.getValue().toString());
//        }
//        Map<String, Object> source = response.getSource();
//        for (Entry<String, Object> sourceEach : source.entrySet()) {
//            System.out.println("sourceEach.key:" + sourceEach.getKey() + " " + "sourceEach.value:" + sourceEach.getValue());
//        }
//        // 关闭client
//        closeClient();
//        return source;
//    }

    /**
     * @Description: 查询数据, 多条件查询
     */
//    public static SearchResponse getSearchResponse(String indexName, String type, Map<String, Object> queryCondition, Map<String, Object> filterCondition) throws Exception {
//        setUp();
//        //TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("", "");
//        MatchQueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("", "");
//        for (Entry<String, Object> conditionEach : queryCondition.entrySet()) {
//            matchQueryBuilder = QueryBuilders.matchQuery(conditionEach.getKey(), conditionEach.getValue());
//        }
//        SearchResponse response = client.prepareSearch(indexName)
//                .setTypes(type)
//                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
//                .setQuery(matchQueryBuilder)                 // Query
//                //.setPostFilter(QueryBuilders.rangeQuery("age").from(12).to(18))     // Filter
//                .setFrom(0).setSize(60).setExplain(false)    //true,有结果解释；false，没有
//                .get();
//        closeClient();
//        // 输出结果
//        SearchHit[] hits = response.getHits().hits();
//        for (int i = 0; i < hits.length; i++) {
//            SearchHit hiti = hits[i];
//            System.out.println(hiti.getSourceAsString());
//        }
//        System.out.println(response.toString());
//        return response;
    }
//
//    /**
//     * @throws
//     * @Title: getMultiSearchResponse
//     * @Description: TODO(多条件搜索)
//     * @param: @param indexName
//     * @param: @param type
//     * @param: @param queryCondition
//     * @param: @param filterCondition
//     * @param: @return
//     * @param: @throws Exception
//     * @return: MultiSearchResponse
//     */
//    public static MultiSearchResponse getMultiSearchResponse(String indexName, String type, Map<String, Object> queryCondition, Map<String, Object> filterCondition) throws Exception {
//        setUp();
//        SearchRequestBuilder srb1 = client
//                .prepareSearch().setQuery(QueryBuilders.queryStringQuery("elasticsearch")).setSize(1);
//        SearchRequestBuilder srb2 = client
//                .prepareSearch().setQuery(QueryBuilders.matchQuery("name", "kimchy")).setSize(1);
//
//        MultiSearchResponse sr = client.prepareMultiSearch()
//                .add(srb1)
//                .add(srb2)
//                .get();
//        // You will get all individual responses from MultiSearchResponse#getResponses()
//        long nbHits = 0;
//        for (MultiSearchResponse.Item item : sr.getResponses()) {
//            SearchResponse response = item.getResponse();
//            nbHits += response.getHits().getTotalHits();
//        }
//        closeClient();
//        return sr;
//    }
//
//
//    public static void createIndex(String indexName) throws Exception {
//        setUp();
//        client.admin().indices().prepareCreate(indexName).get();
//        closeClient();
//    }
//
//    /**
//     * @throws
//     * @Title: insertData
//     * @Description: TODO(插入一条)
//     * @param: @param indexName
//     * @param: @param json
//     * @param: @return
//     * @param: @throws Exception
//     * @return: IndexResponse
//     */
//    public static IndexResponse insertData(String indexName, String type, String id, JSONObject json) throws Exception {
//
//        setUp();
//        IndexResponse response = client.prepareIndex(indexName, type, id)
//                .setSource(json)
//                .get();
//        //查询添加的索引
//        // Index name
//        String _index = response.getIndex();
//        // Type name
//        String _type = response.getType();
//        // Document ID (generated or not)
//        String _id = response.getId();
//        // Version (if it's the first time you index this document, you will get: 1)
//        long _version = response.getVersion();
//        // status has stored current instance statement.
//        RestStatus status = response.status();
//        closeClient();
//        return response;
//    }
//
//    /**
//     * @throws
//     * @Title: batchInsertData
//     * @Description: TODO(批量插入)
//     * @param: @param indexName
//     * @param: @param type
//     * @param: @param jsonArr
//     * @param: @return
//     * @param: @throws Exception
//     * @return: BulkResponse
//     */
//    public static BulkResponse batchInsertData(String indexName, String type, JSONArray jsonArr) throws Exception {
//        setUp();
//        BulkRequestBuilder bulkRequest = client.prepareBulk();
//        for (int i = 0; i < jsonArr.size(); i++) {
//            JSONObject jsonObj = jsonArr.getJSONObject(i);
//            //插入单条
//            bulkRequest.add(client.prepareIndex(indexName, type)
//                    .setSource(jsonObj)
//            );
//        }
//
//        BulkResponse bulkResponse = bulkRequest.get();
//        if (bulkResponse.hasFailures()) {
//            //
//            //处理失败
//            return null;
//        }
//        closeClient();
//        return bulkResponse;
//    }
//
//    /**
//     * @throws
//     * @Title: updateData
//     * @Description: TODO(更新单条数据)
//     * @param: @param indexName
//     * @param: @param type
//     * @param: @param id
//     * @param: @param json
//     * @param: @return
//     * @param: @throws Exception
//     * @return: UpdateResponse
//     */
//    public static UpdateResponse updateData(String indexName, String type, String id, JSONObject json) throws Exception {
//        setUp();
//        UpdateResponse updateResponse = client.prepareUpdate(indexName, type, id)
//                .setDoc(json.toString(), XContentType.JSON).get();
//        closeClient();
//        return updateResponse;
//    }
//
//    /**
//     * @throws
//     * @Title: deleteIndex
//     * @Description: TODO(删除索引)
//     * @param: @param indexName
//     * @param: @return
//     * @param: @throws Exception
//     * @return: boolean
//     */
//    public static boolean deleteIndex(String indexName) throws Exception {
//        setUp();
//        IndicesExistsRequest inExistsRequest = new IndicesExistsRequest(indexName);
//
//        IndicesExistsResponse inExistsResponse = client.admin().indices()
//                .exists(inExistsRequest).actionGet();
//        if (inExistsResponse.isExists()) {
//            DeleteIndexResponse dResponse = client.admin().indices().prepareDelete(indexName)
//                    .execute().actionGet();
//            if (dResponse.isAcknowledged()) {
//                closeClient();
//                return true;
//            }
//            closeClient();
//            return false;
//        }
//
//        closeClient();
//        return false;
//    }
//
//    /**
//     * @throws
//     * @Title: deleteData
//     * @Description: TODO(这里用一句话描述这个方法的作用)
//     * @param: @return
//     * @param: @throws Exception
//     * @return: DeleteResponse
//     */
//    public static DeleteResponse deleteData(String indexName, String type, String id) throws Exception {
//        setUp();
//        DeleteResponse response = client.prepareDelete(indexName, type, id).get();
//        closeClient();
//        return response;
//    }
//
//    /**
//     * @throws Exception
//     * @throws
//     * @Title: DeleteByQueryAction
//     * @Description: TODO(通过查询条件删除数据)
//     * @param: @param indexName
//     * @param: @param type
//     * @return: void
//     */
//    public static void DeleteByQueryAction(String indexName, String type) throws Exception {
//
//    }

//}
