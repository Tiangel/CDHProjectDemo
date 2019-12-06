package com.cloudera.phoenixdemo.service.impl;

import com.cloudera.phoenixdemo.constant.IMChatConstant;
import com.cloudera.phoenixdemo.service.IQueryIMChatService;
import com.cloudera.phoenixdemo.utils.ResponseDto;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Charles
 * @package com.hdjt.bigdata.service.impl
 * @classname QueryIMChatServiceImpl
 * @description TODO
 * @date 2019-11-19 20:13
 */
@Service
public class QueryIMChatServiceImpl implements IQueryIMChatService {

    @Autowired
    private RestHighLevelClient highLevelClient;

    private static Logger logger = LoggerFactory.getLogger(QueryIMChatServiceImpl.class);

    @Override
    public ResponseDto queryIMChatFromES(Map<String, Object> map) {

        List<Map<String, Object>> list = new ArrayList<>();
        try {
            if (map != null && !map.isEmpty()) {
                int fromNum = (int) map.get(IMChatConstant.COMMUNITY_IM_CHAT_FROMNUM);
                int querySize = (int) map.get(IMChatConstant.COMMUNITY_IM_CHAT_QUERYSIZE);
                Map<String, Object> queryAndCondition = (Map<String, Object>) map.get(IMChatConstant.COMMUNITY_IM_CHAT_QUERY_AND_CONDITION);
                Map<String, Object> queryOrCondition = (Map<String, Object>) map.get(IMChatConstant.COMMUNITY_IM_CHAT_QUERY_OR_CONDITION);
                Map<String, Boolean> sortFieldsToAsc = (Map<String, Boolean>) map.get(IMChatConstant.COMMUNITY_IM_CHAT_ORDERBY_CLAUSE);
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                //AND 条件
                if (queryAndCondition != null && !queryAndCondition.isEmpty()) {
                    queryAndCondition.forEach((k, v) -> {
                        if (v instanceof Map) {
                            //范围选择map  暂定时间
                            Map<String, Date> mapV = (Map<String, Date>) v;
                            if (mapV != null && !mapV.isEmpty()) {
                                boolQueryBuilder.filter(
                                        QueryBuilders.rangeQuery(k)
                                                .gte(mapV.get(IMChatConstant.IM_CHAT_QUERY_TIMESTAMP_BEGINDATE))
                                                .lt(mapV.get(IMChatConstant.IM_CHAT_QUERY_TIMESTAMP_ENDDATE))
                                );
                            }
                        } else if (IMChatConstant.IM_CHAT_QUERY_MSG.equalsIgnoreCase(k)) {
                            //普通模糊匹配
                            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery(k, v.toString()));
                        } else {
                            boolQueryBuilder.filter(QueryBuilders.termQuery(k, v.toString()));
                        }
                    });
                }
                // OR 条件
                if (queryOrCondition != null && !queryOrCondition.isEmpty()) {
                    BoolQueryBuilder queryOrBuilder = QueryBuilders.boolQuery();
                    queryOrCondition.forEach((k, v) -> {
                        queryOrBuilder.should(QueryBuilders.termQuery(k, v.toString()));
                    });
                    boolQueryBuilder.must(queryOrBuilder);
//                    boolQueryBuilder.minimumShouldMatch(1);
                }
                sourceBuilder.query(boolQueryBuilder);

                //分页
                fromNum = fromNum <= -1 ? IMChatConstant.COMMUNITY_IM_CHAT_DEFAULT_FROM_NUM : fromNum;
                querySize = querySize >= 1000 ? IMChatConstant.COMMUNITY_IM_CHAT_MAX_PAGE_SIZE : querySize;
                querySize = querySize <= 0 ? IMChatConstant.COMMUNITY_IM_CHAT_DEFAULT_PAGE_SIZE : querySize;
                sourceBuilder.from(fromNum);
                sourceBuilder.size(querySize);
                //超时
                sourceBuilder.timeout(new TimeValue(IMChatConstant.COMMUNITY_IM_CHAT_TIMEOUT, TimeUnit.SECONDS));
                //排序
                if (sortFieldsToAsc != null && !sortFieldsToAsc.isEmpty()) {
                    sortFieldsToAsc.forEach((k, v) -> {
                        // true 为升序，false 为降序
                        sourceBuilder.sort(new FieldSortBuilder(k).order(v ? SortOrder.ASC : SortOrder.DESC));
                    });
                } else {
                    sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
                }

                SearchRequest request = new SearchRequest();
                //索引
                request.indices(IMChatConstant.COMMUNITY_IM_CHAT_INDEX);
                //各种组合条件
                request.source(sourceBuilder);

                //请求
                logger.info("查询请求参数 ====> " + request.source().toString());
                SearchResponse response = highLevelClient.search(request, RequestOptions.DEFAULT);

                //获取source
//            List<Map<String, Object>> collect = Arrays.stream(response.getHits().getHits()).map(b -> {
//                return b.getSourceAsMap();
//            }).collect(Collectors.toList());
                SearchHits hits = response.getHits();
                SearchHit[] searchHits = hits.getHits();
                for (SearchHit hit : searchHits) {
                    Map sourceAsString = hit.getSourceAsMap();
                    list.add(sourceAsString);
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            e.printStackTrace();
            return new ResponseDto(e);
        }
        return new ResponseDto(list);
    }
}
