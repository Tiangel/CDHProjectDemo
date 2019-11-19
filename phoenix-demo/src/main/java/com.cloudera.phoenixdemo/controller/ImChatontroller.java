package com.cloudera.phoenixdemo.controller;

import com.hdjt.bigdata.Constant.IMChatConstant;
import com.hdjt.bigdata.utils.ResponseDto;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
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
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RestController
@RequestMapping("/api/imchat")
public class ImChatontroller {

    @Autowired
    private RestHighLevelClient highLevelClient;
    private static Logger logger = LoggerFactory.getLogger(ImChatontroller.class);


    /**
     * 根据id获取ES对象
     */
    @RequestMapping(value = "/{id}", method = RequestMethod.GET)
    public ResponseDto getIMChatById(@PathVariable("id") String id) {
        GetResponse getResponse = null;
        String responseBody = null;
        GetRequest getRequest = new GetRequest(IMChatConstant.COMMUNITY_IM_CHAT_INDEX, id);
        try {
            getResponse = highLevelClient.get(getRequest, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                responseBody = getResponse.getSourceAsString();
            } else {
                responseBody = "can not found the community_im_chat by your id";
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            return new ResponseDto(e);
        }
        return new ResponseDto(responseBody);
    }


    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public ResponseDto query(@RequestBody Map<String, Object> map) {

        List<Map<String, Object>> list = new ArrayList<>();
        try {
            if (map != null && !map.isEmpty()) {
                int fromNum = (int) map.get("fromNum");
                int querySize = (int) map.get("querySize");
                Map<String, Object> queryCondition = (Map<String, Object>) map.get("queryCondition");
                Map<String, Boolean> sortFieldsToAsc = (Map<String, Boolean>) map.get("sortFieldsToAsc");
                SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
                //条件
                if (queryCondition != null && !queryCondition.isEmpty()) {
                    BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                    queryCondition.forEach((k, v) -> {
                        if (v instanceof Map) {
                            //范围选择map  暂定时间
                            Map<String, Date> mapV = (Map<String, Date>) v;
                            if (mapV != null && !mapV.isEmpty()) {
                                boolQueryBuilder.must(
                                        QueryBuilders.rangeQuery(k)
                                                .gte(mapV.get("start"))
                                                .lt(mapV.get("end"))
                                );
                            }
                        } else {
                            //普通模糊匹配
                            boolQueryBuilder.must(QueryBuilders.matchPhraseQuery(k, v.toString()));
                        }
                    });
                    sourceBuilder.query(boolQueryBuilder);
                }
                //分页
                fromNum = fromNum <= -1 ? IMChatConstant.COMMUNITY_IM_CHAT_PAGE_NUM : fromNum;
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