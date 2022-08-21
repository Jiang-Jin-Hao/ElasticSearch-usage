package com.example;

import com.alibaba.fastjson.JSON;
import com.example.pojo.User;
import com.example.utils.ESconst;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * es7.6.1 API测试
 */
@SpringBootTest
class EsApiApplicationTests {

    @Autowired
    @Qualifier("restHighLevelClient")   //  通过名字注入
    private RestHighLevelClient client;

    //    测试创建索引  索引=数据库
    @Test
    void testCreateIndex() throws IOException {
//        创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("jiang_index");
//        执行创建请求，请求后获得相应
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    //    测试获取索引
    @Test
    void testExistIndex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("jiang_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists == false ? "不存在" : "存在");
    }

    //    测试删除索引
    @Test
    void deleteIndex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("jiang_index");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println("是否删除成功：" + delete.isAcknowledged());
    }

    //    测试添加文档
    @Test
    void testAddDocument() throws IOException {
//        创建对象
        User user = new User("tracy", 18);
//        创建请求
        IndexRequest request = new IndexRequest("jiang_index");
//        规则:    put /jiang_index/_doc/1
        request.id("1");
        request.timeout(TimeValue.timeValueSeconds(1)); //  超时时间
//        将数据放入请求
        request.source(JSON.toJSONString(user), XContentType.JSON);
//        客户端发送请求
        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

        System.out.println(indexResponse.toString());
        System.out.println(indexResponse.status()); //  CREATED=创建成功
    }

    //    判断文档是否存在 get /index/_doc/1
    @Test
    void testIsExists() throws IOException {
        GetRequest getRequest = new GetRequest("jiang_index", "1");
//        不获取返回的上下文，效率更高
        getRequest.fetchSourceContext(new FetchSourceContext(false));
//        排序依据
        getRequest.storedFields("_none_");

        boolean exists = client.exists(getRequest, RequestOptions.DEFAULT);

        System.out.println("是否存在:" + exists);
    }

    //    获取文档信息
    @Test
    void testGetDocument() throws IOException {
        GetRequest getRequest = new GetRequest("jiang_index", "1");
        GetResponse getResponse = client.get(getRequest, RequestOptions.DEFAULT);
//        转换为map文档格式
        String sourceAsString = getResponse.getSourceAsString();
        System.out.println(sourceAsString);
        System.out.println(getResponse);
    }

    //    更新文档记录
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("jiang_index", "1");
        request.timeout("1s");  //  超时时间
        User user = new User("anna", 20);
        request.doc(JSON.toJSONString(user), XContentType.JSON);
        UpdateResponse updateResponse = client.update(request, RequestOptions.DEFAULT);

        System.out.println(updateResponse.toString());
        System.out.println(updateResponse.status());    //  OK=更新成功
    }

    //    删除文档记录
    @Test
    void testDeleteDocument() throws IOException {
        DeleteRequest request = new DeleteRequest("jiang_index", "1");
        DeleteResponse deleteResponse = client.delete(request, RequestOptions.DEFAULT);

        System.out.println(deleteResponse.toString());
        System.out.println("是否删除成功:" + deleteResponse.status());  //  OK=删除成功
    }

    //    批量插入
    @Test
    void testBulkDocument() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        ArrayList<User> userList = new ArrayList<>();
        userList.add(new User("lvy", 18));
        userList.add(new User("yuki", 19));
        userList.add(new User("susan", 20));
//        批处理请求
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(
                    new IndexRequest("jiang_index")
                            .id(i + 1 + "")
                            .source(JSON.toJSONString(userList.get(i)), XContentType.JSON)
            );
        }
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        System.out.println("是否失败:" + bulkResponse.hasFailures());
    }

    /**
     * 测试查询
     * SearchRequest    搜索请求 <--库
     * SearchSourceBuilder  条件构造 <--查询条件
     * HighLightBuilder 构建高亮
     * TermQueryBuilder 精确查询
     * MatchAllQueryBuilder 匹配全部
     * xxx Builder
     * @throws IOException
     */
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest(ESconst.ES_INDEX);
//        构建搜索的条件:高亮、排序、分页
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
//        查询条件  使用QueryBuilders工具类快速匹配
//        termQuery:精确匹配
//        mathAllQuery:查询匹配所有
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "");
        sourceBuilder.query(termQueryBuilder);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); //  超时时间
        sourceBuilder.from();   //  分页
        sourceBuilder.size();
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest,RequestOptions.DEFAULT);
        SearchHits hits = searchResponse.getHits();
        System.out.println(JSON.toJSONString(hits));
        for (SearchHit hit : hits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
        }
    }
}
