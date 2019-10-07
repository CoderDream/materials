package com.qf.gp1922.day19;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class ESTest {
    public Client client;

    // ES提供的Java api的服务端口为9300
    // 可以多添加几个节点，这样在获取一个节点中的数据的时候出现网络问题，ES会自动切换节点
    @Before
    public void getClient() throws Exception {
        // 错误：NoNodeAvailableException[None of the configured nodes are available
        Map<String, String> map = new HashMap<String, String>();
        map.put("cluster.name", "elasticsearch");
        Settings.Builder settings = Settings.builder().put(map);

        client = TransportClient.builder().settings(settings).build()
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node01"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node02"), 9300))
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("node03"), 9300));

    }

    /**
     * 使用json来创建文档、索引、自动创建映射
     */
    @Test
    public void createDoc_1() {
        // json数据
        String source = "{" +
                "\"id\":\"1\"," +
                "\"title\":\"ElasticSearch是一个基于Lucene的搜索服务器\"," +
                "\"content\":\"它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口\"" +
                "}";

        // 开始创建文档
        // .execute().actionGet() == .get()
        IndexResponse indexResponse = client.prepareIndex("blog01", "article", "1").setSource(source).get();

        // 获取响应信息
        System.out.println("索引：" + indexResponse.getIndex());
        System.out.println("类型：" + indexResponse.getType());
        System.out.println("ID：" + indexResponse.getId());
        System.out.println("版本：" + indexResponse.getVersion());
        System.out.println("是否创建成功：" + indexResponse.isCreated());

        client.close();
    }

    /**
     * 使用map来创建文档
     */
    @Test
    public void createDoc_2() {
        // map格式的数据
        Map<String, Object> source = new HashMap<>();
        source.put("id", "2");
        source.put("title", "我们建立一个网站或应用程序");
        source.put("content", "但是想要完成搜索工作的创建是非常困难的");

        // 创建文档
        IndexResponse indexResponse = client.prepareIndex("blog01", "article", "2").setSource(source).get();

        // 获取响应信息
        System.out.println("索引：" + indexResponse.getIndex());
        System.out.println("类型：" + indexResponse.getType());
        System.out.println("ID：" + indexResponse.getId());
        System.out.println("版本：" + indexResponse.getVersion());
        System.out.println("是否创建成功：" + indexResponse.isCreated());

        client.close();
    }

    /**
     * 使用es的帮助类来创建文档
     */
    @Test
    public void createDoc_3() throws Exception {
        XContentBuilder sourse = XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "3")
                .field("title", "Elasticsearch是用Java开发的")
                .field("content", "并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎")
                .endObject();

        IndexResponse indexResponse = client.prepareIndex("blog01", "article", "3").setSource(sourse).get();

        // 获取响应信息
        System.out.println("索引：" + indexResponse.getIndex());
        System.out.println("类型：" + indexResponse.getType());
        System.out.println("ID：" + indexResponse.getId());
        System.out.println("版本：" + indexResponse.getVersion());
        System.out.println("是否创建成功：" + indexResponse.isCreated());

        client.close();
    }

    /**
     * 搜索文档数据，搜索单个索引
     */
    @Test
    public void testGetData_1() {
        GetResponse getResponse = client.prepareGet("blog01", "article", "2").get();
        System.out.println(getResponse.getSourceAsString());

        client.close();
    }

    /**
     * 搜索文档数据，搜索多个索引
     */
    @Test
    public void testGetData_2() {
        MultiGetResponse itemResponses = client.prepareMultiGet()
                .add("blog", "article", "1")
                .add("blog", "article", "2", "3")
                .get();

        // 遍历获取的数据
        for (MultiGetItemResponse item : itemResponses) {
            GetResponse response = item.getResponse();
            System.out.println(response.getSourceAsString());
        }

        client.close();
    }

    /**
     * 更新文档数据
     */
    @Test
    public void testUpdate_1() throws Exception {
        UpdateRequest request = new UpdateRequest();
        request.index("blog"); // 指定更新的index
        request.type("article"); // 指定更新的type
        request.id("1"); // 指定更新的id

        request.doc(XContentFactory.jsonBuilder()
                .startObject()
                .field("id", "1")
                .field("title", "更新：ElasticSearch是一个基于Lucene的搜索服务器")
                .field("content", "更新：它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口")
                .endObject());

        // 开始更新
        UpdateResponse updateResponse = client.update(request).get();

        // 获取响应信息
        System.out.println("索引：" + updateResponse.getIndex());
        System.out.println("类型：" + updateResponse.getType());
        System.out.println("ID：" + updateResponse.getId());
        System.out.println("版本：" + updateResponse.getVersion());
        System.out.println("是否创建成功：" + updateResponse.isCreated());

        client.close();
    }

    /**
     * 更新文档数据
     */
    @Test
    public void testUpdate_2() throws Exception {
        // 开始更新
        UpdateResponse updateResponse = client.update(new UpdateRequest("blog", "article", "2")
                .doc(XContentFactory.jsonBuilder().startObject()
                        .field("id", "2")
                        .field("title", "更新：我们建立一个网站或应用程序")
                        .field("content", "更新：但是想要完成搜索工作的创建是非常困难的")
                        .endObject()
                )).get();


        // 获取响应信息
        System.out.println("索引：" + updateResponse.getIndex());
        System.out.println("类型：" + updateResponse.getType());
        System.out.println("ID：" + updateResponse.getId());
        System.out.println("版本：" + updateResponse.getVersion());
        System.out.println("是否创建成功：" + updateResponse.isCreated());

        client.close();
    }

    /**
     * 更新文档数据，设置查询的条件，如果查询不到数据，就添加数据
     */
    @Test
    public void testUpdate_3() throws Exception {
        IndexRequest indexRequest = new IndexRequest("blog", "article", "4")
                .source(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("id", "4")
                        .field("title", "一个基于Lucene的搜索服务器")
                        .field("content", "它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口")
                        .endObject());

        // 设置更新的数据，如果查不到，则更新
        UpdateRequest updateRequest = new UpdateRequest("blog01", "article", "4")
                .doc(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("title", "我们建立一个网站或应用程序，并要添加搜索功能")
                        .endObject())
                .upsert(indexRequest);

        UpdateResponse updateResponse = client.update(updateRequest).get();

        // 获取响应信息
        System.out.println("索引：" + updateResponse.getIndex());
        System.out.println("类型：" + updateResponse.getType());
        System.out.println("ID：" + updateResponse.getId());
        System.out.println("版本：" + updateResponse.getVersion());
        System.out.println("是否创建成功：" + updateResponse.isCreated());

        client.close();
    }

    /**
     * 删除数据
     */
    @Test
    public void testDel() {
        DeleteResponse deleteResponse = client.prepareDelete("blog", "article", "4").get();
        System.out.println("是否返回结果：" + deleteResponse.isFound());
        client.close();
    }

    /**
     * 查询文档数据
     */
    @Test
    public void testSearch() {
        // QueryString查询
        SearchResponse searchResponse = client.prepareSearch("blog01")
                .setTypes("article")
                .setQuery(QueryBuilders.queryStringQuery("引擎")) // 发现在查询是只是依据第一个字来查询
                .get();

        // 获取结果集对象，获取命中次数
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询的结果数据有" + hits.getTotalHits() + "条");
        // 遍历每条数据
        Iterator<SearchHit> it = hits.iterator();
        while (it.hasNext()) {
            SearchHit searchHit = it.next();
            // 打印整条信息
            System.out.println(searchHit.getSourceAsString());

            // 获取字段信息
            System.out.println("id:" + searchHit.getSource().get("id"));
            System.out.println("title:" + searchHit.getSource().get("title"));
            System.out.println("content" + searchHit.getSource().get("content"));
        }


        client.close();
    }

    /**
     * 创建索引
     */
    @Test
    public void testCreateIndex() {
        // 创建index
        client.admin().indices().prepareCreate("blog01").get();

        // 删除index
//        client.admin().indices().prepareDelete("blog01").get();
    }

    /**
     * 创建一个带有映射的索引
     */
    @Test
    public void testCreateIndexAndMapping() throws Exception {
        final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("article")
                .startObject("properties")
                .startObject("id")
                .field("type", "integer").field("store", "yes")
                .endObject()
                .startObject("title")
                .field("type", "string").field("store", "yes").field("analyzer", "ik")
                .endObject()
                .startObject("content")
                .field("type", "string").field("store", "yes").field("analyzer", "ik")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        PutMappingRequest request = Requests.putMappingRequest("blog01")
                .type("article")
                .source(mappingBuilder);

        client.admin().indices().putMapping(request).get();

        client.close();
    }


    /**
     * 各种查询
     */
    @Test
    public void testSearchMapping() {
        // queryString查询
//        SearchResponse searchResponse = client.prepareSearch("blog01")
//                .setTypes("article")
//                .setQuery(QueryBuilders.queryStringQuery("引擎"))
//                .get();

        // 词条查询
        // 它仅匹配在给定的字段中去查询，而且是查询含有该词条的内容
//        SearchResponse searchResponse = client.prepareSearch("blog01")
//                .setTypes("article")
//                .setQuery(QueryBuilders.termQuery("content", "引擎"))
//                .get();

        // 通配符查询
//        SearchResponse searchResponse = client.prepareSearch("blog01")
//                .setTypes("article")
//                .setQuery(QueryBuilders.wildcardQuery("content", "接?"))
//                .get();

        // 模糊查询
//        SearchResponse searchResponse = client.prepareSearch("blog01")
//                .setTypes("article")
//                .setQuery(QueryBuilders.fuzzyQuery("title", "Lucena"))
//                .get();

        // 解析字符串查询,指定字段
//        SearchResponse searchResponse = client.prepareSearch("blog01")
//                .setTypes("article")
//                .setQuery(QueryBuilders.queryStringQuery("接口").field("content").field("title"))
//                .get();

        // 字段匹配查询
//        SearchResponse searchResponse = client.prepareSearch("blog01").setTypes("article")
//                .setQuery(QueryBuilders.matchPhrasePrefixQuery("title", "服 ").slop(1))
//                .get();

        // 范围查询
        SearchResponse searchResponse = client.prepareSearch("blog01").setTypes("article")
                .setQuery(QueryBuilders.rangeQuery("id")
                        //.from("它提供").to("引擎").includeLower(true).includeUpper(true))
                        .gt(1).lt(4))
                .get();


        this.getHits(searchResponse);

        client.close();
    }


    /**
     * 查看响应信息方法
     */
    public void getHits(SearchResponse searchResponse) {
        // 获取结果集对象，获取命中次数
        SearchHits hits = searchResponse.getHits();
        System.out.println("查询的结果数据有" + hits.getTotalHits() + "条");
        // 遍历每条数据
        Iterator<SearchHit> it = hits.iterator();
        while (it.hasNext()) {
            SearchHit searchHit = it.next();
            // 打印整条信息
            System.out.println(searchHit.getSourceAsString());

            // 获取字段信息
            System.out.println("id:" + searchHit.getSource().get("id"));
            System.out.println("title:" + searchHit.getSource().get("title"));
            System.out.println("content" + searchHit.getSource().get("content"));
        }
    }

}
