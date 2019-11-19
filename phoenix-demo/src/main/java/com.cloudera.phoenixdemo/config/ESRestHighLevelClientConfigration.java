package com.cloudera.phoenixdemo.config;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Charles
 * @package com.hdjt.bigdata.config
 * @classname ESConfigration
 * @description TODO
 * @date 2019-11-14 9:37
 */
@Configuration
public class ESRestHighLevelClientConfigration {

    @Value("${elasticsearch.host}")
    private String esHost;

    @Value("${elasticsearch.port}")
    private int esPort;

    @Value("${elasticsearch.clusterName}")
    private String esClusterName;

    private RestHighLevelClient highLevelClient;

    public static final String DEFAULT_SCHEME_NAME = "http";
    public static final int CONNECT_TIMEOUT = 5000;
    public static final int SOCKET_TIMEOUT = 60000;
    public static final int IO_THREAD_COUNT = 1;

    @PostConstruct
    public void initialize() throws Exception {
        List<HttpHost> list = new ArrayList<HttpHost>();
        String[] esHosts = esHost.trim().split(",");
        HttpHost[] httpHostArray = new HttpHost[esHosts.length];
//        for (String host : esHosts) {
//            list.add(new HttpHost(host, esPort, DEFAULT_SCHEME_NAME));
//            httpHostArray
//        }
        for (int i = 0; i < esHosts.length; i++) {
            httpHostArray[i] = new HttpHost(esHosts[i], esPort, DEFAULT_SCHEME_NAME);
        }

        RestClientBuilder clientBuilder = RestClient.builder(httpHostArray);

        // 设置请求头，每个请求都会带上这个请求头
//        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
//        clientBuilder.setDefaultHeaders(defaultHeaders);

        // 设置监听器，每次节点失败都可以监听到，可以作额外处理
        clientBuilder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                super.onFailure(node);
                System.out.println(node.getName() + "==节点失败了");
            }
        });

        /*
        配置节点选择器，客户端以循环方式将每个请求发送到每一个配置的节点上，
        发送请求的节点，用于过滤客户端，将请求发送到这些客户端节点，默认向每个配置节点发送，
        这个配置通常是用户在启用嗅探时向专用主节点发送请求（即只有专用的主节点应该被HTTP请求命中）
        */
        clientBuilder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

        /*
        配置异步请求的线程数量，Apache Http Async Client默认启动一个调度程序线程，以及由连接管理器使用的许多工作线程
        （与本地检测到的处理器数量一样多，取决于Runtime.getRuntime().availableProcessors()返回的数量）。线程数可以修改如下,
        这里是修改为1个线程，即默认情况
        */
        clientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultIOReactorConfig(
                        IOReactorConfig.custom().setIoThreadCount(IO_THREAD_COUNT).build()
                );
            }
        });

        /*
        配置请求超时，将连接超时（默认为1秒）和套接字超时（默认为30秒）增加，
        这里配置完应该相应地调整最大重试超时（默认为30秒），即上面的setMaxRetryTimeoutMillis，一般于最大的那个值一致即60000
        */
        clientBuilder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                // 连接5秒超时，套接字连接60s超时
                return requestConfigBuilder.setConnectTimeout(CONNECT_TIMEOUT).setSocketTimeout(SOCKET_TIMEOUT);
            }
        });
        highLevelClient = new RestHighLevelClient(clientBuilder);
    }



    @Bean
    public RestHighLevelClient highLevelClient() {
        return highLevelClient;
    }

    @PreDestroy
    public void destroy() throws IOException {
        if (highLevelClient != null) {
            highLevelClient.close();
        }
    }
}