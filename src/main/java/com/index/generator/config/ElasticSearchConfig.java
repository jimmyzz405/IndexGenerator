package com.index.generator.config;


import org.apache.commons.io.IOUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class ElasticSearchConfig {

    private static final String ES_NODES_SPLIT = ",";
    private RestClient lowLevelRestClient;
    private RestHighLevelClient client;
    private Sniffer sniffer;

//    @Value("127.0.0.1:9200")
    private String dataNodeUrl = "127.0.0.1:9200";

//    @Value("${elasticsearch.connect.scheme}")
    private String connectScheme;

    @PostConstruct
    public void init(String serverUrl) {
        RestClientBuilder builder = RestClient.builder(loadNode(serverUrl));
        builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS)
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(
                            HttpAsyncClientBuilder httpClientBuilder) {
                        httpClientBuilder.setMaxConnPerRoute(30).setMaxConnTotal(50);
                        return httpClientBuilder;
                    }
                }).setRequestConfigCallback(new RequestConfigCallback() {
            @Override
            public Builder customizeRequestConfig(
                    Builder requestConfigBuilder) {
                return requestConfigBuilder
                        .setConnectTimeout(5000)
                        .setSocketTimeout(600000);
            }
        });
//    lowLevelRestClient = builder.build();

//    NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(
//        lowLevelRestClient,
//        TimeUnit.SECONDS.toMillis(5),
//        ElasticsearchNodesSniffer.Scheme.HTTP);
//    sniffer = Sniffer.builder(lowLevelRestClient).setNodesSniffer(nodesSniffer).build();
        client = new RestHighLevelClient(builder);

    }

    @PreDestroy
    public void destroy() {
        IOUtils.closeQuietly(sniffer);
        IOUtils.closeQuietly(lowLevelRestClient);
        IOUtils.closeQuietly(client);

    }

    @Bean
    public RestHighLevelClient getClient() {
        return client;
    }

    private HttpHost[] loadNode(String serverUrl) {
        List<HttpHost> load = new ArrayList<>();
        for (String s : serverUrl.split(ES_NODES_SPLIT)) {
            String url = s.split(":")[0];
            int port = Integer.parseInt(s.split(":")[1]);
            load.add(new HttpHost(url, port));
        }
        return load.toArray(new HttpHost[0]);
    }
}
