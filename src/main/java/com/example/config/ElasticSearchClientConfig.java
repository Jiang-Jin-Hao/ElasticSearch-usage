package com.example.config;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration  //xml
public class ElasticSearchClientConfig {
    @Bean
    public RestHighLevelClient restHighLevelClient(){
        RestHighLevelClient client=new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost",9200,"http")
//                        可配置多个为集群
                )
        );
        return client;
    }
}
