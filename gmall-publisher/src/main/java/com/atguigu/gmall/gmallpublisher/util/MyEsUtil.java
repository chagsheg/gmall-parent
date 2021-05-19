package com.atguigu.gmall.gmallpublisher.util;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;

public class MyEsUtil {
 private   static    JestClientFactory factory ;
    static  {
       factory = new JestClientFactory();
        HttpClientConfig build = new HttpClientConfig.Builder("http://localhost:9200")
                .multiThreaded(true)
                .maxTotalConnection(20)
                // 1000毫秒  = 1s
                .connTimeout(1000 * 10)
                .readTimeout(1000 * 10)
                .build();
        factory.setHttpClientConfig(build);
    }

    public static  JestClient  getClient(){
        return factory.getObject();
    }
}
