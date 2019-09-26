package com.zqh.elasticsearch.factory;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

/**
 * @discription:
 * @date: 2019/09/26 11:22
 */
public class ClientFactory {

    private static RestHighLevelClient restHighLevelClient = null;

    public static RestHighLevelClient getRestHighLevelClient() {
        if (null == restHighLevelClient) {
            synchronized (ClientFactory.class) {
                if (null == restHighLevelClient) {
                    RestClientBuilder builder = RestClient.builder(new HttpHost("", 9200, "http"));
                    restHighLevelClient = new RestHighLevelClient(builder);
                }
            }
        }
        return restHighLevelClient;
    }


    public static void closeClient() {
        if (null != restHighLevelClient) {
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
