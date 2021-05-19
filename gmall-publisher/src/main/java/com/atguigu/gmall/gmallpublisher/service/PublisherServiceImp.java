package com.atguigu.gmall.gmallpublisher.service;


import com.atguigu.gmall.gmallpublisher.util.MyEsUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import io.searchbox.core.search.aggregation.TermsAggregation;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.GetMapping;

import java.io.IOException;
import java.util.HashMap;

import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/8/19 9:10 下午
 * <p>
 * 服务接口的实现类
 */
@Service  // 在 Controller 中可以使用注入的方式创建该实现类的对
public class PublisherServiceImp implements PublisherService {



    @Override

    public Long getDau(String date) {

        JestClient client = MyEsUtil.getClient();
        Search search = new Search
                .Builder(DSL.getDauDSL())
                .addIndex("gmall_dau_info_" + date)
                .addType("_doc").build();
        try {
            SearchResult searchResult = client.execute(search);
            // 查询总的日活
            Long result = searchResult.getTotal();
            client.shutdownClient();
            return result == null ? 0L : result;
        } catch (IOException e) {
            e.printStackTrace();
        }


        return 0L;
    }



    @Override
    public Map<String, Long> getHourDau(String date) {
        // 1. 最终返回值  hour->count
        HashMap<String, Long> result = new HashMap<>();

        // 2. 查询
        JestClient client = MyEsUtil.getClient();
        Search search = new Search
                .Builder(DSL.getHourDauDSL())
                .addIndex("gmall_dau_info_" + date)
                .addType("_doc").build();

        try {
            // 3. 执行查询
            SearchResult searchResult = client.execute(search);
            // 4. 获取聚合结果
            TermsAggregation agg = searchResult.getAggregations().getTermsAggregation("group_by_hour");
            if (agg != null) {
                List<TermsAggregation.Entry> buckets = agg.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    String hour = bucket.getKey();
                    Long count = bucket.getCount();
                    // 5. 把聚合值存储最终返回的 Map中
                    result.put(hour, count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}