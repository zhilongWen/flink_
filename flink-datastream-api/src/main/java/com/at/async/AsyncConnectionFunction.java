package com.at.async;

import com.at.pojo.Event;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @create 2022-05-31
 */
public abstract class AsyncConnectionFunction extends RichAsyncFunction<Event, ClickUserInfo> implements ConnectFunction {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void asyncInvoke(Event input, ResultFuture<ClickUserInfo> resultFuture) throws Exception {

        System.out.println("async invoke : " + Thread.currentThread().getName() + "\t" + Thread.currentThread().getId());

        Executors.newFixedThreadPool(1).execute(() -> {

            System.out.println("async invoke TT: " + Thread.currentThread().getName() + "\t" + Thread.currentThread().getId());


            ClickUserInfo clickUserInfo = null;

            String key = getKey(input);

            UserInfo userInfo = ConnectUtil.query("select name,age,sex,address from rule_table where name = '" + key + "'");


            clickUserInfo = join(input, userInfo);


            //将关联后的数据数据继续向下传递
            resultFuture.complete(Arrays.asList(clickUserInfo));

        });

    }


}
