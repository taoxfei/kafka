package com.tao.kafka.producer;
 
import com.alibaba.fastjson.JSONObject;
import com.tao.kafka.domain.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
public class KafkaProducer {
 
    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @RequestMapping("/send")
    public void send(Object obj) {
        User user = new User();
        user.setId(1);
        user.setName("taoxuefei");
        String userInfo = JSONObject.toJSONString(user);
        //1.异步发送 默认发送方式
        //kafkaTemplate.send("first", userInfo);
        //2.同步发送其实就是发送时强制监听结果
        CompletableFuture<SendResult<String, Object>> sendResult = kafkaTemplate.send("first", userInfo);
        //开始监听,设置一个时间，超过后放弃此处监听
        SendResult<String, Object> result = null;
        try {
            result = sendResult.get(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        System.out.println("生产者发送成功的数据："+result.getProducerRecord().value());
    }
}