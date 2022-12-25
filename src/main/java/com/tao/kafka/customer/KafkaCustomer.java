package com.tao.kafka.customer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class KafkaCustomer {
    /**
     * 定义此消费者接收topics = "first"的消息，与controller中的topic对应上即可
     * @param record 变量代表消息本身，可以通过ConsumerRecord<?,?>类型的record变量来打印接收的消息的各种信息
     */
    @KafkaListener(topics = "first",groupId = "grouptest")
    public void userAListener(ConsumerRecord<?,?> record) {
        //System.out.println(record.topic());
        //System.out.println(record.offset());
        //System.out.println(record.value());
        Optional<?> optional = Optional.ofNullable(record.value());
        if (optional.isPresent()) {
            Object msg = optional.get();
            System.out.println("消费者A收到的消息："+msg);;
        }
    }

    @KafkaListener(topics = "first",groupId = "grouptest")
    public void userBListener(ConsumerRecord<?,?> record) {
        //System.out.println(record.topic());
        //System.out.println(record.offset());
        //System.out.println(record.value());
        Optional<?> optional = Optional.ofNullable(record.value());
        if (optional.isPresent()) {
            Object msg = optional.get();
            System.out.println("消费者B收到的消息："+msg);;
        }
    }
}
