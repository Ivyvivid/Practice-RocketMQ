package com.example.demo.mq.producer;

import com.example.demo.conf.RocketMQConfig;
import lombok.Getter;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class Producer {

    @Getter
    private DefaultMQProducer producer;

    @Autowired
    private RocketMQConfig properties;

    @PostConstruct
    public void producer() {
        producer = new DefaultMQProducer(properties.getProducerGroupName());
        producer.setNamesrvAddr(properties.getNamesrvAddr());
        producer.setInstanceName(properties.getProducerInstanceName());
        producer.setVipChannelEnabled(false);
        producer.setRetryTimesWhenSendAsyncFailed(10);
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

}
