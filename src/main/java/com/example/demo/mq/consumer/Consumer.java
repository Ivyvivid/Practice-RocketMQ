package com.example.demo.mq.consumer;

import com.example.demo.conf.RocketMQConfig;
import com.example.demo.model.MessageEvent;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.stream.Collectors;

@Component
public class Consumer {
    @Autowired
    RocketMQConfig properties;

    @Autowired
    ApplicationEventPublisher publisher;
    private boolean isFirstSub = true;
    private long starttime = System.currentTimeMillis();

    @PostConstruct
    public void Consumer() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(properties.getConsumerGroupName());
        consumer.setNamesrvAddr(properties.getNamesrvAddr());
        consumer.setInstanceName(properties.getConsumerInstanceName());
        consumer.setVipChannelEnabled(false);
        //从消息队列头开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        if (properties.isConsumerBroadcasting()) {
            //设置广播消费模式
            consumer.setMessageModel(MessageModel.BROADCASTING);
        }
        //设置批量消费
        consumer.setConsumeMessageBatchMaxSize(properties.getConsumerBatchMaxSize() == 0 ? 1 : properties.getConsumerBatchMaxSize());
        List<String> subscribeList = properties.getSubscribe();
        for (String subscribe :
                subscribeList) {
            consumer.subscribe(subscribe.split(":")[0], subscribe.split(":")[1]);
        }
        //顺序消费
        if (properties.isEnableOrderConsumer()){
            //注册消息监听器
            consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) ->{
                context.setAutoCommit(true);
                msgs = filterMessage(msgs);
                if (msgs.size() == 0) {
                    return ConsumeOrderlyStatus.SUCCESS;
                }
                publisher.publishEvent(new MessageEvent(msgs,consumer));
                return ConsumeOrderlyStatus.SUCCESS;
            });
            //并发消费
        } else{
            consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) ->{
                msgs = filterMessage(msgs);
                if (msgs.size() == 0) {
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                }
                publisher.publishEvent(new MessageEvent(msgs,consumer));
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            });
        }
        consumer.start();
    }

    /**
     * 消息过滤
     * @param msgs
     * @return
     */
    List<MessageExt> filterMessage(List<MessageExt> msgs) {
        if (isFirstSub && properties.isEnableHistoryConsumer()) {
            msgs = msgs.stream()
                    .filter(item -> starttime - item.getBornTimestamp() < 0)
                    .collect(Collectors.toList());
        }
        if (isFirstSub && msgs.size() > 0) {
            isFirstSub = false;
        }
        return msgs;
    }

}
