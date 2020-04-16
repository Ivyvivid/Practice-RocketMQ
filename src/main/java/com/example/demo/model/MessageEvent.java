package com.example.demo.model;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.context.ApplicationEvent;

import java.util.List;

/**
 * 事件监听
 */
@Setter
@Getter
public class MessageEvent extends ApplicationEvent {
    List<MessageExt> msgs;
    DefaultMQPushConsumer consumer;
    public MessageEvent(List<MessageExt> msgs, DefaultMQPushConsumer consumer) {
        super(msgs);
        this.consumer = consumer;
        this.setMsgs(msgs);
    }
}
