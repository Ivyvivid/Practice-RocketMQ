package com.example.demo.controller;

import com.alibaba.fastjson.JSON;
import com.example.demo.common.common;
import com.example.demo.model.User;
import com.example.demo.mq.producer.Producer;
import com.example.demo.service.WebSocketService;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.ModelAndView;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.List;

@RestController
public class Controller {
    @Autowired
    Producer producer;


    @RequestMapping("/push")
    public void pushMsg() {
        for (int i = 0; i < 100; i++) {
            User user = new User();
            user.setLoginName("tom" + i);
            String json = JSON.toJSONString(user);
            Message msg = new Message("user-topic", "white", json.getBytes());

            try {
                SendResult result = producer.getProducer().send(msg, new MessageQueueSelector() {
                    @Override
                    public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                        int index = (Integer) o % list.size();
                        return list.get(index);
                    }
                },i);
                System.out.println("消息id：" + result.getMsgId() + "," + "发送状态：" + result.getSendStatus() );
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (RemotingException e) {
                e.printStackTrace();
            } catch (MQClientException e) {
                e.printStackTrace();
            } catch (MQBrokerException e) {
                e.printStackTrace();
            }
        }
    }
}
