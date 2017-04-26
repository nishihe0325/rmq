package org.apache.rocketmq.example.client;

import java.util.List;
import java.util.Properties;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.utils.PropertiesManager;

public class Consumer {
    private static final String CONFIG = "/rocketmq.properties";

    private static Properties properties;
    private static String groupName;
    private static String namesrvAddr;
    private static ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
    private static String topics;
    private static String subExpression;

    public static void main(String[] args) throws Exception {
        PropertiesManager.configFile(CONFIG);
        PropertiesManager.start();

        groupName = PropertiesManager.get("rocketmq.groupName");
        namesrvAddr = PropertiesManager.get("rocketmq.namesrvAddr");
        consumeFromWhere = consumeFromWhere(PropertiesManager.get("rocketmq.consumeFromWhere"));
        topics = PropertiesManager.get("rocketmq.topics");
        subExpression = PropertiesManager.get("rocketmq.subExpression");

        // 各种不同消费
        pushConsumer();
    }

    public static void pushConsumer() throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumeFromWhere(consumeFromWhere);
        consumer.subscribe(topics, subExpression);
        consumer.registerMessageListener(new BizMessageListenerConcurrently());
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    private static class BizMessageListenerConcurrently implements MessageListenerConcurrently {
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    private static ConsumeFromWhere consumeFromWhere(String consumeFromWhere) {
        ConsumeFromWhere fromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;

        switch (consumeFromWhere) {
            case "CONSUME_FROM_FIRST_OFFSET":
                fromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
                break;
            case "CONSUME_FROM_LAST_OFFSET":
                fromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
                break;
            case "CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST":
                fromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST;
                break;
            case "CONSUME_FROM_MIN_OFFSET":
                fromWhere = ConsumeFromWhere.CONSUME_FROM_MIN_OFFSET;
                break;
            case "CONSUME_FROM_MAX_OFFSET":
                fromWhere = ConsumeFromWhere.CONSUME_FROM_MAX_OFFSET;
                break;
            case "CONSUME_FROM_TIMESTAMP":
                fromWhere = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP;
                break;
        }

        return fromWhere;
    }

}
