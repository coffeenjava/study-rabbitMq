package study.alcohol.rabbitmq.basic;

import com.rabbitmq.client.AMQP;

import java.util.UUID;

/**
 * Created by CoffeeAndJava on 2017. 5. 8..
 */
public class MessagingMain {
    String JSON_TYPE = "application/vnd.ccm.pmsg.v1+json";

    String exchange1 = "exchange-1";
    String exchange2 = "exchange-2";
    String queue1 = "queue-1";
    String queue2 = "queue-2";
    String queue3 = "queue-3";
    String routingKey1 = "routing-1";
    String routingKey2 = "routing-2";
    String routingKey3 = "routing-3";
    String msg1 = "this is 1st message";
    String msg2 = "this is 2nd message";
    String msg3 = "this is 3rd message";

    public static void main(String[] args) {
        // mq 접속
        MqManager mqManager = new MqManager();
        mqManager.start();

        MessagingMain m = new MessagingMain();
//        m.publishMessage(mqManager);
        m.consumeMessage(mqManager);

        // mq 접속 종료
        mqManager.stop();
    }

    void publishMessage(MqManager mqManager) {
        /**
         * 1. exchange 생성
         */
        String messageId = UUID.randomUUID().toString(); // ?
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                .contentType(JSON_TYPE)
                .contentEncoding("UTF-8")
                .messageId(messageId)
                .deliveryMode(2)
                .build();

//        mqManager.basicPublish(exchange1, routingKey1, props, msg1);
        mqManager.basicPublish(exchange2, routingKey3, props, msg2);
        mqManager.basicPublish(exchange2, routingKey3, props, msg3);
//        mqManager.basicPublish(exchange1, routingKey2, props, msg3);
    }

    void consumeMessage(MqManager mqManager) {
        String msg;
        while ((msg = mqManager.basicGet(queue3)) != null) {
            System.out.println("["+msg+"]");
        }
    }
}