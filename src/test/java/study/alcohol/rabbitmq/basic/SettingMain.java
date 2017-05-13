package study.alcohol.rabbitmq.basic;

import com.rabbitmq.client.AMQP.Exchange;

import java.io.IOException;
import java.util.Map;

/**
 * Created by CoffeeAndJava on 2017. 5. 8..
 */
public class SettingMain {
    public static void main(String[] args) {
        SettingMain m = new SettingMain();
        m.initMq();
    }

    void initMq() {
        // mq 접속
        MqManager mqManager = new MqManager();
        mqManager.start();

        /**
         * 1. exchange 생성
         */
        final String exchange1 = "exchange-1";
        final String ex_type = "direct";
        final boolean ex_durable = true;
        final boolean ex_autoDelete = false;
        final Map<String, Object> ex_args = null;

        Exchange.DeclareOk ok = mqManager.callResult(c -> {
            try {
                return c.exchangeDeclare(exchange1,ex_type,ex_durable,ex_autoDelete,ex_args);
            } catch (IOException ioe) {}
            return null;
        });

        final String exchange2 = "exchange-2";
        mqManager.exchangeDeclare(exchange2,ex_type,ex_durable,ex_autoDelete,ex_args);

        // declareOk 용도 ?

        /**
         * 2. queue 생성
         */
        final String queue1 = "queue-1";
        final boolean q_durable = true;
        final boolean q_autoDelete = false;
        final boolean q_exclusive = false;
        final Map<String, Object> q_args = null;
        mqManager.queueDeclare(queue1, q_durable, q_exclusive, q_autoDelete, q_args);

        final String queue2 = "queue-2";
        mqManager.queueDeclare(queue2, q_durable, q_exclusive, q_autoDelete, q_args);

        final String queue3 = "queue-3";
        mqManager.queueDeclare(queue3, q_durable, q_exclusive, q_autoDelete, q_args);

        /**
         * 3. queue 바인딩
         */
        final String routingKey1 = "routing-1";
        mqManager.queueBind(queue1, exchange1, routingKey1);

        final String routingKey2 = "routing-2";
        mqManager.queueBind(queue2, exchange2, routingKey2);

        final String routingKey3 = "routing-3";
        mqManager.queueBind(queue3, exchange2, routingKey3);

        // mq 접속 종료
        mqManager.stop();
    }
}