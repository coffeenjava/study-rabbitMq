package study.alcohol.rabbitmq.basic;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import study.alcohol.rabbitmq.basic.lambda.*;

/**
 * Created by CoffeeAndJava on 2017. 5. 8..
 */
public class MqManager {

    private ConnectionFactory factory;
    private ScheduledExecutorService executor;
    private Connection connection;

    public MqManager() {
        factory = new ConnectionFactory();
        factory.setUsername("ccm-dev");
        factory.setPassword("coney123");
        factory.setVirtualHost("ccm-dev-vhost");
        factory.setHost("localhost");
        factory.setPort(5672);

        // 재시작용 스레드 풀
        executor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * MQ Connect
     */
    public void start() {
        try {
            connection = factory.newConnection();
            connection.addShutdownListener((cause) -> {
                // 의도된 종료가 아닐 경우 restart
                if (cause.isInitiatedByApplication() == false) {
                    restart();
                }
            });
        } catch (Exception e) {
            restart();
        }
    }

    /**
     * 재시작
     */
    private void restart() {
        connection = null;
        executor.schedule(()-> start(), 15, TimeUnit.SECONDS);
    }

    /**
     * 종료
     */
    public void stop() {
        executor.shutdown();

        Optional.ofNullable(connection).ifPresent(c -> {
            try {
                c.close();
            } catch (Exception e) {
            } finally {
                c = null;
            }
        });
    }

    /**
     * Channel 생성
     * @return
     */
    private Optional<Channel> createChannel() {
        return Optional.ofNullable(connection).map(fnCreateChannel);
    }

    // channel 꺼내기용 함수
    private Function<Connection, Channel> fnCreateChannel = c -> {
        try {
            return c.createChannel();
        } catch (IOException e) {
            return null;
        }
    };

    /**
     * Channel 종료
     * @param channel
     */
    private void closeChannel(Channel channel) {
        try {
            channel.close();
        } catch (IOException e) {
        } finally {
            channel = null;
        }
    }

    /**
     * Exchange 생성
     *
     * @param exchange
     * @param type
     * @param durable
     * @param autoDelete
     * @param args
     * @return
     */
    public AMQP.Exchange.DeclareOk exchangeDeclare(String exchange, String type,
            boolean durable, boolean autoDelete, Map<String,Object> args) {
        return callResult(c -> {
            try {
                return c.exchangeDeclare(exchange, type, durable, autoDelete, args);
            } catch (IOException ioe) {}
            return null;
        });
    }

    /**
     * Queue 생성
     *
     * @param queue
     * @param durable
     * @param exclusive
     * @param autoDelete
     * @param args
     * @return
     */
    public AMQP.Queue.DeclareOk queueDeclare(String queue, boolean durable,
            boolean exclusive, boolean autoDelete, Map<String,Object> args) {
        return callResultWithCatch(c ->
                c.queueDeclare(queue, durable, exclusive, autoDelete, args));
    }

    /**
     * Queue 바인딩
     *
     * @param queue
     * @param exchange
     * @param routingKey
     * @return
     */
    public AMQP.Queue.BindOk queueBind(String queue, String exchange, String routingKey) {
        return callResultWithCatch(c ->
                c.queueBind(queue, exchange, routingKey));
    }

    /**
     * 메시지 보내기
     *
     * @param exchange
     * @param routingKey
     * @param props
     * @param msg
     */
    public void basicPublish(String exchange, String routingKey, AMQP.BasicProperties props,
                             String msg) {
        callWithCatch(c -> c.basicPublish(exchange, routingKey, props, msg.getBytes()));
    }

    /**
     * 메시지 받기
     *
     * @param queue
     * @return
     */
    public String basicGet(String queue) {
        // ack 설정에 따른 변화
        boolean autoAck = true;
        Optional<GetResponse> maybeRes = Optional.ofNullable(callResultWithCatch(c ->
                c.basicGet(queue, autoAck)));
        return maybeRes.map(r -> new String(r.getBody())).orElse(null);
    }

    public String nackGet(String queue) {
        // ack 설정에 따른 변화
        boolean autoAck = false;
        Optional<GetResponse> maybeRes = Optional.ofNullable(callResultWithCatch(c ->
        {
            GetResponse res = c.basicGet(queue, autoAck);
            /**
             * basicNack
             *  requeue
             *      true => 받은 메시지를 다시 queue 의 원래 위치에 ready 상태로 둔다.
             *      false => nack 상태값의 메시지로 queue 에서 제거. => dlx(dead letter exchange) queue 로 보낸다.
             */
            c.basicNack(res.getEnvelope().getDeliveryTag(), false, false);

            return res;
        }));
        return maybeRes.map(r -> new String(r.getBody())).orElse(null);
    }

    public String multipleAckTest(String queue) {
        boolean autoAck = false; // 메시지를 queue 에 그대로 둔다.
        Optional<GetResponse> maybeRes = Optional.ofNullable(callResultWithCatch(c -> {
            GetResponse res = c.basicGet(queue, autoAck);
            c.basicGet(queue, autoAck);
            /**
             * basicAck
             * bulk 처리 용도
             *      mutiple
             *          true => 한번에 여러개 메시지 받아 처리 한 후 ack.
             *          false => 대상 Response 만 ack.
             */
            c.basicAck(res.getEnvelope().getDeliveryTag(), false);
            return res;
        }));
        return maybeRes.map(r -> new String(r.getBody())).orElse(null);
    }

    /***** helper 용 메서드 **************************************************/

    /**
     * Channel 을 직접 사용
     * return 값 필요할 경우 사용
     *
     * @param fnc
     * @param <R>
     * @return
     */
    public <R> R callResult(Function<Channel, R> fnc) {
        Optional<Channel> channel = createChannel();
        return channel.map(c -> {
            R r = fnc.apply(c);
            closeChannel(c);
            return r;
        }).orElse(null);
    }

    /**
     * Channel 을 직접 사용
     * return 값 없음
     *
     * @param fnc
     */
    public void call(Consumer<Channel> fnc) {
        Optional<Channel> channel = createChannel();
        channel.ifPresent(c -> {
            fnc.accept(c);
            closeChannel(c);
        });
    }

    /**
     * Channel 을 직접 사용
     * return 값 필요할 경우 사용
     * try catch 처리 포함
     *
     * @param fnc   try-catch 처리가능한 CatchFunction
     * @param <R>
     * @return
     */
    private <R> R callResultWithCatch(CatchFunction<Channel, R> fnc) {
        Optional<Channel> channel = createChannel();
        return channel.map(c -> {
            try {
                return fnc.apply(c);
            } catch (Exception e) {}
            return null;
        }).orElse(null);
    }

    /**
     * Channel 을 직접 사용
     * return 값 없음
     * try catch 처리 포함
     *
     * @param fnc   try-catch 처리가능한 CatchConsumer
     */
    private void callWithCatch(CatchConsumer<Channel> fnc) {
        Optional<Channel> channel = createChannel();
        channel.ifPresent(c -> {
            try {
                fnc.accept(c);
            } catch (Exception e) {}
            finally {
                closeChannel(c);
            }
        });
    }
}
