package study.alcohol.rabbitmq.basic.lambda;

/**
 * Created by CoffeeAndJava on 2017. 5. 10..
 */
@FunctionalInterface
public interface CatchConsumer<T> {
    void accept(T t) throws Exception;
}
