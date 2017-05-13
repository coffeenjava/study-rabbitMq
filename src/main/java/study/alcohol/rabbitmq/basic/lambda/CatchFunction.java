package study.alcohol.rabbitmq.basic.lambda;

/**
 * Created by CoffeeAndJava on 2017. 5. 10..
 */
@FunctionalInterface
public interface CatchFunction<T,R> {
    R apply(T t) throws Exception;
}
