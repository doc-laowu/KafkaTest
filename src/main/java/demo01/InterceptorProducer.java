package demo01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class InterceptorProducer {

    private static final String topic = "test";
    private static final Integer threads = 2;

    public static void main(String[] args) throws Exception {
        // 1 设置配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "node01:9092,node02:9092,node03:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2 构建拦截链
        List<String> interceptors = new ArrayList<>();
        interceptors.add("demo01.TimeInterceptor");
        interceptors.add("demo01.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        String topic = "test";
        Producer<String, String> producer = new KafkaProducer<>(props);

//        for(int j=0; j<threads; j++){
//            new Thread(new Runnable() {
//                @Override
//                public void run() {
//                    // 3 发送消息
//                    for (int i = 0; i < 10; i++) {
//                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
//                        producer.send(record);
//                    }
//                    // 4 一定要关闭producer，这样才会调用interceptor的close方法
//                    producer.close();
//                }
//            });
//        }


        // 3 发送消息
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "message" + i);
            producer.send(record);
        }
        // 4 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();
    }
}

