package ProducerApi;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 同步的方式生产数据
 */
public class ProducerSync {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:2181,node02:2181,node03:2181");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.test.id");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i <= 20; i++){

            ProducerRecord msg = new ProducerRecord<String, String>("test", "it " + i);

            try {
                Future<RecordMetadata> future = producer.send(msg);
                RecordMetadata metadata =  future.get();
                System.out.println(metadata.topic() + ":" + metadata.partition() + ":" + metadata.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        // 关闭客户端
        producer.close();

    }

}
