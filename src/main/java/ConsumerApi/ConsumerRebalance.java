package ConsumerApi;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * 分区再平衡
 */
public class ConsumerRebalance {

    private static final String topic = "test";
    private static final Integer threads = 2;

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:2181,node02:2181,node03:2181");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "vvvvv");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "vvvvv_local");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 使用自定义的反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        // 设置offset提交方式
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
            /**
             * 在分区在平衡的时候提交offset，防止再平衡之后重复消费
             * @param collection
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                /*
                    本方法在consumer分重平衡分配了partition之后，消费之前开始调用
                 */
                for(TopicPartition tp : collection){
                    // 从db中读取了offset之后再开始消费
                    consumer.seek(tp, KafkaConsumerSeek.getOffsetFromDb());
                }
            }
        });

        try {
            while (true) {

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync();
            }
        }finally {
            consumer.close();
        }

    }

}
