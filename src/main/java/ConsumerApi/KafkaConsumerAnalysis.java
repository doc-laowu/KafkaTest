package ConsumerApi;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 消费者的api
 */
public class KafkaConsumerAnalysis {

    private static final String topic = "test";
    private static final Integer threads = 2;
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:2181,node02:2181,node03:2181");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "vvvvv");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "vvvvv_local");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 使用自定义的反序列化类
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        // 设置offset提交方式
        // 自动定期提交方式
//        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 手动提交方式
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅整个topic
//        consumer.subscribe(Arrays.asList(topic));
        // consumer获取kafka指定topic的分区信息
//        List<TopicPartition> partitions = new ArrayList<>();
//        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
//        for(PartitionInfo pi : partitionInfos){
//            partitions.add(new TopicPartition(pi.topic(), pi.partition()));
//        }
        // 订阅特定的分区
//        consumer.assign(partitions);


        try {

            /**
             *
             *kafka的消费者提供了 pause(暂停消费者) 和 resume(回复消费者) 这两个方法
             *
             *
             * 退出程序的操作可以通过
             *
             * isRunning.set(false);
             *
             * 和
             *
             * consumer.wakeup(); 这种方式退出消费者需要捕获对应的异常信息
             *
             */
            while (isRunning.get()) {

                ConsumerRecords<String, String> records = consumer.poll(1000L);

//                for (ConsumerRecord<String, String> record : records) {
//
//                    System.out.println(record.topic() + ":" + record.partition() + ":" + record.offset());
//                    System.out.println(record.key() + ":" + record.value());
//
//                }

                // 手动同步提交代码
//                consumer.commitSync();





                /**
                 * 按分区粒度同步提交offset
                 */
                for(TopicPartition partition : records.partitions()){

                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

                    for(ConsumerRecord<String, String> record : partitionRecords){
                        System.out.println(record.topic() + ":" + record.partition() + ":" + record.offset());
                        System.out.println(record.key() + ":" + record.value());
                    }
                    // 按照每个分区的粒度进行offset提交
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition,
                            new OffsetAndMetadata(lastConsumedOffset + 1)));

                }





                /*
                    异步代码提交方式
                 */
                consumer.commitAsync(new OffsetCommitCallback(){

                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {

                        if(null == e){
                            System.out.println(map);
                        }else{
                            // 提交异常
                        }

                    }
                });


            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {

            try{
                // 当上面异步提交出错的时候，下面可以同步提交一次，不会丢失offset信息
                consumer.commitSync();
            }finally {
                // 关闭消费者
                consumer.close();
            }
        }

    }

}
