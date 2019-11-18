package ConsumerApi;

import jdk.nashorn.internal.ir.Assignment;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * kafka消费者指定<partition, offset>消费
 */
public class KafkaConsumerSeek {

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
        consumer.subscribe(Arrays.asList(topic));

        Set<TopicPartition> assignment = new HashSet<>();

        // 判断消费者是否分配到了分区
        while (0 == assignment.size()){
            consumer.poll(Duration.ofMillis(100L));
            // 获取分配的分区方案
            assignment = consumer.assignment();
        }
        /*
            从每个分区的末尾开始消费数据
         */
        Map<TopicPartition, Long> offsets = consumer.endOffsets(assignment);
        for(TopicPartition tp : assignment){
            consumer.seek(tp, offsets.get(tp));
        }


        /**
         * 按照时间进行消费
         *
         * 从前一天的时间点开始消费
         *
         */
        Map<TopicPartition, Long> timeStampToSearch = new HashMap<>();
        for(TopicPartition tp : assignment){
            timeStampToSearch.put(tp, System.currentTimeMillis() - 1*24*3600*1000);
        }
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestampMap =
                consumer.offsetsForTimes(timeStampToSearch);
        for(TopicPartition tp : assignment){
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestampMap.get(tp);
            if(null != offsetAndTimestamp){
                consumer.seek(tp, offsetAndTimestamp.offset());
            }
        }


        /**
         * 将kafka的消费的offset写入db种
         */
        for(TopicPartition tp : assignment){
            long offset = getOffsetFromDb();
            consumer.seek(tp, offset);
        }
        while(true){

            ConsumerRecords<String, String> poll = consumer.poll(Duration.ofMillis(100));
            for(TopicPartition tp : poll.partitions()){

                List<ConsumerRecord<String, String>> records = poll.records(tp);
                for(ConsumerRecord<String, String> record : records){

                    // procee the record
                }

                long offset = records.get(records.size() - 1).offset();
                // 将offset写入db中
                storedOffsetToDb(tp, offset + 1);

            }

        }


    }

    /**
     * 从数据库中获取offset
     * @return
     */
    public static long getOffsetFromDb(){
        return 0L;
    }

    /**
     * 将offset写入到db中
     */
    public static void storedOffsetToDb(TopicPartition tp, long offset){

    }

}
