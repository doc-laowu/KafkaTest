package demo01;


import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.clients.producer.ProducerInterceptor;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MySimpleConsumer {
    public static void main(String[] args) {
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("node01");
        brokers.add("node02");
        brokers.add("node03");
        //端口号
        int port = 9092;
        //主题
        String topic = "test";
        //分区号
        int partition = 0;
        //偏移量
        long offset = 10;
        getData(brokers, port, topic, partition, offset);
    }
    public static void getData(List<String> brokers, int port, String topic, int partition, long offset){
        PartitionMetadata partitionMetadatum = findLeader(brokers, port, topic, partition);
        String leader = partitionMetadatum.leader().host();
        SimpleConsumer consumer = new SimpleConsumer(leader, port, 1000, 1024 * 4, "client" + topic);
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 1000).build();

        FetchResponse fetchResponse = consumer.fetch(fetchRequest);
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {

            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println("offset:"+messageAndOffset.offset()
                    +"==="+new String(bytes));

            /**
             * 打印 key值， 如果没有则不打印
             */
//            ByteBuffer key = messageAndOffset.message().key();
//            byte[] bytes1 = new byte[key.limit()];
//            key.get(bytes1);
//            System.out.println("===key:"+new String(bytes1));
        }
    }
    //获取分区的leader
    public static PartitionMetadata findLeader(List<String> brokers,int port,String topic,int partition){
        for (String broker : brokers) {
            SimpleConsumer consumer = new SimpleConsumer(broker, port, 1000, 1024 * 4, "client");
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));

            TopicMetadataResponse topicMetadataResponse = consumer.send(topicMetadataRequest);

            List<TopicMetadata> topicMetadata = topicMetadataResponse.topicsMetadata();
            for (TopicMetadata metadatum : topicMetadata) {

                List<PartitionMetadata> partitionMetadata = metadatum.partitionsMetadata();
                for (PartitionMetadata partitionMetadatum : partitionMetadata) {
                    if(partitionMetadatum.partitionId()==partition){
                        return partitionMetadatum;
                    }
                }
            }
        }
        return null;
    }
}

