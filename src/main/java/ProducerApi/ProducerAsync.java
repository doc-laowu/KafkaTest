package ProducerApi;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerAsync {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:2181,node02:2181,node03:2181");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 使用自定义的序列化器
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        //使用自定义的分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        // 使用自定义的拦截器, 配置成拦截器链
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ProducerInterceptorPrefix.class.getName() + ","
                + ProducerInterceptorPrefixPlus.class.getName());

        /*
            生产者客户端中用于缓存消息的缓冲区大小，
         */
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        /*
            用于指定 ProducerBatch 可以复用内存区域的大小
         */
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");

        /*
            用来设定 KafkaProducer 对应的客户端 id
         */
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer.test.id");

        /*
            用来控制 KafkaProducer 中 sendQ 方法和 partitionsF or（）方法的阻塞时间。
            当生产者的发 送缓冲区己满，或者没有可用的元数据时，这 些方法就会阻塞
         */
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "60000 ");

        /*
            retries 参数用来配置生产者重试的次数
         */
        props.put(ProducerConfig.RETRIES_CONFIG, "3");

        /*
            如果在这个时间内元数据没有更新的话会被 强制更新
         */
        props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, "300000");

        /*
           两次失败重启之间的间隔

           当ack > 0时，重试可能会导致数据乱序，此时需要将以下参数设置为1

           max.in.flight.requests.per.connection
         */
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100");

        /*
            以限制每个连接（也就是 客户端与 Node 之间的连接）最多缓存的请求数
         */
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        /*
            这个配置的值有
            0 : 不需要确认是否写入副本，直接向生产者响应
            1 : 需要有一个fllower副本写入该消息
            -1 (all) : 需要等待所有的fllower副本都写入该条消息(需要min.insync.replicas配合达到高可靠性)
         */
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        /*
             max.request.size : 端生产者客户端能发送的消息的最大值,需要注意该值不能超过broker端的message.max.bytes的值
         */
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576B");

        /*
            来指定消息的压缩方式，默认值为“none”
         */
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        /*
            指定在多久之后关闭限制的连接
         */
        props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, "540000");

        /*
        这个参数用来指定生产者发送 ProducerBatch 之前等待更多消息（ ProducerRecord）加入 Producer Batch 的时间，默认值为 0。
        生产者客户端会在 ProducerBatch 被填满或等待时间超过 linger.ms 值时发迭出去。增大这个参数的值会增加消息的延迟，
        但是同时能提升一定的吞吐量
         */
        props.put(ProducerConfig.LINGER_MS_CONFIG, "540000");

        /*
            这个参数用来设置 Socket 接收消息缓冲区（ SO 阻CBUF）的大小
         */
        props.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, "32768B");

        /*
            设置 Socket 发送消息缓冲区 CSO SNDBUF）的大小
         */
        props.put(ProducerConfig.SEND_BUFFER_CONFIG, "131072B");

        /*
            这个参数用来配置 Producer 等待请求响应的最长时间，默认值为 30000 (ms）。请求超时 之后可以选择进行重试。
            注意这个参数需要比 broker 端参数 replica.lag.time.max.ms 的值要大，这样可以减少因客户端重试而
            引起的消息重复的概率。
         */
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");




        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i <= 20; i++){

            ProducerRecord msg = new ProducerRecord<String, String>("test", "it " + i);

            producer.send(msg, new Callback() {

                // 当消息发送完成后进行回调

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if(null != recordMetadata && null == e){
                        System.out.println(recordMetadata.topic() + ":"
                                + recordMetadata.partition() + ":" + recordMetadata.offset());
                    }
                    if(null == recordMetadata && null != e){
                        e.printStackTrace();
                    }

                }
            });
        }

        // 关闭客户端
        producer.close();

    }

}
