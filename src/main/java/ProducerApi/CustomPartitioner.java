package ProducerApi;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 自定义序列化器
 */
public class CustomPartitioner implements Partitioner {

    private AtomicInteger counter = new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keybytes, Object value,
                         byte[] valuebytes, Cluster cluster) {

        // 获取分区列表
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);

        int numPartitions = partitionInfos.size();

        if(null == key){
            return counter.getAndIncrement() % numPartitions;
        }else {
            return Utils.abs(Utils.murmur2(keybytes)) % numPartitions;
        }
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
    }
}
