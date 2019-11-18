package demo01;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

public class ConsumerDemo {
	private static final String topic = "test";
	private static final Integer threads = 2;

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "node01:2181,node02:2181,node03:2181");
		props.put("group.id", "vvvvv");
		props.put("auto.offset.reset", "smallest");

		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer =Consumer.createJavaConsumerConnector(config);
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(topic, threads);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
		
		for(final KafkaStream<byte[], byte[]> kafkaStream : streams){
			new Thread(new Runnable() {
				@Override
				public void run() {
					for(MessageAndMetadata<byte[], byte[]> mm : kafkaStream){
						String msg = new String(mm.message());
						System.out.println(msg);
					}
				}
			}).start();
		}
	}
}