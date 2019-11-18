package demo01;

import java.util.Properties;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


/**
 * 向kafka生产数据
 */

public class ProducerDemo {
	public static void main(String[] args) {

		Properties props = new Properties();
		props.put("zk.connect", "node01:2181,node02:2181,node03:2181");
		props.put("metadata.broker.list", "node01:9092,node02:9092,node03:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i <= 20; i++)
			producer.send(new ProducerRecord<String, String>("test", "it " + i));
		producer.close();

	}
}