package ProducerApi;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义的拦截器
 */
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> producerRecord) {

        // 获取要发送的消息
        String value = producerRecord.value();
        StringBuilder sb = new StringBuilder();
        sb.append(value).append("prefix_laowu_plus");

        return new ProducerRecord<>(producerRecord.topic(), producerRecord.partition(),
                producerRecord.timestamp(), producerRecord.key(), producerRecord.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

        if(null == e){
            ++sendSuccess;
        }else{
            ++sendFailure;
        }

    }

    @Override
    public void close() {

        System.out.println("send msg success ratio is :" + (sendSuccess/(sendSuccess + sendFailure)));

    }

    @Override
    public void configure(Map<String, ?> map) {
        // do nothing
    }
}
