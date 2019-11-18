package ConsumerApi;

import ProducerApi.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义的反序列化类
 */
public class CompanyDeserializer implements Deserializer<Company> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {

        if (data == null) {
            return null;
        }

        if (data.length < 8) {
            throw new SerializationException("Size of data received " + "by DemoDeseriali zer is shorter than expected!");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        int nameLen, addressLen;
        String name = null, address= null;

        nameLen = buffer.getInt();
        byte[] nameBytes = new byte[nameLen];
        buffer.get(nameBytes);

        addressLen = buffer.getInt();
        byte[] addressBytes = new byte[addressLen];
        buffer.get(addressBytes);

        try {
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return new Company(name, address);
    }

    @Override
    public void close() {

    }
}
