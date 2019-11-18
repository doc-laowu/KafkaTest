package ProducerApi;

import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 * 自定义的序列化器,使用时直接使用该类申明
 */
public class CompanySerializer implements Serializer<Company> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {
        // do nothing
    }

    @Override
    public byte[] serialize(String s, Company data) {

        if(null == data){
            return null;
        }
        byte[] name, address;

        try {

            if (data.getName() != null) {
                name = data.getName().getBytes("UTF-8");
            } else {
                return new byte[0];
            }

            if (data.getAddress() != null) {
                address = data.getAddress().getBytes("UTF-8");
            } else {
                return new byte[0];
            }

            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + name.length + address.length);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);

            return buffer.array();
        }catch (UnsupportedEncodingException e){
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {
        // do nothing
    }
}
