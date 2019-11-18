package ProducerApi;


/**
 * 需要序列化的pojo类
 */
public class Company {

    public Company(String name, String address) {
        this.name = name;
        this.address = address;
    }

    private String name;

    private String address;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

}
