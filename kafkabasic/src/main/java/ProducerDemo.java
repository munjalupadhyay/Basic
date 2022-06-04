import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        System.out.println("Helllo");

        Properties properties = getProperties();

        //plainProducer(properties);

        //plainProducerWithCallBack(properties);

        // producerWithKeys(properties);

    }

    private static void plainProducerWithCallBack(Properties properties) {

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world"+i);

            // whatever you pass in callback it will get called when kafka client got success from kafka broker
            producer.send(producerRecord, new Callback() {
                @Override public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("message saved to : " + "Topic " + recordMetadata.topic() + " Partition "
                                + recordMetadata.partition() + " Offset " + recordMetadata.offset() + " Timestamp "
                                + recordMetadata.timestamp());
                    }else{
                        e.printStackTrace();
                    }
                }
            });

        }
        producer.flush();
        producer.close();
    }

    private static void producerWithKeys(Properties properties){
        // by default if we do not specify key then it will be null and your message can go to any partition.
        // but if se specify jey , then we can assure that all message with same key will be in same partition. and odering between those messages is gaurenteed.

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        for(int i=0;i<10;i++) {
            String topicName="temp_topic";
            String key= "_id"+(i%3);
            String value="hello world"+i;
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key ,value);

            producer.send(producerRecord, new Callback() {
                @Override public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("message saved to : " + "Topic " + recordMetadata.topic() + " Partition "
                                + recordMetadata.partition() + " Offset " + recordMetadata.offset() + " Timestamp "
                                + recordMetadata.timestamp());
                    }else{
                        e.printStackTrace();
                    }
                }
            });
        }
        producer.flush();
        producer.close();
    }

    private static void plainProducer(Properties properties) {
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);
        ProducerRecord<String,String> producerRecord=new ProducerRecord<>("demo_java","hello world");
        producer.send(producerRecord);
        producer.flush();
        producer.close();
    }

    private static Properties getProperties() {
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
