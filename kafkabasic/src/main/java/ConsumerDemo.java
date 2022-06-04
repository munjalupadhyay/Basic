import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    public static void main(String args[]){
        Properties properties=getProperties();
        plainConsumer(properties);
    }

    public static void plainConsumer(Properties properties) {
        String topicName="temp_topic";
        KafkaConsumer<String,String> kafkaConsumer=new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topicName));
        while(true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records){
                System.out.println("Key= "+record.key()+" value= "+record.value());
                System.out.println("Topic= "+record.topic()+ "Partition= "+record.partition()
                        +" offset= "+record.offset());
            }

            // if you set enable.auto.commit.offset to false. you will have to take care of comitting offset to kafka
            // their are two methos to do that, commitAsync() and commitSync().
            // commitSync() will block your running thread. untill the thread writes the offset to broker.
            // commitAsync() is a non blocking call and your main thread who is polling will never be blocked.
            // but if commitAsync() fails. you offser will not get comitted , and you will not even know that.
            // their is on more method commitAsync(callback). this callback will be called by the thread who is polling. and you can write error message or may be write logic for retry.
            // kafkaConsumer.commitAsync();
        }
    }

    private static Properties getProperties() {
        Properties properties=new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "application1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // ENABLE_AUTO_COMMIT_CONFIG will be default true, so after auto.commit.interval.ms  time it will be automatically comitted to kafka.
        // if you make ENABLE_AUTO_COMMIT_CONFIG as false , your consumer have to take care of commiting offset to broker.
        //properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }
}
