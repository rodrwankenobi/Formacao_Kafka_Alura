package br.com.alura.ecommerce;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
            KafkaProducer<String, String> producer = new KafkaProducer<>(properties());
            var key = UUID.randomUUID().toString();
            var value= key + "1234,1234,1345";
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER",key,value);
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("Enviado com sucesso para o tópico: " + data.topic() + ":: particao " + data.partition() + ":: offset ::" + data.offset() + " :: timestamp " + data.timestamp());
            };
            var email = "Thank you! We are processing your order!";
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL",key,email);
            producer.send(record,callback).get();
            producer.send(emailRecord,callback);
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,FraudDetectorService.class.getSimpleName() + UUID.randomUUID().toString());
        return properties;
    }
}
