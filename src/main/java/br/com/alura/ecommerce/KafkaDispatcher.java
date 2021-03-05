package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

class KafkaDispatcher<T> implements Closeable {
    private final KafkaProducer<String, T> producer;

    KafkaDispatcher(){
        this.producer = new KafkaProducer<String,T>(properties());
    }
    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.CLIENT_ID_CONFIG,FraudDetectorService.class.getSimpleName() + UUID.randomUUID().toString());
        return properties;
    }

    public void send(String topic, String key, T value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic,key,value);
        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("Enviado com sucesso para o tópico: " + data.topic() + ":: particao " + data.partition() + ":: offset ::" + data.offset() + " :: timestamp " + data.timestamp());
        };
        this.producer.send(record,callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}

