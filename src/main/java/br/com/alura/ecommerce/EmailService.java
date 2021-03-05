package br.com.alura.ecommerce;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try(var service = new KafkaService(EmailService.class.getSimpleName(),"ECOMMERCE_SEND_EMAIL", emailService::parse)) {
            service.run();
        }
    }
    private void parse(ConsumerRecord<String,String> record) {
        System.out.println("-----------------------------------------------------------");
        System.out.printf("Email Sent!");
        System.out.println("Particao: " + record.partition());
        System.out.println("Topico: " + record.topic());
        System.out.println("Offset: " + record.offset());
        System.out.println("Key: " + record.key());
        System.out.println("Value: " + record.value());
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
