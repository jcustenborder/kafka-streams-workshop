package io.confluent.examples.kafka.streams;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Console;
import java.time.Instant;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpendingCategoryProducer {
  private static final Logger log = LoggerFactory.getLogger(SpendingCategoryProducer.class);

  static String readPattern(Console console, String message, Pattern pattern) {
    while (true) {
      System.out.println(message);
      String line = console.readLine();

      Matcher matcher = pattern.matcher(line);
      if (matcher.matches()) {
        return line;
      } else {
        System.out.println(
            String.format("'%s' does not match '%s'", line, pattern.pattern())
        );
      }
    }
  }

  static long readNumber(Console console, String message) {
    Pattern numberPattern = Pattern.compile("^\\d+$");
    String number = readPattern(console, message, numberPattern);
    return Long.parseLong(number);
  }

  static String readString(Console console, String message) {
    System.out.println(message);
    return console.readLine();
  }

  public static void main(String[] args) {
    Console console = System.console();

    if (null == console) {
      log.error("No Console found.");
      return;
    }

    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-0-internal:9092");
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"confluent\" password=\"password\";");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry-0-internal:8081");


    try (Producer<Object, Object> producer = new KafkaProducer<>(properties)) {
      while (true) {
        long customerID = readNumber(console, "Customer Number");
        String division = readPattern(console, "Division", Pattern.compile("^[a-z]+$"));
        String content = readString(console, "content");

        CustomerDashboardKey key = CustomerDashboardKey.newBuilder()
            .setCustomerID(customerID)
            .build();
        CustomerDashboardContentUpdateEvent value = CustomerDashboardContentUpdateEvent.newBuilder()
            .setCustomerID(customerID)
            .setDivision(division)
            .setContent(content)
            .setUpdatedDate(Instant.now())
            .build();
        log.info("Sending:\n{}\n{}", key, value);
        ProducerRecord<Object, Object> record = new ProducerRecord<>(
            "dashboard.updates",
            key,
            value
        );
        producer.send(record, (recordMetadata, e) -> {
          if (null != e) {
            log.error("Exception thrown", e);
          } else {
            log.trace(
                "Record written to {}:{}:{}",
                recordMetadata.topic(),
                recordMetadata.partition(),
                recordMetadata.offset()
            );
          }
        });
      }
    }
  }
}
