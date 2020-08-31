package io.confluent.examples.kafka.streams;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public abstract class StreamsRunner {
  private static final Logger log = LoggerFactory.getLogger(StreamsRunner.class);

  private Properties properties;

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  protected <T extends SpecificRecord> Serde<T> serde(Class<T> cls, boolean isKey) {
    Serde<T> result = new SpecificAvroSerde<>();
    Map<String, Object> settings = this.properties.entrySet().stream().collect(
        Collectors.toMap(
            e -> e.getKey().toString(),
            Map.Entry::getValue
        )
    );
    result.configure(settings, isKey);
    return result;
  }

  public abstract Topology buildTopology();

  public static void run(StreamsRunner runner) {

    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    properties.put(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"confluent\" password=\"password\";");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schemaregistry-0-internal:8081");

    String applicationName = runner.getClass().getName();
    //Kafka Streams requires local storage. This is replicated to the change log topics on the kafka brokers
    File stateDirectory = new File("/tmp", applicationName);
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.getAbsolutePath());
    //Every streams application must have a unique application ID.
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);
    runner.setProperties(properties);

    Topology topology = runner.buildTopology();
    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);

    kafkaStreams.start();
  }
}
