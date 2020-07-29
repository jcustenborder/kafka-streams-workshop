package io.confluent.examples.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public abstract class StreamsRunner {
  private static final Logger log = LoggerFactory.getLogger(StreamsRunner.class);


  protected <T> Serde<T> serde(Class<T> cls) {
    return null;
  }

  public abstract Topology buildTopology();

  public static void run(StreamsRunner runner) {
    Topology topology = runner.buildTopology();
    Properties properties = new Properties();

    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
    properties.put("schema.registry.url", "http://schema-registry:8081");

    String applicationName = runner.getClass().getName();
    //Kafka Streams requires local storage. This is replicated to the change log topics on the kafka brokers
    File stateDirectory = new File("/tmp", applicationName);
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDirectory.getAbsolutePath());
    //Every streams application must have a unique application ID.
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);

    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
  }
}
