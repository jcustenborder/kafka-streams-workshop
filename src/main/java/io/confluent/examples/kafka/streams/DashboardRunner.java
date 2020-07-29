package io.confluent.examples.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class DashboardRunner extends StreamsRunner {
  @Override
  public Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();


    return builder.build();
  }

}
