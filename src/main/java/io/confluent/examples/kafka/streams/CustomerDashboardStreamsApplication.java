package io.confluent.examples.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class CustomerDashboardStreamsApplication extends StreamsRunner {

  @Override
  public Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    Serde<CustomerDashboardKey> customerDashboardKeySerde = serde(CustomerDashboardKey.class, true);
    Serde<CustomerDashboardContentUpdateEvent> customerDashboardUpdateSerde = serde(CustomerDashboardContentUpdateEvent.class, false);
    Serde<CustomerDashboardEvent> customerDashboardSerde = serde(CustomerDashboardEvent.class, false);

    /*
    Data is written to the topic keyed by the customer id. Each update contains the content for the update
    as well as the division that owns the content. In this example we are using a single input topic
    called "dashboard.updates" to receive all of the updates for a customer. This means that each
    division of the company would write to the same topic in order to aggregate dashboard content. If this
    is not desired and you prefer to have each division own their topic, then you can specify this here
    by using multiple topics.
     */
    KStream<CustomerDashboardKey, CustomerDashboardContentUpdateEvent> dashboardUpdateStreams = builder.stream(
        "dashboard.updates",
        Consumed.with(customerDashboardKeySerde, customerDashboardUpdateSerde)
    );

    KTable<CustomerDashboardKey, CustomerDashboardEvent> dashboardAggregateTable =
        // The data is already keyed by the customer ID so we will just group by the existing key.
        dashboardUpdateStreams.groupByKey(
            Grouped.with(customerDashboardKeySerde, customerDashboardUpdateSerde)
        )
            .aggregate(
                // Create an initial object for the aggregation. This is our initial value.
                () -> CustomerDashboardEvent.newBuilder()
                    .setCustomerID(0)
                    .setUpdatedDate(Instant.ofEpochMilli(0))
                    .setContent(new LinkedHashMap<>())
                    .build(),
                (customerDashboardKey, dashboardUpdate, existingDashboard) -> {
                  /*
                  Each of the new content update events are handed to this method in the order that they were created. We take a copy of the
                  existing map containing the previous updates and overlay the new dashboard update on top of it. In this case we are writing
                  over the existing values per division.
                  */
                  Map<String, CustomerDashboardContentUpdateEvent> allExistingContent = new LinkedHashMap<>();
                  if (null != existingDashboard.getContent()) {
                    allExistingContent.putAll(existingDashboard.getContent());
                  }
                  allExistingContent.put(dashboardUpdate.getDivision(), dashboardUpdate);
                  return CustomerDashboardEvent.newBuilder(existingDashboard)
                      .setCustomerID(customerDashboardKey.getCustomerID())
                      .setContent(allExistingContent)
                      .setUpdatedDate(dashboardUpdate.getUpdatedDate())
                      .build();
                },
                Materialized.with(
                    customerDashboardKeySerde,
                    customerDashboardSerde
                )
            );
    dashboardAggregateTable.toStream().to("dashboard", Produced.with(customerDashboardKeySerde, customerDashboardSerde));


    return builder.build();
  }

  public static void main(String... args) throws Exception {
    StreamsRunner.run(new CustomerDashboardStreamsApplication());
  }
}
