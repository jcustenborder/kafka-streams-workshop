package io.confluent.examples.kafka.streams;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;

import java.math.BigDecimal;
import java.time.LocalDate;

import static java.time.temporal.TemporalAdjusters.firstDayOfMonth;

public class SpendingCategorizationStreamsApplication extends StreamsRunner {
  @Override
  public Topology buildTopology() {
    /*
    Serdes are how we serialize and deserialize data that is stored in Kafka. Here we are defining
    a serde for the key and value that we are going to be using.
     */
    Serde<AccountKey> transactionEventKeySerde = serde(AccountKey.class, true);
    Serde<TransactionEvent> transactionEventSerde = serde(TransactionEvent.class, false);
    Serde<SpendingCategoryKey> spendingCategoryKeySerde = serde(SpendingCategoryKey.class, true);
    Serde<SpendingCategory> spendingCategorySerde = serde(SpendingCategory.class, false);

    StreamsBuilder streamsBuilder = new StreamsBuilder();

    /*
    Stream of incoming transactions keyed by the account.
    Consumed.with is specifying how we should read the data in Kafka. Here we are saying that
    the key of the message is TransactionEventKey and the value is TransactionEvent.
     */
    KStream<AccountKey, TransactionEvent> accountTransactionStream = streamsBuilder
        .stream("transactions", Consumed.with(transactionEventKeySerde, transactionEventSerde));

    KStream<SpendingCategoryKey, SpendingCategory> spendingCategoryStream = accountTransactionStream.groupBy(
        (transactionEventKey, transactionEvent) -> {
      /*
      The transaction dates are spread across an entire month. We need to adjust each of the dates
      so that we can group by the start of the period and the account id, and the mccCode (Merchant category code).
      We're going to take the date of the transaction and move the day to the 1st day of the month.
       */
          LocalDate adjustedDate = transactionEvent.getTransactionDate().with(firstDayOfMonth());
          return SpendingCategoryKey.newBuilder()
              .setAccountID(transactionEvent.getAccountID())
              .setCategoryCode(transactionEvent.getMccCode())
              .setPeriod(adjustedDate)
              .build();
        },
        Grouped.with(spendingCategoryKeySerde, transactionEventSerde)
    ).aggregate(
        /*
        This is an initializer for the initial value. For this case we are just going to use a
        blank object since we're going to use the values from the key later.
         */
        () -> SpendingCategory.newBuilder()
            .setAccountID(0)
            .setCategoryCode(0)
            .setAmount(BigDecimal.ZERO)
            .setPeriod(LocalDate.ofEpochDay(0))
            .build(),
        /*
        Take each of the incoming values and add the amount to the last amount.
         */
        (spendingCategoryKey, transactionEvent, lastValue) -> {
          //Take the value of the previous amount and add it to the current amount.
          BigDecimal amount = lastValue.getAmount().add(transactionEvent.getAmount());
          return SpendingCategory.newBuilder()
              .setAccountID(spendingCategoryKey.getAccountID())
              .setPeriod(spendingCategoryKey.getPeriod())
              .setCategoryCode(spendingCategoryKey.getCategoryCode())
              .setAmount(amount)
              .build();
        },
        Materialized.with(spendingCategoryKeySerde, spendingCategorySerde)
    ).toStream();
    /*
    We have now taken our incoming TransactionEvent(s) and grouped them by the period (ex 12/01/2019), account id,
    and the category id.
     */
    spendingCategoryStream.to("spendingbycategory", Produced.with(spendingCategoryKeySerde, spendingCategorySerde));
    return streamsBuilder.build();
  }

  public static void main(String... args) throws Exception {
    StreamsRunner.run(new SpendingCategorizationStreamsApplication());
  }
}
