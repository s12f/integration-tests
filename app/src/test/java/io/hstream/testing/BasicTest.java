package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.Consumer;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Producer;
import io.hstream.RecordId;
import io.hstream.Stream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(BasicExtension.class)
class BasicTest {

  private String hStreamDBUrl;
  private HStreamClient hStreamClient;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  @BeforeEach
  public void setup() throws Exception {
    System.out.println("db url: " + hStreamDBUrl);
    // Thread.sleep(1000000);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  void testCreateStream() throws Exception {
    final String streamName = randText();
    hStreamClient.createStream(streamName);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
  }

  @Test
  void testDeleteStream() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    hStreamClient.deleteStream(streamName);
    streams = hStreamClient.listStreams();
    Assertions.assertEquals(0, streams.size());
  }

  @Test
  void testWriteRaw() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    RecordId rId = producer.write(record).join();
    Assertions.assertNotNull(rId);
  }

  @Test
  void testWriteJSON() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    var producer = hStreamClient.newProducer().stream(streamName).build();
    HRecord hRec = HRecord.newBuilder().put("x", "y").put("acc", 0).put("init", false).build();
    RecordId rId = producer.write(hRec).join();
    Assertions.assertNotNull(rId);
  }

  @Test
  void testWriteRawBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    Random rand = new Random();
    byte[] rRec = new byte[128];
    var xs = new CompletableFuture[100];
    for (int i = 0; i < 100; i++) {
      rand.nextBytes(rRec);
      xs[i] = producer.write(rRec);
    }
    CompletableFuture.allOf(xs).join();
    for (int i = 0; i < 100; i++) {
      Assertions.assertNotNull(xs[i]);
    }
  }

  @Test
  void testWriteJSONBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer =
        hStreamClient.newProducer().stream(streamName).enableBatch().recordCountLimit(100).build();
    Random rand = new Random();
    var xs = new CompletableFuture[100];
    for (int i = 0; i < 100; i++) {
      HRecord hRec =
          HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
      xs[i] = producer.write(hRec);
    }
    CompletableFuture.allOf(xs).join();
    for (int i = 0; i < 100; i++) {
      Assertions.assertNotNull(xs[i]);
    }
  }

  @Test
  void testWriteRawBatchMultiThread() throws Exception {
    Producer producer =
        hStreamClient.newProducer().stream(randStream(hStreamClient))
            .enableBatch()
            .recordCountLimit(10)
            .build();
    Random rand = new Random();
    final int cnt = 100;
    var xs = new CompletableFuture[100];

    Thread t0 =
        new Thread(
            () -> {
              for (int i = 0; i < cnt / 2; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                xs[i] = producer.write(rRec);
              }
            });

    Thread t1 =
        new Thread(
            () -> {
              for (int i = cnt / 2; i < cnt; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                xs[i] = producer.write(rRec);
              }
            });

    t0.start();
    t1.start();
    t0.join();
    t1.join();
    for (int i = 0; i < cnt; i++) {
      Assertions.assertNotNull(xs[i]);
    }
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  void testConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random random = new Random();
    byte[] rawRecord = new byte[100];
    final int count = 10;
    for (int i = 0; i < count; ++i) {
      random.nextBytes(rawRecord);
      producer.write(rawRecord).join();
    }

    AtomicInteger readCount = new AtomicInteger();
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer-1")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  readCount.incrementAndGet();
                  responder.ack();
                })
            .build();

    Consumer consumer2 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer-2")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  readCount.incrementAndGet();
                  responder.ack();
                })
            .build();

    Consumer consumer3 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer-3")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  readCount.incrementAndGet();
                  responder.ack();
                })
            .build();

    consumer1.startAsync().awaitRunning();
    consumer2.startAsync().awaitRunning();
    consumer3.startAsync().awaitRunning();

    Thread.sleep(5000);

    consumer1.stopAsync().awaitTerminated();
    consumer2.stopAsync().awaitTerminated();
    consumer3.stopAsync().awaitTerminated();

    Assertions.assertEquals(count, readCount.get());
  }

  @Test
  public void testConsumerInTurn() throws Exception {
    final int recordCount = 10;
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);

    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random random = new Random();
    for (int i = 0; i < recordCount; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      producer.write(rawRecord).join();
    }

    final int maxReceivedCountC1 = 3;
    CountDownLatch latch1 = new CountDownLatch(1);
    AtomicInteger c1ReceivedRecordCount = new AtomicInteger(0);
    Consumer consumer1 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer1")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  if (c1ReceivedRecordCount.get() < maxReceivedCountC1) {
                    responder.ack();
                    if (c1ReceivedRecordCount.incrementAndGet() == maxReceivedCountC1) {
                      latch1.countDown();
                    }
                  }
                })
            .build();
    consumer1.startAsync().awaitRunning();
    latch1.await();
    consumer1.stopAsync().awaitTerminated();

    Thread.sleep(3000);

    final int maxReceivedCountC2 = recordCount - maxReceivedCountC1;
    CountDownLatch latch2 = new CountDownLatch(1);
    AtomicInteger c2ReceivedRecordCount = new AtomicInteger(0);
    Consumer consumer2 =
        hStreamClient
            .newConsumer()
            .subscription(subscription)
            .name("consumer2")
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  if (c2ReceivedRecordCount.get() < maxReceivedCountC2) {
                    responder.ack();
                    if (c2ReceivedRecordCount.incrementAndGet() == maxReceivedCountC2) {
                      latch2.countDown();
                    }
                  }
                })
            .build();
    consumer2.startAsync().awaitRunning();
    latch2.await();
    consumer2.stopAsync().awaitTerminated();

    Assertions.assertEquals(recordCount, c1ReceivedRecordCount.get() + c2ReceivedRecordCount.get());
  }
}
