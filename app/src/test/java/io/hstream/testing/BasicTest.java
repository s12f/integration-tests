package io.hstream.testing;

import static io.hstream.testing.TestUtils.buildRecord;
import static io.hstream.testing.TestUtils.consume;
import static io.hstream.testing.TestUtils.consumeAsync;
import static io.hstream.testing.TestUtils.diffAndLogResultSets;
import static io.hstream.testing.TestUtils.doProduce;
import static io.hstream.testing.TestUtils.generateKeysIncludingDefaultKey;
import static io.hstream.testing.TestUtils.handleForKeys;
import static io.hstream.testing.TestUtils.handleForKeysSync;
import static io.hstream.testing.TestUtils.makeBufferedProducer;
import static io.hstream.testing.TestUtils.produce;
import static io.hstream.testing.TestUtils.randRawRec;
import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randSubscriptionWithTimeout;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.FlowControlSetting;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.HStreamClientBuilder;
import io.hstream.HStreamDBClientException;
import io.hstream.Producer;
import io.hstream.Record;
import io.hstream.Stream;
import io.hstream.Subscription;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
class BasicTest {

  private static final Logger logger = LoggerFactory.getLogger(BasicTest.class);
  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private List<GenericContainer<?>> hServers;
  private List<String> hServerUrls;
  private String logMsgPathPrefix;
  private ExtensionContext context;
  private final Random globalRandom = new Random();

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(List<GenericContainer<?>> hServers) {
    this.hServers = hServers;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  public void setLogMsgPathPrefix(String logMsgPathPrefix) {
    this.logMsgPathPrefix = logMsgPathPrefix;
  }

  public void setExtensionContext(ExtensionContext context) {
    this.context = context;
  }

  @BeforeEach
  public void setup(TestInfo info) throws Exception {
    logger.info("hStreamDBUrl " + hStreamDBUrl);
    HStreamClientBuilder builder = HStreamClient.builder().serviceUrl(hStreamDBUrl);
    var securityPath = getClass().getClassLoader().getResource("security").getPath();
    Set<String> tags = info.getTags();
    if (tags.contains("tls")) {
      builder = builder.enableTls().tlsCaPath(securityPath + "/ca.cert.pem");
    }
    if (tags.contains("tls-authentication")) {
      builder =
          builder
              .enableTlsAuthentication()
              .tlsKeyPath(securityPath + "/role.key-pk8.pem")
              .tlsCertPath(securityPath + "/signed.role.cert.pem");
    }
    hStreamClient = builder.build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  @Timeout(20)
  void testConnections() throws Exception {
    for (var hServerUrl : hServerUrls) {
      logger.info("hServerUrl is " + hServerUrl);
      try (HStreamClient client = HStreamClient.builder().serviceUrl(hServerUrl).build()) {
        List<Stream> res = client.listStreams();
        Assertions.assertTrue(res.isEmpty());
      }
    }
  }

  @Test
  @Timeout(60)
  void testCreateStream() {
    final String streamName = randText();
    hStreamClient.createStream(streamName);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
  }

  @Test
  @Timeout(60)
  void testListStreams() {
    Assertions.assertTrue(hStreamClient.listStreams().isEmpty());
    var streamNames = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      streamNames.add(randStream(hStreamClient));
    }
    var res =
        hStreamClient.listStreams().parallelStream()
            .map(Stream::getStreamName)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(streamNames.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(60)
  void testDeleteStream() {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    hStreamClient.deleteStream(streamName);
    streams = hStreamClient.listStreams();
    Assertions.assertEquals(0, streams.size());
  }

  @Test
  @Timeout(60)
  void testListSubscriptions() {
    String streamName = randStream(hStreamClient);
    var subscriptions = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      subscriptions.add(randSubscription(hStreamClient, streamName));
    }
    var res =
        hStreamClient.listSubscriptions().parallelStream()
            .map(Subscription::getSubscriptionId)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(subscriptions.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(20)
  void testDeleteSubscription() throws Exception {
    final String stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    Assertions.assertEquals(
        subscription, hStreamClient.listSubscriptions().get(0).getSubscriptionId());
    hStreamClient.deleteSubscription(subscription);
    Assertions.assertEquals(0, hStreamClient.listSubscriptions().size());
    Thread.sleep(1000);
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnNonExistStreamShouldFail() throws Exception {
    String stream = randText();
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(hStreamClient, stream);
        });
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnDeletedStreamShouldFail() throws Exception {
    String stream = randStream(hStreamClient);
    hStreamClient.deleteStream(stream);
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(hStreamClient, stream);
        });
  }

  @Test
  @Timeout(20)
  void testCreateConsumerOnDeletedSubscriptionShouldFail() throws Exception {
    String stream = randStream(hStreamClient);
    String subscription = randSubscription(hStreamClient, stream);
    hStreamClient.deleteSubscription(subscription);

    Assertions.assertThrows(
        ExecutionException.class, () -> consume(hStreamClient, subscription, "c1", 10, x -> false));
  }

  @Test
  @Timeout(60)
  void testCreateConsumerWithoutSubscriptionNameShouldFail() {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> hStreamClient.newConsumer().name("test-consumer").build());
  }

  // -------------------------------------------------------------------------------------------
  // Producer, BufferedProducer
  @Test
  @Timeout(60)
  void testWriteRaw() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    String rId = producer.write(buildRecord(record)).join();
    Assertions.assertNotNull(rId);

    final String subscription = randSubscription(hStreamClient, streamName);
    List<byte[]> res = new ArrayList<>();
    consume(
        hStreamClient,
        subscription,
        "c1",
        20,
        (r) -> {
          res.add(r.getRawRecord());
          return false;
        });
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testWriteRawOutOfPayloadLimitShouldFail() {
    int max = 1024 * 1024 + 20;
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[max];
    rand.nextBytes(record);
    Assertions.assertThrows(Exception.class, () -> producer.write(buildRecord(record)).join());
  }

  @Test
  @Timeout(60)
  void testWriteJSON() throws Exception {
    final String streamName = randStream(hStreamClient);
    List<Stream> streams = hStreamClient.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());

    var producer = hStreamClient.newProducer().stream(streamName).build();
    HRecord hRec = HRecord.newBuilder().put("x", "y").put("acc", 0).put("init", false).build();
    String rId = producer.write(buildRecord(hRec)).join();
    Assertions.assertNotNull(rId);

    final String subscription = randSubscription(hStreamClient, streamName);
    List<HRecord> res = new ArrayList<>();
    consume(
        hStreamClient,
        subscription,
        "c1",
        20,
        null,
        receivedHRecord -> {
          res.add(receivedHRecord.getHRecord());
          return false;
        });
    Assertions.assertEquals(hRec.toString(), res.get(0).toString());
  }

  @Test
  @Timeout(60)
  void testWriteMixPayload() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[128];
    rand.nextBytes(record);
    var rawRecords = new ArrayList<String>();
    var hRecords = new ArrayList<HRecord>();
    for (int i = 0; i < 100; i++) {
      if (rand.nextInt() % 2 == 0) {
        rand.nextBytes(record);
        producer.write(buildRecord(record)).join();
        rawRecords.add(Arrays.toString(record));
      } else {
        HRecord hRec =
            HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
        producer.write(buildRecord(hRec)).join();
        hRecords.add(hRec);
      }
    }

    final String subscription = randSubscription(hStreamClient, streamName);
    List<HRecord> hRes = new ArrayList<>();
    List<String> rawRes = new ArrayList<>();
    int total = rawRecords.size() + hRecords.size();
    AtomicInteger received = new AtomicInteger();
    consume(
        hStreamClient,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          rawRes.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return received.incrementAndGet() != total;
        },
        receivedHRecord -> {
          hRes.add(receivedHRecord.getHRecord());
          return received.incrementAndGet() != total;
        });
    var hRecordInput =
        hRecords.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var hOutputRecord = hRes.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(hRecordInput, hOutputRecord);
    Assertions.assertEquals(rawRecords, rawRes);
  }

  @Test
  @Timeout(60)
  void testWriteRawBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    var records = doProduce(producer, 128, 100);
    producer.close();

    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    consume(
        hStreamClient,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return res.size() < records.size();
        });
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  void testNoBatchWriteInForLoopShouldNotStuck() throws Exception {
    final String streamName = randStream(hStreamClient);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    var records = doProduce(producer, 128, globalRandom.nextInt(100));

    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    consume(
        hStreamClient,
        subscription,
        "c1",
        10,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return res.size() < records.size();
        });
    Assertions.assertEquals(
        records.stream().sorted().collect(Collectors.toList()),
        res.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  @Timeout(60)
  void testWriteJSONBatch() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    Random rand = new Random();
    var futures = new CompletableFuture[100];
    var records = new ArrayList<HRecord>();
    for (int i = 0; i < 100; i++) {
      HRecord hRec =
          HRecord.newBuilder().put("x", rand.nextInt()).put("y", rand.nextDouble()).build();
      futures[i] = producer.write(buildRecord(hRec));
      records.add(hRec);
    }
    CompletableFuture.allOf(futures).join();
    producer.close();

    final String subscription = randSubscription(hStreamClient, streamName);
    List<HRecord> res = new ArrayList<>();
    consume(
        hStreamClient,
        subscription,
        "c1",
        20,
        null,
        receivedHRecord -> {
          res.add(receivedHRecord.getHRecord());
          return res.size() < records.size();
        });
    var input = records.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    var output = res.parallelStream().map(HRecord::toString).collect(Collectors.toList());
    Assertions.assertEquals(input, output);
  }

  // TODO: multi-thread
  @Test
  @Timeout(60)
  void testWriteRawBatchMultiThread() throws Exception {
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(10).ageLimit(10).build())
            .build();
    Random rand = new Random();
    final int cnt = 100;
    var futures = new CompletableFuture[100];

    Thread t0 =
        new Thread(
            () -> {
              for (int i = 0; i < cnt / 2; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                futures[i] = producer.write(buildRecord(rRec));
              }
            });

    Thread t1 =
        new Thread(
            () -> {
              for (int i = cnt / 2; i < cnt; i++) {
                byte[] rRec = new byte[128];
                rand.nextBytes(rRec);
                futures[i] = producer.write(buildRecord(rRec));
              }
            });

    t0.start();
    t1.start();
    t0.join();
    t1.join();
    producer.close();
    for (int i = 0; i < cnt; i++) {
      Assertions.assertNotNull(futures[i]);
    }
  }

  @Test
  @Timeout(60)
  void testMixWriteBatchAndNoBatchRecords() throws Exception {
    final String streamName = randStream(hStreamClient);
    int totalWrites = 10;
    int batchWrites = 0;
    int batchSize = 5;
    BufferedProducer batchProducer = makeBufferedProducer(hStreamClient, streamName, batchSize);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    Random rand = new Random();
    var records = new ArrayList<String>();
    var recordIds = new ArrayList<String>();

    for (int i = 0; i < totalWrites; i++) {
      int next = rand.nextInt(10);
      if (next % 2 == 0) {
        batchWrites++;
        logger.info("[turn]: {}, batch write!!!!!\n", i);
        var writes = new ArrayList<CompletableFuture<String>>(5);
        for (int j = 0; j < batchSize; j++) {
          var rRec = new byte[] {(byte) i};
          records.add(Arrays.toString(rRec));
          writes.add(batchProducer.write(buildRecord(rRec)));
        }
        writes.forEach(w -> recordIds.add(w.join()));
      } else {
        logger.info("[turn]: {}, no batch write!!!!!\n", i);
        var rRec = new byte[] {(byte) i};
        records.add(Arrays.toString(rRec));
        recordIds.add(producer.write(buildRecord(rRec)).join());
      }
    }

    batchProducer.close();

    int totalSize = batchWrites * batchSize + (totalWrites - batchWrites);
    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    List<String> receivedRecordIds = new ArrayList<>();
    consume(
        hStreamClient,
        subscription,
        "test-consumer",
        20,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          receivedRecordIds.add(receivedRawRecord.getRecordId());
          return res.size() < totalSize;
        });
    logger.info(
        "wait join !!!!! batch writes = {}, single writes = {}\n",
        batchWrites,
        totalWrites - batchWrites);
    logger.info("send rid: ");
    Assertions.assertEquals(recordIds.size(), records.size());
    for (int i = 0; i < recordIds.size(); i++) {
      logger.info(recordIds.get(i) + ": " + records.get(i));
    }
    logger.info("received rid");
    for (int i = 0; i < receivedRecordIds.size(); i++) {
      logger.info(receivedRecordIds.get(i) + ": " + res.get(i));
    }
    Assertions.assertEquals(records.size(), res.size());
    Assertions.assertEquals(records, res);
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordAndClose() throws Exception {
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(-1).build())
            .build();
    Random random = new Random();
    final int count = 10;
    CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<String> future = producer.write(buildRecord(rawRecord));
      recordIdFutures[i] = future;
    }
    // flush and close producer
    producer.close();
    CompletableFuture.allOf(recordIdFutures).join();
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordBasedTimer() throws Exception {
    try (BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .batchSetting(BatchSetting.newBuilder().recordCountLimit(100).ageLimit(100).build())
            .build()) {
      Random random = new Random();
      final int count = 10;
      CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
      for (int i = 0; i < count; ++i) {
        byte[] rawRecord = new byte[100];
        random.nextBytes(rawRecord);
        CompletableFuture<String> future = producer.write(buildRecord(rawRecord));
        recordIdFutures[i] = future;
      }
      CompletableFuture.allOf(recordIdFutures).join();
    }
  }

  @Test
  @Timeout(60)
  public void testWriteBatchRawRecordBasedBytesSize() throws Exception {
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(randStream(hStreamClient))
            .batchSetting(
                BatchSetting.newBuilder()
                    .recordCountLimit(100)
                    .ageLimit(-1)
                    .bytesLimit(4096)
                    .build())
            .build();
    Random random = new Random();
    final int count = 42;
    CompletableFuture<?>[] recordIdFutures = new CompletableFuture[count];
    for (int i = 0; i < count; ++i) {
      byte[] rawRecord = new byte[100];
      random.nextBytes(rawRecord);
      CompletableFuture<String> future = producer.write(buildRecord(rawRecord));
      recordIdFutures[i] = future;
    }
    for (int i = 0; i < count - 1; ++i) {
      recordIdFutures[i].join();
    }

    Assertions.assertThrows(
        TimeoutException.class, () -> recordIdFutures[41].get(3, TimeUnit.SECONDS));
    producer.close();
    recordIdFutures[41].join();
  }

  @Test
  @Timeout(60)
  public void testWriteRandomSizeRecords() throws Exception {
    var stream = randStream(hStreamClient);
    BufferedProducer producer =
        hStreamClient.newBufferedProducer().stream(stream)
            .flowControlSetting(FlowControlSetting.newBuilder().bytesLimit(40960).build())
            .build();
    int count = 1000;
    var pairs = produce(producer, count, new TestUtils.RandomSizeRecordGenerator(128, 10240));
    producer.close();
    logger.info("wrote :{}", pairs);

    var sub = randSubscription(hStreamClient, stream);
    var res = new HashMap<String, TestUtils.RecordsPair>();
    consume(hStreamClient, sub, 20, handleForKeysSync(res, count));
    Assertions.assertEquals(pairs, res);
  }

  @Test
  @Timeout(60)
  public void testBadFlowControlSettingShouldFail() {
    var stream = randStream(hStreamClient);
    Assertions.assertThrows(
        HStreamDBClientException.class,
        () ->
            hStreamClient.newBufferedProducer().stream(stream)
                .batchSetting(BatchSetting.newBuilder().bytesLimit(4096).build())
                .flowControlSetting(FlowControlSetting.newBuilder().bytesLimit(1024).build())
                .build());
  }

  @Disabled("HS-937")
  @Test
  @Timeout(60)
  void testCreateConsumerWithExistedConsumerNameShouldFail() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    var future1 = consumeAsync(hStreamClient, subscription, "c1", receivedRawRecord -> false);
    Thread.sleep(1500);
    Assertions.assertThrows(
        ExecutionException.class, () -> consume(hStreamClient, subscription, "c1", 10, x -> false));
    future1.complete(null);
  }

  @Test
  @Timeout(60)
  void testCreateConsumerWithExistedConsumerNameOnDifferentSubscription() throws Exception {
    // should be okay
    final String streamName = randStream(hStreamClient);
    final String subscription0 = randSubscription(hStreamClient, streamName);
    final String subscription1 = randSubscription(hStreamClient, streamName);
    var future1 = consumeAsync(hStreamClient, subscription0, "c1", receivedRawRecord -> false);
    var future2 = consumeAsync(hStreamClient, subscription1, "c1", receivedRawRecord -> false);
    Thread.sleep(1500);
    Assertions.assertFalse(future1.isCompletedExceptionally());
    Assertions.assertFalse(future2.isCompletedExceptionally());
    future1.complete(null);
    future2.complete(null);
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawRecord() throws Exception {
    final String streamName = randStream(hStreamClient);
    var producer = hStreamClient.newProducer().stream(streamName).build();
    var rand = new Random();
    byte[] record = new byte[1024 * 4];
    rand.nextBytes(record);
    String rId = producer.write(buildRecord(record)).join();
    Assertions.assertNotNull(rId);

    final String subscription = randSubscription(hStreamClient, streamName);
    List<byte[]> res = new ArrayList<>();
    var lock = new ReentrantLock();
    consume(
        hStreamClient,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          res.add(receivedRawRecord.getRawRecord());
          return false;
        });
    Assertions.assertArrayEquals(record, res.get(0));
  }

  @Test
  @Timeout(60)
  void testConsumeLargeRawBatchRecord() throws Exception {
    final String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    var records = doProduce(producer, 1024 * 4, 2700);
    producer.close();
    final String subscription = randSubscription(hStreamClient, streamName);
    List<String> res = new ArrayList<>();
    consume(
        hStreamClient,
        subscription,
        "c1",
        35,
        receivedRawRecord -> {
          res.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return res.size() < records.size();
        });
    logger.info("records size = " + records.size());
    logger.info("res size = " + res.size());
    Assertions.assertEquals(records, res);
  }

  // -----------------------------------------------------------------------------------------------
  @Test
  @Timeout(60)
  void testWriteToDeletedStreamShouldFail() throws Exception {
    String stream = randStream(hStreamClient);

    Producer producer = hStreamClient.newProducer().stream(stream).build();

    String id0 = producer.write(randRawRec()).join();
    String id1 = producer.write(randRawRec()).join();
    Assertions.assertTrue(id0.compareTo(id1) < 0);

    hStreamClient.deleteStream(stream);
    Assertions.assertThrows(Exception.class, () -> producer.write(randRawRec()).join());
  }

  // TODO: serviceUrl
  @Test
  @Timeout(60)
  void testMultiThreadListStream() throws Exception {
    randStream(hStreamClient);

    ExecutorService executor = Executors.newCachedThreadPool();
    for (String hServerUrl : hServerUrls) {
      executor.execute(
          () -> {
            HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();
            Assertions.assertNotNull(c.listStreams());
          });
    }
  }

  // TODO: serviceUrl
  @Test
  @Timeout(60)
  void testMultiThreadCreateSameStream() throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<>();

    String stream = randText();

    ArrayList<Thread> threads = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      threads.add(
          new Thread(
              () -> {
                HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();

                try {
                  c.createStream(stream);
                } catch (Exception e) {
                  synchronized (exceptions) {
                    exceptions.add(e);
                  }
                }
              }));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assertions.assertEquals(hServerUrls.size() - 1, exceptions.size());
  }

  // TODO: serviceUrl
  @Test
  @Timeout(60)
  void testCreateThenDeleteStreamFromDifferentServerUrl() throws Exception {
    ArrayList<HStreamClient> clients = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      clients.add(HStreamClient.builder().serviceUrl(hServerUrl).build());
    }
    String stream = randStream(clients.get(0));
    clients.get(1).deleteStream(stream);
    for (int i = 2; i < clients.size(); i++) {
      int finalI = i;
      Assertions.assertThrows(Exception.class, () -> clients.get(finalI).deleteStream(stream));
    }
  }

  // TODO: serviceUrl
  @Test
  @Timeout(60)
  void testMultiThreadDeleteSameStream() throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<>();

    String stream = randStream(hStreamClient);

    ArrayList<Thread> threads = new ArrayList<>();
    for (String hServerUrl : hServerUrls) {
      threads.add(
          new Thread(
              () -> {
                HStreamClient c = HStreamClient.builder().serviceUrl(hServerUrl).build();

                try {
                  c.deleteStream(stream);
                } catch (Exception e) {
                  synchronized (exceptions) {
                    exceptions.add(e);
                  }
                }
              }));
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    Assertions.assertEquals(hServerUrls.size() - 1, exceptions.size());
  }

  // TODO: serviceUrl
  @Test
  @Timeout(60)
  void testWriteRawThenReadFromDifferentServerUrl() throws Exception {
    Random rand = new Random();
    byte[] randRecs = new byte[128];
    rand.nextBytes(randRecs);
    final int total = 64;

    HStreamClient hStreamClient1 = HStreamClient.builder().serviceUrl(hServerUrls.get(1)).build();
    String stream = randStream(hStreamClient1);
    hStreamClient1.close();

    Set<String> recordIds0 = new HashSet<>();
    String subscription = randSubscription(hStreamClient, stream);
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    for (int i = 0; i < total; ++i) {
      recordIds0.add(producer.write(buildRecord(randRecs)).join());
    }

    Set<String> recordIds1 = new HashSet<>();
    HStreamClient hStreamClient2 = HStreamClient.builder().serviceUrl(hServerUrls.get(2)).build();
    consume(
        hStreamClient2,
        subscription,
        "c1",
        20,
        receivedRawRecord -> {
          recordIds1.add(receivedRawRecord.getRecordId());
          return recordIds1.size() < total;
        });
    Assertions.assertEquals(recordIds0, recordIds1);
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistSubscriptionShouldFail() throws Exception {
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          try {
            hStreamClient.deleteSubscription(randText());
          } catch (Throwable e) {
            logger.info("============= error\n{}", e.toString());
            throw e;
          }
        });
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistStreamShouldFail() throws Exception {
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          try {
            hStreamClient.deleteStream(randText());
          } catch (Throwable e) {
            logger.info("============= error\n{}", e.toString());
            throw e;
          }
        });
  }

  // ------------------------------------------------------------------------
  // Security
  /* TLS cases
  Tag("tls"): enable tls in servers and client
  Tag("tls-authentication"): enable tls authentication in servers and client
   */
  @Test
  @Timeout(20)
  @Tag("tls")
  void testTls() {}

  @Test
  @Timeout(20)
  @Tag("tls")
  @Tag("tls-authentication")
  void testTlsAuthentication() {}

  @Test
  @Timeout(20)
  void testUntrustedServer() {
    String caPath = getClass().getClassLoader().getResource("security/ca.cert.pem").getPath();
    Assertions.assertThrows(
        Exception.class, () -> HStreamClient.builder().enableTls().tlsCaPath(caPath).build());
  }

  @Test
  @Timeout(20)
  @Tag("tls")
  @Tag("tls-authentication")
  void testUntrustedClient() {
    String caPath = getClass().getClassLoader().getResource("security/ca.cert.pem").getPath();
    Assertions.assertThrows(
        Exception.class, () -> HStreamClient.builder().enableTls().tlsCaPath(caPath).build());
  }
  // ----------------------------------------------------------------------------------------------
  // consumer group, ordering, keys
  @Test
  void testOrder() throws Exception {
    String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    int count = 1000;
    TestUtils.RecordsPair pair = produce(producer, 1024, count);
    producer.close();
    String subscription = randSubscription(hStreamClient, streamName);
    var ids = new ArrayList<String>(count);
    var records = new ArrayList<String>(count);
    consume(
        hStreamClient,
        subscription,
        streamName,
        10,
        receivedRawRecord -> {
          ids.add(receivedRawRecord.getRecordId());
          records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return ids.size() < count;
        });
    Assertions.assertEquals(pair.ids, ids);
    Assertions.assertEquals(pair.records, records);
  }

  @Test
  void testOrderWithKeys() throws Exception {
    String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    int count = 1000;
    int keys = 5;
    var pairs = produce(producer, 1024, count, keys);
    producer.close();
    String subscription = randSubscription(hStreamClient, streamName);
    var received = new HashMap<String, TestUtils.RecordsPair>(keys);
    AtomicInteger receivedCount = new AtomicInteger();
    consume(
        hStreamClient,
        subscription,
        streamName,
        10,
        receivedRawRecord -> {
          var key = receivedRawRecord.getHeader().getOrderingKey();
          if (!received.containsKey(key)) {
            received.put(key, new TestUtils.RecordsPair());
          }
          received.get(key).ids.add(receivedRawRecord.getRecordId());
          received.get(key).records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return receivedCount.incrementAndGet() < count;
        });
    Assertions.assertEquals(pairs, received);
  }

  @Test
  @Timeout(60)
  void testOrderWithRandomKeys() throws Exception {
    String streamName = randStream(hStreamClient);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 100);
    int count = 1000;
    int keys = 5;
    var pairs = produce(producer, 128, count, new TestUtils.RandomKeyGenerator(keys));
    producer.close();
    String subscription = randSubscription(hStreamClient, streamName);
    var received = new HashMap<String, TestUtils.RecordsPair>(keys);
    AtomicInteger receivedCount = new AtomicInteger();
    consume(
        hStreamClient,
        subscription,
        streamName,
        10,
        receivedRawRecord -> {
          var key = receivedRawRecord.getHeader().getOrderingKey();
          if (!received.containsKey(key)) {
            received.put(key, new TestUtils.RecordsPair());
          }
          received.get(key).ids.add(receivedRawRecord.getRecordId());
          received.get(key).records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
          return receivedCount.incrementAndGet() < count;
        });
    Assertions.assertTrue(diffAndLogResultSets(pairs, received));
  }

  @Test
  @Timeout(60)
  void testConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 50);
    final int count = 500;
    final int keysSize = 3;
    // write
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();

    // read
    var received = new HashMap<String, TestUtils.RecordsPair>();
    var latch = new CountDownLatch(count);

    consumeAsync(hStreamClient, subscription, handleForKeys(received, latch));
    consumeAsync(hStreamClient, subscription, handleForKeys(received, latch));
    consumeAsync(hStreamClient, subscription, handleForKeys(received, latch));

    Assertions.assertTrue(latch.await(20, TimeUnit.SECONDS));
    Assertions.assertTrue(diffAndLogResultSets(pairs, received));
  }

  @Test
  @Timeout(60)
  public void testConsumerInTurn() throws Exception {
    final int recordCount = 2500;
    final String streamName = randStream(hStreamClient);

    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 50);
    int keysSize = 1;
    var wrote = produce(producer, 1, 2500, keysSize);
    logger.info("wrote:{}", wrote);
    producer.close();

    var sub = randSubscriptionWithTimeout(hStreamClient, streamName, 5);
    // receive part of records and stop consumers
    final int maxReceivedCountC1 = Math.max(1, globalRandom.nextInt(recordCount / 3));
    final int maxReceivedCountC2 = Math.max(1, globalRandom.nextInt(recordCount / 3));
    var rest = recordCount - maxReceivedCountC1 - maxReceivedCountC2;
    logger.info(
        "maxReceivedCountC1:{}, C2:{}, rest:{}", maxReceivedCountC1, maxReceivedCountC2, rest);
    var received = new HashMap<String, TestUtils.RecordsPair>();

    // consumer 1
    consume(hStreamClient, sub, "c1", 10, handleForKeysSync(received, maxReceivedCountC1));
    logger.info("received:{}", received);
    // waiting for server to handler ACKs
    Thread.sleep(7000);

    // consumer 2
    consume(hStreamClient, sub, "c2", 10, handleForKeysSync(received, maxReceivedCountC2));
    logger.info("received:{}", received);
    // waiting for server to handler ACKs
    Thread.sleep(7000);

    // start a new consumer to consume the rest records.
    consume(hStreamClient, sub, "c3", 10, handleForKeysSync(received, rest));
    Assertions.assertTrue(diffAndLogResultSets(wrote, received));
  }

  @Test
  @Timeout(60)
  void testAddConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 50);
    final int count = 5000;
    final int keysSize = 5;
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();
    var res = new HashMap<String, TestUtils.RecordsPair>();
    var latch = new CountDownLatch(count);
    var f1 = consumeAsync(hStreamClient, subscription, handleForKeys(res, latch));
    var f2 = consumeAsync(hStreamClient, subscription, handleForKeys(res, latch));

    Thread.sleep(1000);
    var f3 = consumeAsync(hStreamClient, subscription, handleForKeys(res, latch));
    Assertions.assertTrue(latch.await(20, TimeUnit.SECONDS));
    CompletableFuture.allOf(f1, f2, f3).complete(null);
    Assertions.assertTrue(diffAndLogResultSets(pairs, res));
  }

  @Test
  @Timeout(60)
  void testReduceConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 50);
    final int count = 5000;
    final int keysSize = 5;
    var wrote = produce(producer, 100, count, keysSize);
    producer.close();
    CountDownLatch signal = new CountDownLatch(count);
    var received = new HashMap<String, TestUtils.RecordsPair>();
    var f1 = consumeAsync(hStreamClient, subscription, handleForKeys(received, signal));
    var f2 = consumeAsync(hStreamClient, subscription, handleForKeys(received, signal));
    var f3 = consumeAsync(hStreamClient, subscription, handleForKeys(received, signal));

    while (signal.getCount() > count / 2) {
      Thread.sleep(100);
    }
    f1.complete(null);

    while (signal.getCount() > count / 3) {
      Thread.sleep(100);
    }
    f2.complete(null);

    boolean done = signal.await(20, TimeUnit.SECONDS);
    f3.complete(null);
    Assertions.assertTrue(done);

    Assertions.assertEquals(wrote, received);
  }

  @Timeout(60)
  @Test
  void testLargeConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    Producer producer = hStreamClient.newProducer().stream(streamName).build();
    int count = 200;
    byte[] rRec = new byte[100];
    for (int i = 0; i < count; i++) {
      producer.write(Record.newBuilder().rawRecord(rRec).orderingKey("k_" + i % 10).build()).join();
    }
    CountDownLatch signal = new CountDownLatch(count);
    // start 5 consumers
    for (int i = 0; i < 5; i++) {
      hStreamClient
          .newConsumer()
          .subscription(subscription)
          .rawRecordReceiver(
              ((receivedRawRecord, responder) -> {
                logger.info("received:{}", receivedRawRecord.getRecordId());
                signal.countDown();
              }))
          .build()
          .startAsync()
          .awaitRunning();
    }

    for (int i = 0; i < 15; i++) {
      Thread.sleep(100);
      hStreamClient
          .newConsumer()
          .subscription(subscription)
          .rawRecordReceiver(
              ((receivedRawRecord, responder) -> {
                logger.info("received:{}", receivedRawRecord.getRecordId());
                signal.countDown();
              }))
          .build()
          .startAsync()
          .awaitRunning();
    }

    Assertions.assertTrue(signal.await(20, TimeUnit.SECONDS), "failed to receive all records");
  }

  @Timeout(60)
  @Test
  void testDynamicConsumerToConsumerGroup() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, streamName);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 50);
    final int count = 20000;
    final int keysSize = 10;
    CountDownLatch signal = new CountDownLatch(count);
    var pairs = produce(producer, 100, count, keysSize);
    producer.close();
    var res = new HashMap<String, TestUtils.RecordsPair>();
    var futures = new LinkedList<CompletableFuture<Void>>();
    // start 5 consumers
    for (int i = 0; i < 5; i++) {
      futures.add(consumeAsync(hStreamClient, subscription, handleForKeys(res, signal)));
    }

    // randomly kill and start some consumers
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      if (globalRandom.nextInt(4) == 0) {
        futures.pop().complete(null);
        logger.info("stopped a consumer");
      } else {
        futures.add(consumeAsync(hStreamClient, subscription, handleForKeys(res, signal)));
        logger.info("started a new consumer");
      }
    }

    Assertions.assertTrue(signal.await(20, TimeUnit.SECONDS), "failed to receive all records");
    futures.forEach(it -> it.complete(null));
    Assertions.assertEquals(pairs, res);
  }

  @Test
  @Timeout(60)
  void testShardBalance() throws Exception {
    var stream = randStream(hStreamClient);
    final String subscription = randSubscription(hStreamClient, stream);
    int shardCount = 10;
    int recordCount = 100;
    int consumerCount = 7;

    // Async Read
    List<List<String>> readRes = new ArrayList<>();
    var futures = new CompletableFuture[consumerCount];
    var receivedKeys = new ArrayList<HashSet<String>>();
    var latch = new CountDownLatch(recordCount);
    for (int i = 0; i < consumerCount; ++i) {
      var records = new LinkedList<String>();
      readRes.add(records);
      var keys = new HashSet<String>();
      receivedKeys.add(keys);
      futures[i] =
          consumeAsync(
              hStreamClient,
              subscription,
              "c" + i,
              receivedRawRecord -> {
                synchronized (keys) {
                  records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
                  keys.add(receivedRawRecord.getHeader().getOrderingKey());
                  latch.countDown();
                  return true;
                }
              });
    }

    Thread.sleep(10000);
    // Write
    Producer producer = hStreamClient.newProducer().stream(stream).build();
    var keys = generateKeysIncludingDefaultKey(shardCount);
    var writeRes = produce(producer, 32, recordCount, new TestUtils.RandomKeyGenerator(keys));
    Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
    CompletableFuture.allOf(futures).complete(null);

    // Analysis
    // Keys balancing part
    logger.info("===== Keys Stats =====");

    HashSet<String> unionOfKeys = new HashSet<>();
    for (int i = 0; i < consumerCount; ++i) {
      HashSet<String> ownedKeys = receivedKeys.get(i);
      logger.info("Consumer {}: {}", i, ownedKeys);
      // 1. When consumer number <= key number, every consumer owns at least 1 key
      Assertions.assertFalse(ownedKeys.isEmpty());
      unionOfKeys.addAll(ownedKeys);
    }
    logger.info("All allocated keys: {}", unionOfKeys);

    // 2. Every item written to the database is read out
    HashSet<String> writeResAsSet = new HashSet<>();
    HashSet<String> readResAsSet = new HashSet<>();
    for (var thisValue : writeRes.values()) {
      writeResAsSet.addAll(thisValue.records);
    }
    for (var thisValue : readRes) {
      readResAsSet.addAll(thisValue);
    }
    Assertions.assertEquals(readResAsSet, writeResAsSet);

    // 3. Assert the union of keys all consumers own is equal to all keys
    HashSet<String> expectedKeys = new HashSet<>(writeRes.keySet());
    Assertions.assertEquals(unionOfKeys, expectedKeys);
  }

  // --------------------------------------------------------------------------------------------
  // ack and resend cases
  @Test
  @Timeout(60)
  void testServerResend() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscriptionName = randSubscriptionWithTimeout(hStreamClient, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 1);
    produce(producer, 1024, 1);
    producer.close();

    var received = new TestUtils.RecordsPair();
    var future =
        consumeAsync(
            hStreamClient,
            subscriptionName,
            "c1",
            receivedRawRecord -> {
              synchronized (received) {
                received.ids.add(receivedRawRecord.getRecordId());
                received.records.add(Arrays.toString(receivedRawRecord.getRawRecord()));
                return true;
              }
            },
            null,
            responder -> {});
    Thread.sleep(3000);
    synchronized (received) {
      Assertions.assertEquals(1, received.ids.size());
    }
    Thread.sleep(4000);
    future.complete(null);
    Assertions.assertEquals(2, received.ids.size());
    Assertions.assertEquals(received.ids.get(0), received.ids.get(1));
    Assertions.assertEquals(received.records.get(0), received.records.get(1));
  }

  @Test
  @Timeout(60)
  void testRandomlyDropACKs() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscriptionName = randSubscriptionWithTimeout(hStreamClient, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName);
    int recordCount = globalRandom.nextInt(100) + 50;
    produce(producer, 128, recordCount);
    producer.close();
    logger.info("wrote {} records", recordCount);

    var received = new AtomicInteger();
    var dropped = new AtomicInteger();
    var future =
        consumeAsync(
            hStreamClient,
            subscriptionName,
            "c1",
            r -> true,
            null,
            responder -> {
              received.incrementAndGet();
              if (globalRandom.nextInt(2) == 0 && received.get() <= recordCount) {
                dropped.incrementAndGet();
              } else {
                responder.ack();
              }
            });
    Thread.sleep(8000);
    future.complete(null);
    logger.info("dropped:{}", dropped.get());
    Assertions.assertEquals(recordCount + dropped.get(), received.get());
  }

  @Test
  @Timeout(60)
  void testBufferedACKs() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscriptionName = randSubscriptionWithTimeout(hStreamClient, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName);
    int recordCount = 999;
    produce(producer, 128, recordCount);
    producer.close();

    var received = new AtomicInteger();
    consume(hStreamClient, subscriptionName, 20, r -> received.incrementAndGet() < recordCount);
    // after consuming all records, and stopping consumer, ACKs should be sent to servers,
    // so next consumer should not receive any new records except ackSender resend.
    Assertions.assertThrows(
        TimeoutException.class, () -> consume(hStreamClient, subscriptionName, 6, r -> false));
  }

  @Test
  @Timeout(60)
  void testACKsWhenStopConsumer() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String sub = randSubscriptionWithTimeout(hStreamClient, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName);
    int recordCount = 999;
    produce(producer, 128, recordCount);
    producer.close();
    logger.info("wrote {} records", recordCount);

    var received = new AtomicInteger();
    int c1 = 500;
    consume(hStreamClient, sub, 20, r -> received.incrementAndGet() < c1);
    logger.info("received {} records", c1);
    // after consuming some records, and stopping consumer, ACKs should be sent to servers,
    // so the count next consumer received should not greater than recordCount - c1.
    Assertions.assertThrows(
        TimeoutException.class,
        () ->
            consume(hStreamClient, sub, 6, r -> received.incrementAndGet() < recordCount - c1 + 1));
  }

  @Test
  @Timeout(60)
  void testIdempotentACKs() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscriptionName = randSubscriptionWithTimeout(hStreamClient, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 32);
    final int count = 99;
    produce(producer, 1024, count);
    producer.close();

    var received = new AtomicInteger();
    var future =
        consumeAsync(
            hStreamClient,
            subscriptionName,
            "c1",
            receivedRawRecord -> received.incrementAndGet() < count,
            null,
            responder -> {
              // duplicate ACKs
              responder.ack();
              responder.ack();
            });
    future.get(20, TimeUnit.SECONDS);
  }

  @Test
  @Timeout(60)
  void testAutoFlushACKs() throws Exception {
    final String streamName = randStream(hStreamClient);
    final String subscriptionName = randSubscriptionWithTimeout(hStreamClient, streamName, 5);
    BufferedProducer producer = makeBufferedProducer(hStreamClient, streamName, 10);
    final int count = 10;
    produce(producer, 1024, count);
    producer.close();

    var received = new AtomicInteger(0);
    var consumer =
        hStreamClient
            .newConsumer()
            .subscription(subscriptionName)
            .ackBufferSize(100)
            .ackAgeLimit(100)
            .rawRecordReceiver(
                (receivedRawRecord, responder) -> {
                  received.incrementAndGet();
                  responder.ack();
                })
            .build();
    consumer.startAsync().awaitRunning();
    Thread.sleep(7000);
    consumer.stopAsync().awaitTerminated();
    Assertions.assertEquals(count, received.get());
  }
}
