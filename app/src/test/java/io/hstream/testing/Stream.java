package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Stream {
  private static final Logger logger = LoggerFactory.getLogger(Stream.class);
  private HStreamClient client;
  private List<String> hServerUrls;
  private final Random globalRandom = new Random();

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  public void setHServerUrls(List<String> hServerUrls) {
    this.hServerUrls = hServerUrls;
  }

  @Test
  @Timeout(60)
  void testCreateStream() {
    final String streamName = randText();
    client.createStream(streamName);
    List<io.hstream.Stream> streams = client.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
  }

  @Test
  @Timeout(60)
  void testListStreams() {
    Assertions.assertTrue(client.listStreams().isEmpty());
    var streamNames = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      streamNames.add(randStream(client));
    }
    var res =
        client.listStreams().parallelStream()
            .map(io.hstream.Stream::getStreamName)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(streamNames.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(60)
  void testDeleteStream() {
    final String streamName = randStream(client);
    List<io.hstream.Stream> streams = client.listStreams();
    Assertions.assertEquals(1, streams.size());
    Assertions.assertEquals(streamName, streams.get(0).getStreamName());
    client.deleteStream(streamName);
    streams = client.listStreams();
    Assertions.assertEquals(0, streams.size());
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistStreamShouldFail() throws Exception {
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          try {
            client.deleteStream(randText());
          } catch (Throwable e) {
            logger.info("============= error\n{}", e.toString());
            throw e;
          }
        });
  }

  // TODO: serviceUrl
  @Test
  @Timeout(60)
  void testMultiThreadListStream() throws Exception {
    randStream(client);

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
  @Disabled("HS-1297")
  @Test
  @Timeout(60)
  void testMultiThreadDeleteSameStream() throws Exception {
    ArrayList<Exception> exceptions = new ArrayList<>();

    String stream = randStream(client);

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
}
