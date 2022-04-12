package io.hstream.testing;

import static io.hstream.testing.TestUtils.randStream;
import static io.hstream.testing.TestUtils.randSubscription;
import static io.hstream.testing.TestUtils.randText;

import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Subscription {
  private static final Logger logger = LoggerFactory.getLogger(Subscription.class);
  private HStreamClient client;

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  @Test
  @Timeout(60)
  void testListSubscriptions() {
    String streamName = randStream(client);
    var subscriptions = new ArrayList<String>();
    for (int i = 0; i < 100; i++) {
      subscriptions.add(randSubscription(client, streamName));
    }
    var res =
        client.listSubscriptions().parallelStream()
            .map(io.hstream.Subscription::getSubscriptionId)
            .sorted()
            .collect(Collectors.toList());
    Assertions.assertEquals(subscriptions.stream().sorted().collect(Collectors.toList()), res);
  }

  @Test
  @Timeout(20)
  void testDeleteSubscription() throws Exception {
    final String stream = randStream(client);
    final String subscription = randSubscription(client, stream);
    Assertions.assertEquals(subscription, client.listSubscriptions().get(0).getSubscriptionId());
    client.deleteSubscription(subscription);
    Assertions.assertEquals(0, client.listSubscriptions().size());
    Thread.sleep(1000);
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnNonExistStreamShouldFail() throws Exception {
    String stream = randText();
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(client, stream);
        });
  }

  @Test
  @Timeout(20)
  void testCreateSubscriptionOnDeletedStreamShouldFail() throws Exception {
    String stream = randStream(client);
    client.deleteStream(stream);
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          String subscription = randSubscription(client, stream);
        });
  }

  @Test
  @Timeout(20)
  void testDeleteNonExistSubscriptionShouldFail() throws Exception {
    Assertions.assertThrows(
        Throwable.class,
        () -> {
          try {
            client.deleteSubscription(randText());
          } catch (Throwable e) {
            logger.info("============= error\n{}", e.toString());
            throw e;
          }
        });
  }
}
