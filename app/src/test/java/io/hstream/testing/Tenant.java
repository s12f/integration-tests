package io.hstream.testing;

import io.hstream.HStreamClient;
import io.hstream.Subscription;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/* TLS cases
Tag("tls"): enable tls in servers and client
Tag("tls-authentication"): enable tls authentication in servers and client
 */
@Disabled
@Slf4j
@Tag("basicTest")
@ExtendWith(ClusterExtension.class)
public class Tenant {
  HStreamClient client;
  String hStreamDBUrl;

  public void setClient(HStreamClient client) {
    this.client = client;
  }

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  HStreamClient makeClient2() {
    var securityPath = getClass().getResource("/security").getPath();
    return HStreamClient.builder()
        .serviceUrl(hStreamDBUrl)
        .enableTls()
        .tlsCaPath(securityPath + "/root_ca.crt")
        .enableTlsAuthentication()
        .tlsKeyPath(securityPath + "/ee10d1ca-cfdd-4fa2-a66d-6ef785a5d29c-pk8.key")
        .tlsCertPath(securityPath + "/ee10d1ca-cfdd-4fa2-a66d-6ef785a5d29c.crt")
        .build();
  }

  @Test
  @Timeout(20)
  @Tag("tls")
  @Tag("tls-authentication")
  void testStream() {
    var client2 = makeClient2();
    client.createStream("stream01");
    client2.createStream("stream01");
    client2.createStream("stream02");
    var streams1 = client.listStreams();
    Assertions.assertEquals(1, streams1.size());
    Assertions.assertEquals("stream01", streams1.get(0).getStreamName());

    var streams2 = client.listStreams();
    Assertions.assertEquals(2, streams2.size());

    client.deleteStream("stream01");
    client2.deleteStream("stream01");

    // client should not be able to delete stream02 belonging to client02
    Assertions.assertThrows(Exception.class, () -> client.deleteStream("stream02"));
    client2.deleteStream("stream02");
  }

  @Test
  @Timeout(20)
  @Tag("tls")
  @Tag("tls-authentication")
  void testSubscription() {
    var client2 = makeClient2();
    client.createStream("stream01");
    client2.createStream("stream01");
    var sub01 = Subscription.newBuilder().stream("stream01").subscription("sub01").build();
    var sub02 = Subscription.newBuilder().stream("stream01").subscription("sub02").build();

    // create
    client.createSubscription(sub01);
    client2.createSubscription(sub01);
    client2.createSubscription(sub02);

    // list
    Assertions.assertEquals(1, client.listStreams().size());
    Assertions.assertEquals(2, client2.listStreams().size());

    // delete
    client.deleteSubscription(sub01.getSubscriptionId());
    Assertions.assertThrows(
        Exception.class, () -> client.deleteSubscription(sub02.getSubscriptionId()));
    client2.deleteSubscription(sub02.getSubscriptionId());
  }

  @Test
  @Timeout(20)
  @Tag("tls")
  @Tag("tls-authentication")
  void testDiffConn() {
    // same tenant, different connection
    var client2 = makeClient2();
    var client3 = makeClient2();
    client2.createStream("stream01");
    Assertions.assertEquals(1, client3.listStreams().size());
    client3.deleteStream("stream01");
  }
}
