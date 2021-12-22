package io.hstream.testing;


import io.hstream.HStreamClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.GenericContainer;

@ExtendWith(ClusterExtension.class)
class BasicClusterTest {

  private String hStreamDBUrl;
  private HStreamClient hStreamClient;
  private GenericContainer<?>[] hservers;

  public void setHStreamDBUrl(String hStreamDBUrl) {
    this.hStreamDBUrl = hStreamDBUrl;
  }

  public void setHServers(GenericContainer<?>[] hservers) {
    this.hservers = hservers;
  }

  private void terminateHServer(int serverId) {
    hservers[serverId].close();
    System.out.println("[DEBUG]: terminate HServer " + String.valueOf(serverId));
  }

  @BeforeEach
  public void setup() throws Exception {
    System.out.println("[DEBUG]: hStreamDBUrl " + hStreamDBUrl);
    hStreamClient = HStreamClient.builder().serviceUrl(hStreamDBUrl).build();
  }

  @AfterEach
  public void teardown() throws Exception {
    hStreamClient.close();
  }

  // -----------------------------------------------------------------------------------------------

  @Test
  void testConnections() throws Exception {

    try (HStreamClient client1 = HStreamClient.builder().serviceUrl("127.0.0.1:6570").build();
        HStreamClient client2 = HStreamClient.builder().serviceUrl("127.0.0.1:6571").build();
        HStreamClient client3 = HStreamClient.builder().serviceUrl("127.0.0.1:6572").build();
        HStreamClient client4 = HStreamClient.builder().serviceUrl("127.0.0.1:6573").build();
        HStreamClient client5 = HStreamClient.builder().serviceUrl("127.0.0.1:6574").build(); ) {}
  }
}
