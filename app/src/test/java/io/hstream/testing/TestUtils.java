package io.hstream.testing;

import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.hstream.HStreamClient;
import io.hstream.Subscription;
import io.hstream.SubscriptionOffset;
import io.hstream.SubscriptionOffset.SpecialOffset;
import java.nio.file.Path;
import java.util.UUID;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class TestUtils {
  public static String randText() {
    return "test_stream_" + UUID.randomUUID().toString().replace("-", "");
  }

  public static String randStream(HStreamClient c) {
    String streamName = randText();
    c.createStream(streamName, (short) 3);
    return streamName;
  }

  public static String randSubscription(HStreamClient c, String streamName) {
    String subscriptionName = "test_subscription_" + randText();
    Subscription subscription =
        Subscription.newBuilder().subscription(subscriptionName).stream(streamName)
            .offset(new SubscriptionOffset(SpecialOffset.LATEST))
            .ackTimeoutSeconds(10)
            .build();
    c.createSubscription(subscription);
    return subscriptionName;
  }

  // -----------------------------------------------------------------------------------------------

  public static GenericContainer<?> makeZooKeeper(Network network) {
    return new GenericContainer(DockerImageName.parse("zookeeper")).withNetwork(network);
  }

  public static GenericContainer<?> makeHStore(Network network, Path dataDir) {
    return new GenericContainer(DockerImageName.parse("hstreamdb/hstream:latest"))
        .withNetwork(network)
        .withFileSystemBind(
            dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_WRITE)
        .withCommand(
            "bash",
            "-c",
            "ld-dev-cluster "
                + "--root /data/hstore "
                + "--use-tcp "
                + "--tcp-host "
                + "$(hostname -I | cut -f1 -d' ') "
                + "--user-admin-port 6440 "
                + "--no-interactive")
        .waitingFor(Wait.forLogMessage(".*LogDevice Cluster running.*", 1));
  }

  public static GenericContainer<?> makeHServer(
      Network network, Path dataDir, String zkHost, String hstoreHost, int serverId) {
    return makeHServerWith(
        "0.0.0.0", 6570, 65000 + serverId, network, dataDir, zkHost, hstoreHost, serverId);
  }

  public static GenericContainer<?> makeHServerWith(
      String addr,
      int port,
      int internalPort,
      Network network,
      Path dataDir,
      String zkHost,
      String hstoreHost,
      int serverId) {
    return new GenericContainer(DockerImageName.parse("hstreamdb/hstream:v0.6.0"))
        .withNetwork(network)
        .withFileSystemBind(dataDir.toAbsolutePath().toString(), "/data/hstore", BindMode.READ_ONLY)
        .withCreateContainerCmdModifier(
            createContainerCmd -> {
              CreateContainerCmd cmd = (CreateContainerCmd) createContainerCmd;
              var exposedPort = new ExposedPort(port + serverId);
              cmd.withExposedPorts(exposedPort);
              cmd.getHostConfig()
                  .withPortBindings(
                      new PortBinding(Ports.Binding.bindPort(port + serverId), exposedPort));
            })
        .withCommand(
            "bash",
            "-c",
            " hstream-server"
                + " --host "
                + "$(hostname -I | cut -f1 -d' ')"
                + " --port "
                + String.valueOf(port + serverId)
                + " --internal-port "
                + String.valueOf(internalPort)
                + " --address "
                + addr
                // + "$(hostname -I | cut -f1 -d' ')"
                + " --server-id "
                + String.valueOf(serverId)
                + " --zkuri "
                + zkHost
                + ":2181"
                + " --store-config "
                + "/data/hstore/logdevice.conf "
                + " --store-admin-host "
                + hstoreHost
                + " --store-admin-port "
                + "6440")
        .waitingFor(Wait.forLogMessage(".*Server started on port.*", 1));
  }
}
