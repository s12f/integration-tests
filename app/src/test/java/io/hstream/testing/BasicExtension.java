package io.hstream.testing;

import static io.hstream.testing.TestUtils.makeHServer;
import static io.hstream.testing.TestUtils.makeHStore;
import static io.hstream.testing.TestUtils.makeZooKeeper;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

public class BasicExtension implements BeforeEachCallback, AfterEachCallback {

  private Path dataDir;
  private GenericContainer<?> zk;
  private GenericContainer<?> hstore;
  private GenericContainer<?> hserver;
  private Network.NetworkImpl network;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    dataDir = Files.createTempDirectory("hstream");
    network = Network.builder().build();

    zk = makeZooKeeper(network);
    zk.start();
    String zkHost =
        zk.getContainerInfo()
            .getNetworkSettings()
            .getNetworks()
            .get(network.getName())
            .getIpAddress();
    System.out.println("[DEBUG]: zkHost: " + zkHost);

    hstore = makeHStore(network, dataDir);
    hstore.start();
    String hstoreHost =
        hstore
            .getContainerInfo()
            .getNetworkSettings()
            .getNetworks()
            .get(network.getName())
            .getIpAddress();
    System.out.println("[DEBUG]: hstoreHost: " + hstoreHost);

    hserver = makeHServer(network, dataDir, zkHost, hstoreHost, 0);
    hserver.start();
    Thread.sleep(100);
    Object testInstance = context.getRequiredTestInstance();
    testInstance
        .getClass()
        .getMethod("setHStreamDBUrl", String.class)
        .invoke(testInstance, "127.0.0.1:" + 6570);
  }

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    hserver.close();
    hstore.close();
    zk.close();
    network.close();

    hserver = null;
    hstore = null;
    zk = null;
    network = null;
    dataDir = null;
  }
}
