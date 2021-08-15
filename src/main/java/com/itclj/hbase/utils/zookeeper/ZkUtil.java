package com.itclj.hbase.utils.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZkUtil {
    public static ZooKeeperItf connect(String connectString, int sessionTimeout, Watcher watcher) throws ZkConnectException {
        ZooKeeperImpl zooKeeper;
        try {
            zooKeeper = new ZooKeeperImpl(connectString, sessionTimeout, watcher);
        } catch (IOException e) {
            throw new ZkConnectException("Failed to connect with Zookeeper @ '" + connectString + "'", e);
        }
        long waitUntil = System.currentTimeMillis() + sessionTimeout;
        boolean connected = (ZooKeeper.States.CONNECTED).equals(zooKeeper.getState());
        while (!connected && waitUntil > System.currentTimeMillis()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                connected = (ZooKeeper.States.CONNECTED).equals(zooKeeper.getState());
                break;
            }
            connected = (ZooKeeper.States.CONNECTED).equals(zooKeeper.getState());
        }
        if (!connected) {
            Thread.dumpStack();
            zooKeeper.close();
            throw new ZkConnectException("Failed to connect with Zookeeper @ '" + connectString +
                    "' within timeout " + sessionTimeout);
        }
        return zooKeeper;
    }

    public static void createPath(final ZooKeeperItf zk, final String path)
            throws InterruptedException, KeeperException {
        createPath(zk, path, null);
    }

    /**
     * Creates a persistent path on zookeeper if it does not exist yet, including any parents.
     * Keeps retrying in case of connection loss.
     *
     * <p>The supplied data is used for the last node in the path. If the path already exists,
     * the data is updated if necessary.
     */
    public static void createPath(final ZooKeeperItf zk, final String path, final byte[] data)
            throws InterruptedException, KeeperException {

        if (!path.startsWith("/")){
            throw new IllegalArgumentException("Path should start with a slash.");
        }

        if (path.endsWith("/")){
            throw new IllegalArgumentException("Path should not end on a slash.");
        }

        String[] parts = path.substring(1).split("/");

        final StringBuilder subPath = new StringBuilder();
        boolean created = false;
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            subPath.append("/").append(part);

            // Only use the supplied data for the last node in the path
            final byte[] newData = (i == parts.length - 1 ? data : null);

            created = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                @Override
                public Boolean execute() throws KeeperException, InterruptedException {
                    if (zk.exists(subPath.toString(), false) == null) {
                        try {
                            zk.create(subPath.toString(), newData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                            return true;
                        } catch (KeeperException.NodeExistsException e) {
                            return false;
                        }
                    }
                    return false;
                }
            });
        }
        if (!created) {
            // The node already existed, update its data if necessary
            zk.retryOperation(new ZooKeeperOperation<Boolean>() {
                @Override
                public Boolean execute() throws KeeperException, InterruptedException {
                    byte[] currentData = zk.getData(path, false, new Stat());
                    if (!Arrays.equals(currentData, data)) {
                        zk.setData(path, data, -1);
                    }
                    return true;
                }
            });
        }
    }

    /**
     * Updates data on a zookeeper node.
     *
     * <p>
     * The supplied data is used for the last node in the path. The path must
     * already exist. It is not checked if the data is changed or not. This will
     * cause the version of the node to be increased.
     * <p>
     * This operation is retried until it succeeds.
     */
    public static void update(final ZooKeeperItf zk, final String path, final byte[] data, final int version)
            throws InterruptedException, KeeperException {
        zk.retryOperation(new ZooKeeperOperation<Boolean>() {
            @Override
            public Boolean execute() throws KeeperException, InterruptedException {
                zk.setData(path, data, version);
                return true;
            }
        });
    }


    /**
     * Deletes a path (non-recursively) in ZooKeeper, if it exists.
     * <p>
     * If the path doesn't exist, the delete will fail silently. The delete operation is retried until it succeeds, or
     * until it fails with a non-recoverable error.
     * <p>
     * If the path has children, the operation will fail with the underlying {link NotEmptyException}.
     *
     * @param zk Handle to the ZooKeeper where the delete will occur
     * @param path The path to be deleted
     */
    public static void deleteNode(final ZooKeeperItf zk, final String path) throws InterruptedException,
            KeeperException {
        zk.retryOperation(new ZooKeeperOperation<Boolean>() {

            @Override
            public Boolean execute() throws KeeperException, InterruptedException {
                Stat stat = zk.exists(path, false);
                if (stat != null) {
                    try {
                        zk.delete(path, stat.getVersion());
                    } catch (KeeperException.NoNodeException nne) {
                        // This is ok, the node is already gone
                    }
                    // We don't catch BadVersion or NotEmpty as these are probably signs that there is something
                    // unexpected going on with the node that is to be deleted
                }
                return true;
            }
        });
    }

    /**
     * Gets data from a zookeeper node.
     * <p>
     * This operation is retried until it succeeds.
     */
    public static byte[] getData(final ZooKeeperItf zk, final String path, final Watcher watcher, final Stat stat)
            throws InterruptedException, KeeperException {
        final List<byte[]> data = new ArrayList<byte[]>(1);
        zk.retryOperation(new ZooKeeperOperation<Boolean>() {
            @Override
            public Boolean execute() throws KeeperException, InterruptedException {
                data.add(zk.getData(path, watcher, stat));
                return true;
            }
        });
        return data.get(0);
    }
}
