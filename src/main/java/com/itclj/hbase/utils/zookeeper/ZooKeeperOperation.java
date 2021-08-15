package com.itclj.hbase.utils.zookeeper;

import org.apache.zookeeper.KeeperException;

public interface ZooKeeperOperation<T> {
    /**
     * Performs the operation - which may be involved multiple times if the connection
     * to ZooKeeper closes during this operation
     *
     */
    public T execute() throws KeeperException, InterruptedException;
}
