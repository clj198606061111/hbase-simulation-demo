package com.itclj.hbase.utils.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class ReconnectWatcher implements Watcher {

    private Logger logger = LoggerFactory.getLogger(ReconnectWatcher.class);

    private ExpiredCallback expiredCallback;

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() == Watcher.Event.KeeperState.Disconnected) {
            logger.error("ZooKeeper disconnected at " + new Date());
        } else if (event.getState() == Event.KeeperState.Expired) {
            logger.error("ZooKeeper session expired at " + new Date());
            if(expiredCallback != null){
                expiredCallback.run();
            }
        } else if (event.getState() == Event.KeeperState.SyncConnected) {
            logger.info("ZooKeeper connected at " + new Date());
        }
    }

    public void setCallback(ExpiredCallback callback){
        this.expiredCallback = callback;
    }

    public interface ExpiredCallback{

        public void run();

    }
}
