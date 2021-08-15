package com.itclj.hbase.simulation.config;

import com.itclj.hbase.utils.zookeeper.ReconnectWatcher;
import com.itclj.hbase.utils.zookeeper.ZkConnectException;
import com.itclj.hbase.utils.zookeeper.ZkUtil;
import com.itclj.hbase.utils.zookeeper.ZooKeeperItf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ZkConfig {
    private Logger logger = LoggerFactory.getLogger(ZkConfig.class);

    @Value("${spring.zookeeper.simulation.quorum}")
    private String simulationZk;

    @Value("${spring.zookeeper.sessionTimeout:60000}")
    private int zkSessionTimeout;

    @Bean
    public ZooKeeperItf initZk() throws ZkConnectException {
        ReconnectWatcher rw = new ReconnectWatcher();
        ZooKeeperItf zk = ZkUtil.connect(simulationZk, zkSessionTimeout, rw);
        return zk;
    }
}
