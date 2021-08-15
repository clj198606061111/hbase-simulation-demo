package com.itclj.hbase.simulation.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.security.User;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
public class HbaseConfig {

    @Value("${spring.zookeeper.hbase.source}")
    private String hbaseSourceZk;

    @Value("${spring.zookeeper.hbase.target}")
    private String hbaseTargetZk;

    @Value("${spring.zookeeper.simulation.rootNode}")
    private String zkRootNode;

    @Value("${spring.zookeeper.simulation.quorum}")
    private String zkIp;

    @Value("${spring.zookeeper.simulation.port:2181}")
    private int zkPort;

    @Value("${spring.hbase.simulation.rsHandlerCount}")
    private int rsHandlerCount;

    @Value("${spring.hbase.simulation.clusterName}")
    private String simulatorClusterName ;

    @Value("${spring.hbase.userName:hbaseuis}")
    private String hbaseUser;

    @Value("${spring.hbase.group:hbaseuis}")
    private String hbaseGroup;

    @Bean(value = "sourceConnection")
    public Connection initSourceConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM,hbaseSourceZk);
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,3);
        User user = User.createUserForTesting(conf, hbaseUser, new String[]{hbaseGroup});
        Connection connection = ConnectionFactory.createConnection(conf, user);
        return connection;
    }

    @Bean(value = "targetConnection")
    public Connection initTargetConn() throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set(HConstants.ZOOKEEPER_QUORUM,hbaseTargetZk);
        conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,3);
        User user = User.createUserForTesting(conf, hbaseUser, new String[]{hbaseGroup});
        Connection connection = ConnectionFactory.createConnection(conf, user);
        return connection;
    }


    @Bean(name = "hbaseServerConf")
    public Configuration hbaseServerConf(){
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean("hbase.replication", true);
        conf.set("hbase.zookeeper.quorum", zkIp);
        conf.set("msreplication.zookeeper.node.parent", zkRootNode);
        conf.setInt("hbase.zookeeper.property.clientPort", zkPort);
        conf.setInt("hbase.regionserver.handler.count", rsHandlerCount);
        return conf;
    }

}
