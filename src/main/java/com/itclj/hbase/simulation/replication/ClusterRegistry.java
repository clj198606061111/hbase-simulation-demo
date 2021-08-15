package com.itclj.hbase.simulation.replication;

import com.itclj.hbase.common.Constants;
import com.itclj.hbase.utils.zookeeper.ZkUtil;
import com.itclj.hbase.utils.zookeeper.ZooKeeperItf;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.UUID;

/**
 * regionServer 模拟集群注册器
 */
@Component
public class ClusterRegistry {

    private static Logger log = LoggerFactory.getLogger(ClusterRegistry.class);

    @Resource
    private ZooKeeperItf zk;

    @Value("${spring.zookeeper.simulation.rootNode}")
    private String baseZkPath;

    @Value("${spring.hbase.simulation.clusterName}")
    private String simulatorClusterName ;

    // Replace '-' with unicode "CANADIAN SYLLABICS HYPHEN" character in zookeeper to avoid issues
    // with HBase replication naming conventions
    public static final char INTERNAL_HYPHEN_REPLACEMENT = '\u1400';

    private final static String HBASE_ID = "/hbaseid";

    private final static String RS = "/rs";

    public void register() {
        try{
            if (!this.hasSimulatorCluster(simulatorClusterName)) {
                addSimulatorCluster(simulatorClusterName);
            }
        }catch (Exception e){
            log.error("register clusterName "+simulatorClusterName+" error!",e);
        }

    }

    public boolean addSimulatorCluster(String name) throws InterruptedException, KeeperException {
        String internalName = toInternalSimulatorClusterName(name);
        String basePath = baseZkPath + Constants.PATH_SEPARATOR + internalName;
        UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(internalName)); // always gives the same uuid for the same name
        String hbaseid = basePath + HBASE_ID;
        Stat hbaseidStat = zk.exists(hbaseid, false);
        if (hbaseidStat == null) {
            ZkUtil.createPath(zk, hbaseid, Bytes.toBytes(uuid.toString()));
        }

        String rs = basePath + RS;
        Stat rsStat = zk.exists(rs, false);
        if (rsStat == null) {
            ZkUtil.createPath(zk, rs);
        }
        return true;
    }

    public boolean removeSimulatorCluster(String name) {
        String internalName = toInternalSimulatorClusterName(name);
        String basePath = baseZkPath + Constants.PATH_SEPARATOR + internalName;
        try {
            ZkUtil.deleteNode(zk, basePath + HBASE_ID);
            for (String child : zk.getChildren(basePath + RS, false)) {
                ZkUtil.deleteNode(zk, basePath + RS + Constants.PATH_SEPARATOR + child);
            }
            ZkUtil.deleteNode(zk, basePath + RS);
            ZkUtil.deleteNode(zk, basePath);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        } catch (KeeperException ke) {
            log.error("Cleanup in zookeeper failed on " + basePath, ke);
        }
        return true;
    }

    public boolean hasSimulatorCluster(String name) throws Exception {
        String internalName = toInternalSimulatorClusterName(name);
        String basePath = baseZkPath + Constants.PATH_SEPARATOR + internalName;
        String hbaseid = basePath + HBASE_ID;
        String rs = basePath + RS;
        Stat hbaseidStat = zk.exists(hbaseid, false);
        Stat rsStat = zk.exists(rs, false);
        if (hbaseidStat == null || rsStat == null) {
            return false;
        }
        return true;
    }


    private String toInternalSimulatorClusterName(String simulatorClusterName) {
        if (simulatorClusterName.indexOf(INTERNAL_HYPHEN_REPLACEMENT, 0) != -1) {
            throw new IllegalArgumentException("Subscription name cannot contain character \\U1400");
        }
        return simulatorClusterName.replace('-', INTERNAL_HYPHEN_REPLACEMENT);
    }

}
