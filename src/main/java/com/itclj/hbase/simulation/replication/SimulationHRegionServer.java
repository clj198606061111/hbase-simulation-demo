package com.itclj.hbase.simulation.replication;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.itclj.hbase.common.Constants;
import com.itclj.hbase.common.SystemUtils;
import com.itclj.hbase.simulation.celltarget.ReplicationTarget;
import com.itclj.hbase.utils.SpringContextUtils;
import com.itclj.hbase.utils.io.Closer;
import com.itclj.hbase.utils.zookeeper.ReconnectWatcher;
import com.itclj.hbase.utils.zookeeper.ZooKeeperItf;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
public class SimulationHRegionServer extends BaseHRegionServer  {

    private Logger log = LoggerFactory.getLogger(SimulationHRegionServer.class);

    @Value("${spring.hbase.simulation.clusterName}")
    private String subscriptionId;

    @Value("${spring.zookeeper.simulation.rootNode}")
    private String zkRootNode;

    @Resource
    private ZooKeeperItf zk;

    @Resource
    private Configuration hbaseServerConf;

    private RpcServer rpcServer;
    private ServerName serverName;
    private ZooKeeperWatcher zkWatcher;
    private String zkNodePath;
    private AtomicBoolean running = new AtomicBoolean(false);

    @PostConstruct
    public void init() throws IOException {
        String hostName = SystemUtils.getHostName();
        InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
        if (initialIsa.getAddress() == null) {
            throw new IllegalArgumentException("Failed resolve of " + initialIsa);
        }
        String name = "regionserver/" + initialIsa.toString();
        this.rpcServer = new RpcServer(this, name, getServices(),
                initialIsa, // BindAddress is IP we got for this server.
                hbaseServerConf,
                new FifoRpcScheduler(hbaseServerConf, hbaseServerConf.getInt("hbase.regionserver.handler.count",10)));

        this.serverName = ServerName.valueOf(hostName, rpcServer.getListenerAddress().getPort(), System.currentTimeMillis());
        this.zkWatcher = new ZooKeeperWatcher(hbaseServerConf, this.serverName.toString(), null);

        // login the zookeeper client principal (if using security)
        ZKUtil.loginClient(hbaseServerConf, "hbase.zookeeper.client.keytab.file",
                "hbase.zookeeper.client.kerberos.principal", hostName);

        // login the server principal (if using secure Hadoop)
        User.login(hbaseServerConf, "hbase.regionserver.keytab.file",
                "hbase.regionserver.kerberos.principal", hostName);
    }

    public void start() throws IOException, InterruptedException, KeeperException {
        rpcServer.start();
        // Publish our existence in ZooKeeper
        createRsNode();
        running.set(true);
        initZkReconnect();
    }

    public void createRsNode() throws KeeperException, InterruptedException {
        zkNodePath = zkRootNode + Constants.PATH_SEPARATOR + subscriptionId + "/rs/" + serverName.getServerName();
        if(zk.exists(zkNodePath, false) == null){
            zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }else{
            zk.delete(zkNodePath, -1);
            zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

    }

    private void initZkReconnect(){
        ReconnectWatcher watcher = zk.getReconnectWatcher();
        if(watcher != null){
            watcher.setCallback(new ReconnectWatcher.ExpiredCallback() {
                @Override
                public void run() {
                    zk.reconnectUntilSuccess();
                    log.warn(" end reconnect zk at " + new Date());
                    try {
                        createRsNode();
                    } catch (Exception e) {
                        log.error("reconnect success but create rs node fail",e);
                    }
                }
            });
        }
    }

    private List<RpcServer.BlockingServiceAndInterface> getServices() {
        List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<RpcServer.BlockingServiceAndInterface>(1);
        bssi.add(new RpcServer.BlockingServiceAndInterface(
                AdminProtos.AdminService.newReflectiveBlockingService(this),
                AdminProtos.AdminService.BlockingInterface.class));
        return bssi;
    }

    public void stop() {
        Closer.close(zkWatcher);
        if (running.get()) {
            running.set(true);
            Closer.close(rpcServer);
            try {
                // This ZK node will likely already be gone if the index has been removed
                // from ZK, but we'll try to remove it here to be sure
                zk.delete(zkNodePath, -1);
            } catch (Exception e) {
                log.debug("Exception while removing zookeeper node", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
                                                                   final AdminProtos.ReplicateWALEntryRequest request) throws ServiceException {
        try {
            //从容器中获取 保证每次创建实例
            ReplicationTarget target = (ReplicationTarget) SpringContextUtils.getBean("hbaseTarget");
            List<AdminProtos.WALEntry> entryList = request.getEntryList();
            CellScanner cells = ((HBaseRpcController) controller).cellScanner();
            for (AdminProtos.WALEntry entry : entryList) {
                WALProtos.WALKey key = entry.getKey();
                String tableName = key.getTableName().toStringUtf8();
                int count = entry.getAssociatedCellCount();
                for (int i = 0; i < count; i++) {
                    if (!cells.advance()) {
                        log.warn("Expected=" + count + ", index=" + i);
                        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
                    }
                    if (tableName == null || tableName.isEmpty()) {
                        continue;
                    }
                    Cell cell = cells.current();
                    target.addCell(cell,tableName);
                }
            }
            target.flush();
            return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
        } catch (Throwable ie) {
            throw new ServiceException(ie);
        }
    }

}
