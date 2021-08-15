package com.itclj.hbase.simulation.celltarget;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.itclj.service.ReplicationTablecfsService;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class HbaseTarget implements ReplicationTarget{
    private Logger logger = LoggerFactory.getLogger(HbaseTarget.class);

    @Resource(name = "targetConnection")
    public Connection targetConnection;

    @Resource(name = "sourceConnection")
    public Connection sourceConnection;

    @Resource
    private ReplicationTablecfsService tablecfsService;

    Map<String, Map<String, Put>> putMap = Maps.newHashMap();

    Map<String, String> sourceTbNameMap = Maps.newHashMap();
    Map<String, Long> sourceTbTimestampMap = Maps.newHashMap();

    @Override
    public void addCell(Cell cell, String tableName) throws Exception {
        String targetTableName = getTableNameByType(tableName, cell.getTimestamp());
        sourceTbNameMap.put(targetTableName,tableName);
        Map<String,Put> rowKeyPut = putMap.get(targetTableName);
        if(rowKeyPut == null){
            rowKeyPut = Maps.newHashMap();
            putMap.put(targetTableName,rowKeyPut);
        }
        String rowKey = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
        Put put = rowKeyPut.get((rowKey));
        if(put == null){
            put = new Put(rowKey.getBytes());
            rowKeyPut.put(rowKey,put);
        }
        put.add(cell);
    }

    @Override
    public void flush() throws Exception {
        long btime = System.currentTimeMillis();
        long total = 0;
        Throwable error = null;
        List putList = null;
        for(Map.Entry<String,Map<String,Put>> entry : putMap.entrySet()){
            String tableName = entry.getKey();
            Table table = targetConnection.getTable(TableName.valueOf(tableName));
            try{
                putList = Lists.newArrayList(entry.getValue().values());
                total += putList.size();
                table.put(putList);
            }catch (RetriesExhaustedWithDetailsException e){
                for(Throwable t : e.getCauses()){
                    if(t instanceof TableNotFoundException){
                        createTableFromSource(tableName);
                        table.put(Lists.newArrayList(entry.getValue().values()));
                        break;
                    }else{
                        error = t;
                    }
                }
            }
        }
        if(error != null){
            logger.error("put error",error);
            throw new Exception(error);
        }
        logger.info("flush {} take time {} ms",total, System.currentTimeMillis()-btime);
    }

    /**
     * 从来源hbase集群获取表描述 在目标集群建表
     * @param targetTableName
     */
    private void createTableFromSource(String targetTableName) {
        TableName sourceTB = null;
        Admin targetAdmin = null;
        HTableDescriptor descriptor = null;
        byte[][] splitKeys = null;
        String sourceName = sourceTbNameMap.get(targetTableName);
        sourceTB = TableName.valueOf(sourceName);
        try{
            Admin admin = sourceConnection.getAdmin();
            descriptor = admin.getTableDescriptor(sourceTB);
            descriptor.setName(targetTableName.getBytes());
            //splitKeys = HbaseUtils.getTableSplitRowKeys(sourceConnection,sourceTB);
            targetAdmin = targetConnection.getAdmin();
            targetAdmin.createTable(descriptor);
            logger.info("create table {}",targetTableName);
        }catch (NamespaceNotFoundException nne){
            try{
                if(targetAdmin != null){
                    targetAdmin.createNamespace(NamespaceDescriptor.create(sourceTB.getNamespaceAsString()).build());
                    targetAdmin.createTable(descriptor,splitKeys);
                    logger.info("create table {}",targetTableName);
                }
            }catch (Exception e2){
                logger.warn( String.format("create namespace and table %s error",targetTableName),e2);
            }
        }catch (Exception e){
            //多线程情况下会存在同时建表
            logger.warn("create table {} error by {}",targetTableName,e.getMessage());
        }
    }

    private String getTableNameByType(String tableName,long timestamp){
        if(tablecfsService.isTableTypeAll(tableName)){
            return tableName;
        }
        return tableName + "_" + DateFormatUtils.format(timestamp,"yyyyMMdd");
    }

}
