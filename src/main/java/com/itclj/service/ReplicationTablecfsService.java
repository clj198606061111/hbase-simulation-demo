package com.itclj.service;

public interface ReplicationTablecfsService {
    /**
     * 获取所有全量表的表名
     * @return
     */
    boolean isTableTypeAll(String tableName);
}
