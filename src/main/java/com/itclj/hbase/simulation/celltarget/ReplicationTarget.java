package com.itclj.hbase.simulation.celltarget;

import org.apache.hadoop.hbase.Cell;

public interface ReplicationTarget {
    /**
     *
     * @param cell
     */
    public void addCell(Cell cell, String tableName) throws Exception;

    /**
     *
     */
    public void flush() throws Exception;
}
