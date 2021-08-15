package com.itclj.hbase.common;

public class Constants {
    public static final String PATH_SEPARATOR = "/";

    public static final String SEMICOLON_SEPARATOR = ";";

    public static final String UNDERLINE_SEPARATOR = "_";

    public static final String CUSTOM_SEPARATOR = "_";

    public static final String BASE_PATH = "/hbaseuismgr";

    public static final String PROJECT_PATH = "/project";

    public static final String SWITCH_PATH = "/run_config";

    public static final String CONFIG_PATH = "/configs";

    public static final String VERIFY_LEADER_PATH = "/others/verify_leader";

    public static final String HBASE_HDFS_PRE = "/hbase/data";
    public static final String HBASE_QUOTA_SPACE = "SPACE_SIZE";

    public static final short LOG_TYPE_CREATE = 0; //新增
    public static final short LOG_TYPE_UPDATE = 1; //修改
    public static final short LOG_TYPE_DELETE = 2; //删除
    public static final short LOG_TYPE_SWITCH = 3; //切换
    public static final short LOG_TYPE_STOP=4; //停止

    public static final int REPLICATION_TABLE_TYPE_DAY = 0 ; //增量
    public static final int REPLICATION_TABLE_TYPE_ALL = 1 ; //全量

    public static final int MYSQL_MAX_STRING_LENGTH = 2000;

    public static final long REPORT_OVERTIME_LIMIT = 30000;

    public static final String CONFIG_NAME_VERIFY_TYPE = "verifyType";
    public static final String CONFIG_NAME_VERIFY_CONFIG = "verifyConfig";

    public static final String CONFIG_GROUP_CLUSTER_DEFAULT = "clusterDefault";

    public static final String CLUSTER_DEFAULT_HBASE_SOURCE = "hbaseSource";
    public static final String CLUSTER_DEFAULT_HBASE_TARGET = "hbaseTarget";

    public static final String CLUSTER_HBASE = "hbase";
    public static final String CLUSTER_HDFS = "hdfs";
    public static final String CLUSTER_YARN = "yarn";

    public static final String HBASE_VERIFY_NAME = "HBASE_VERIFY";

    public static final String VERIFY_PARAM_START_TIME = "startTime";
    public static final String VERIFY_PARAM_END_TIME = "endTime";
}
