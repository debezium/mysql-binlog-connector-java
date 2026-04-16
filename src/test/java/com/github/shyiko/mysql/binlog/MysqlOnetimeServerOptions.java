package com.github.shyiko.mysql.binlog;

/**
 * @deprecated Use {@link TestDatabaseContainerOptions} instead. This class is part of the legacy
 * onetimeserver infrastructure and will be removed in a future release.
 */
@Deprecated
public class MysqlOnetimeServerOptions {
    public int serverID = MysqlOnetimeServer.nextServerID++;
    public boolean gtid = false;
    public MysqlOnetimeServer masterServer;
    public String extraParams;
    public boolean fullRowMetaData;
}
