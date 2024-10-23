package com.github.shyiko.mysql.binlog;

import org.testng.SkipException;
import org.testng.annotations.Test;

/**
 * @author vjuranek
 */
public class BinaryLogClientBinlogCompressIntegrationTest extends BinaryLogClientGTIDIntegrationTest {
    @Override
    protected MysqlOnetimeServerOptions getOptions() {
        if ( !this.mysqlVersion.atLeast(8,0) )  {
            throw new SkipException("skipping binlog compression on 5.x");
        }

        MysqlOnetimeServerOptions options = new MysqlOnetimeServerOptions();
        options.gtid = true;
        options.extraParams = "--binlog_transaction_compression=ON";
        return options;
    }

    // These tests fail when binlog compression is enabled because the columns of these types are
    // serialized in a plain text representation when the records are uncompressed.

    @Test
    @Override
    public void testDeserializationOfDATE() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testDeserializationOfDATETIME() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testDeserializationOfDateAndTimeAsLong() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testDeserializationOfDateAndTimeAsLongMicrosecondsPrecision() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testDeserializationOfSTRING() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testDeserializationOfTIME() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testDeserializationOfTIMESTAMP() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testDeserializationOfVARSTRING() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testFSP() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    @Test
    @Override
    public void testWriteUpdateDeleteEvents() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    // This test tests custom deserializer which and assumes ordinary binlog,
    // it can be skipped in this testsuite.
    @Test
    @Override
    public void testCustomEventDataDeserializers() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }

    // This test tests switch to specific position in events and checks number of WriteRowsEventData events.
    // In case of compressed binlog we are not able to switch in the middle of the TX.
    @Test
    @Override
    public void testBinlogPositionPointsToTableMapEventUntilTheEndOfLogicalGroup() throws Exception {
        throw new SkipException("Skipped due to binlog compression");
    }
}
