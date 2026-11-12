package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class QueryEventDataDeserializerTest {

    private final QueryEventDataDeserializer deserializer = new QueryEventDataDeserializer();

    @Test
    public void testDeserializeWithTimeZone() throws IOException {
        // in the order in which the server writes them: flags, sql_mode, catalog, auto_increment, charset, time zone,
        // and a later variable
        byte[] statusVariables = concat(
            bytes(0, 0, 0, 0, 0),
            bytes(1, 0, 0, 0, 0, 0, 0, 0, 0),
            variable(6, "std"),
            bytes(3, 1, 0, 1, 0),
            bytes(4, 33, 0, 33, 0, 255, 0),
            variable(5, "+09:00"),
            bytes(17, 1, 2, 3, 4, 5, 6, 7, 8));

        QueryEventData data = deserializer.deserialize(event(statusVariables, "test",
            "create table t (ts timestamp default '2026-09-28 12:34:56')"));

        assertEquals(data.getThreadId(), 7);
        assertEquals(data.getErrorCode(), 0);
        assertEquals(data.getDatabase(), "test");
        assertEquals(data.getSql(), "create table t (ts timestamp default '2026-09-28 12:34:56')");
        assertEquals(data.getTimeZone(), "+09:00");
        assertEquals(data.toString(), "QueryEventData{threadId=7, executionTime=0, errorCode=0, database='test', " +
            "sql='create table t (ts timestamp default '2026-09-28 12:34:56')', timeZone='+09:00'}");
    }

    @Test
    public void testDeserializeNonAsciiDatabaseAndSql() throws IOException {
        QueryEventData data = deserializer.deserialize(event(bytes(0, 0, 0, 0, 0), "주문",
            "alter table t comment '주문 테이블'"));

        assertEquals(data.getDatabase(), "주문");
        assertEquals(data.getSql(), "alter table t comment '주문 테이블'");
    }

    @Test
    public void testDeserializeWithTimeZoneAfterCharacterSetCollations() throws IOException {
        // as MariaDB 11.8 writes them for "create table ... default charset = utf8mb4"
        byte[] statusVariables = concat(
            bytes(0, 0, 0, 0, 0),
            bytes(1, 0, 0, 0, 0, 0, 0, 0, 0),
            variable(6, "std"),
            bytes(131, 5, 8, 0, 8, 0, 33, 0, 33, 0, 28, 0, 28, 0, 1, 0, 1, 0, 45, 0, 45, 0),
            bytes(4, 33, 0, 33, 0, 45, 0),
            variable(5, "Asia/Seoul"),
            bytes(129, 1, 0, 0, 0, 0, 0, 0, 0));

        QueryEventData data = deserializer.deserialize(event(statusVariables, "test",
            "create table t (ts timestamp default '2026-09-28 12:34:56') default charset = utf8mb4"));

        assertEquals(data.getTimeZone(), "Asia/Seoul");
        assertEquals(data.getSql(), "create table t (ts timestamp default '2026-09-28 12:34:56') default charset = utf8mb4");
    }

    @Test
    public void testDeserializeWithoutTimeZone() throws IOException {
        QueryEventData data = deserializer.deserialize(event(concat(
            bytes(0, 0, 0, 0, 0),
            bytes(4, 33, 0, 33, 0, 255, 0)), "", "BEGIN"));

        assertNull(data.getTimeZone());
        assertEquals(data.toString(),
            "QueryEventData{threadId=7, executionTime=0, errorCode=0, database='', sql='BEGIN'}");
    }

    @Test
    public void testUnknownStatusVariableEndsTheSearch() throws IOException {
        // the length of an unknown variable is not known, so the time zone after it cannot be found
        QueryEventData data = deserializer.deserialize(event(concat(
            bytes(99, 1, 2, 3),
            variable(5, "SYSTEM")), "test", "drop table t"));

        assertNull(data.getTimeZone());
        assertEquals(data.getDatabase(), "test");
        assertEquals(data.getSql(), "drop table t");
    }

    @Test
    public void testReadTimeZone() {
        // the catalog variable of MySQL 5.0.0 to 5.0.3 is followed by a terminating zero
        assertEquals(QueryEventDataDeserializer.readTimeZone(concat(
            variable(2, "std"), bytes(0),
            variable(5, "SYSTEM"))), "SYSTEM");
        assertEquals(QueryEventDataDeserializer.readTimeZone(concat(
            bytes(7, 0, 0),
            variable(5, "Asia/Seoul"))), "Asia/Seoul");
        // MariaDB's collations with one pair and with none
        assertEquals(QueryEventDataDeserializer.readTimeZone(concat(
            bytes(131, 1, 45, 0, 46, 1),
            variable(5, "+01:00"))), "+01:00");
        assertEquals(QueryEventDataDeserializer.readTimeZone(concat(
            bytes(131, 0),
            variable(5, "-03:30"))), "-03:30");
        assertNull(QueryEventDataDeserializer.readTimeZone(new byte[0]));
        // truncated
        assertNull(QueryEventDataDeserializer.readTimeZone(bytes(5, 6, '+', '0', '9')));
        assertNull(QueryEventDataDeserializer.readTimeZone(bytes(5)));
        assertNull(QueryEventDataDeserializer.readTimeZone(bytes(131)));
        assertNull(QueryEventDataDeserializer.readTimeZone(bytes(131, 9, 8, 0, 8, 0)));
    }

    private static ByteArrayInputStream event(byte[] statusVariables, String database, String sql) {
        byte[] databaseName = database.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        write(output, bytes(7, 0, 0, 0)); // thread id
        write(output, bytes(0, 0, 0, 0)); // execution time
        output.write(databaseName.length);
        write(output, bytes(0, 0)); // error code
        write(output, bytes(statusVariables.length & 0xFF, statusVariables.length >> 8));
        write(output, statusVariables);
        write(output, databaseName);
        output.write(0);
        write(output, sql.getBytes(StandardCharsets.UTF_8));
        return new ByteArrayInputStream(output.toByteArray());
    }

    private static byte[] variable(int code, String value) {
        byte[] text = value.getBytes(StandardCharsets.US_ASCII);
        return concat(bytes(code, text.length), text);
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        for (byte[] part : parts) {
            write(output, part);
        }
        return output.toByteArray();
    }

    private static void write(ByteArrayOutputStream output, byte[] bytes) {
        output.write(bytes, 0, bytes.length);
    }

    private static byte[] bytes(int... values) {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return bytes;
    }
}
