/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class QueryEventDataDeserializer implements EventDataDeserializer<QueryEventData> {

    // status variables, as defined by Query_log_event of MySQL and MariaDB
    private static final int Q_FLAGS2_CODE = 0;
    private static final int Q_SQL_MODE_CODE = 1;
    private static final int Q_CATALOG_CODE = 2;
    private static final int Q_AUTO_INCREMENT = 3;
    private static final int Q_CHARSET_CODE = 4;
    private static final int Q_TIME_ZONE_CODE = 5;
    private static final int Q_CATALOG_NZ_CODE = 6;
    private static final int Q_LC_TIME_NAMES_CODE = 7;
    private static final int Q_CHARSET_DATABASE_CODE = 8;
    private static final int Q_TABLE_MAP_FOR_UPDATE_CODE = 9;
    private static final int Q_MASTER_DATA_WRITTEN_CODE = 10;
    // MariaDB 11.2 and later, written before the time zone when the statement resolves the collation of a charset
    private static final int Q_CHARACTER_SET_COLLATIONS = 131;

    @Override
    public QueryEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        QueryEventData eventData = new QueryEventData();
        eventData.setThreadId(inputStream.readLong(4));
        eventData.setExecutionTime(inputStream.readLong(4));
        inputStream.skip(1); // length of the name of the database
        eventData.setErrorCode(inputStream.readInteger(2));
        eventData.setTimeZone(readTimeZone(inputStream.read(inputStream.readInteger(2)))); // status variables block
        eventData.setDatabase(inputStream.readZeroTerminatedString());
        eventData.setSql(inputStream.readString(inputStream.available()));
        return eventData;
    }

    /**
     * Reads the time zone from the status variables. The server writes the variables that can precede the time zone
     * first, and they are skipped. An unknown variable ends the search, as the length of its value is not known.
     *
     * @return the time zone, or null if the status variables do not contain it
     */
    // visible for testing
    static String readTimeZone(byte[] statusVariables) {
        int position = 0;
        while (position < statusVariables.length) {
            int code = statusVariables[position++] & 0xFF;
            switch (code) {
                case Q_FLAGS2_CODE:
                case Q_AUTO_INCREMENT:
                case Q_MASTER_DATA_WRITTEN_CODE:
                    position += 4;
                    break;
                case Q_SQL_MODE_CODE:
                case Q_TABLE_MAP_FOR_UPDATE_CODE:
                    position += 8;
                    break;
                case Q_CHARSET_CODE:
                    position += 6;
                    break;
                case Q_LC_TIME_NAMES_CODE:
                case Q_CHARSET_DATABASE_CODE:
                    position += 2;
                    break;
                case Q_CHARACTER_SET_COLLATIONS:
                    // a count of pairs of a charset and a collation, two bytes each
                    if (position >= statusVariables.length) {
                        return null;
                    }
                    position += 1 + (statusVariables[position] & 0xFF) * 4;
                    break;
                case Q_CATALOG_CODE:
                case Q_CATALOG_NZ_CODE:
                case Q_TIME_ZONE_CODE:
                    // a length and a name, followed by a terminating zero for the old catalog variable
                    if (position >= statusVariables.length) {
                        return null;
                    }
                    int length = statusVariables[position++] & 0xFF;
                    if (code == Q_TIME_ZONE_CODE) {
                        return position + length <= statusVariables.length ?
                            new String(statusVariables, position, length, StandardCharsets.US_ASCII) : null;
                    }
                    position += length + (code == Q_CATALOG_CODE ? 1 : 0);
                    break;
                default:
                    return null;
            }
        }
        return null;
    }
}
