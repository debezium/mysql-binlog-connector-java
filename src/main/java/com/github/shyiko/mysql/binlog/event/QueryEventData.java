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
package com.github.shyiko.mysql.binlog.event;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class QueryEventData implements EventData {

    private long threadId;
    private long executionTime;
    private int errorCode;
    private String database;
    private String sql;
    private String timeZone;

    public long getThreadId() {
        return threadId;
    }

    public void setThreadId(long threadId) {
        this.threadId = threadId;
    }

    public long getExecutionTime() {
        return executionTime;
    }

    public void setExecutionTime(long executionTime) {
        this.executionTime = executionTime;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     * @return the time zone of the session that ran the statement, as recorded by the server, such as {@code SYSTEM},
     * {@code +09:00} or {@code Asia/Seoul}. The server records it when the statement depends on it, for example when
     * it contains a {@code TIMESTAMP} literal. Null if the event does not contain it, or if it follows a status
     * variable that is not known, whose length cannot be determined; null does not mean UTC.
     */
    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("QueryEventData");
        sb.append("{threadId=").append(threadId);
        sb.append(", executionTime=").append(executionTime);
        sb.append(", errorCode=").append(errorCode);
        sb.append(", database='").append(database).append('\'');
        sb.append(", sql='").append(sql).append('\'');
        if (timeZone != null) {
            sb.append(", timeZone='").append(timeZone).append('\'');
        }
        sb.append('}');
        return sb.toString();
    }
}
