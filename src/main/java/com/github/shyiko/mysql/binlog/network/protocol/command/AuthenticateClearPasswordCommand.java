package com.github.shyiko.mysql.binlog.network.protocol.command;

import com.github.shyiko.mysql.binlog.io.ByteArrayOutputStream;

import java.io.IOException;

public class AuthenticateClearPasswordCommand implements  Command {
    private String password;

    public AuthenticateClearPasswordCommand(String password) {
        this.password = password;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        buffer.writeZeroTerminatedString(password);
        return buffer.toByteArray();
    }
}
