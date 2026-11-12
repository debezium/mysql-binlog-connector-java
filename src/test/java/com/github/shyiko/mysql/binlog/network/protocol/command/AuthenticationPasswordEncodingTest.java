package com.github.shyiko.mysql.binlog.network.protocol.command;

import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.testng.Assert.assertEquals;

/**
 * The server hashes the bytes of the password, so the client must not turn them into the default charset of the JVM.
 */
public class AuthenticationPasswordEncodingTest {

    private static final String PASSWORD = "비밀번호";
    private static final String SCRAMBLE = "0123456789abcdefghij";

    @Test
    public void testNativePasswordUsesUtf8() throws Exception {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] stage1 = sha1.digest(PASSWORD.getBytes(StandardCharsets.UTF_8));
        byte[] stage2 = sha1.digest(stage1);
        sha1.update(SCRAMBLE.getBytes(StandardCharsets.UTF_8));
        sha1.update(stage2);
        byte[] expected = CommandUtils.xor(stage1, sha1.digest());

        assertEquals(AuthenticateSecurityPasswordCommand.passwordCompatibleWithMySQL411(PASSWORD, SCRAMBLE), expected);
    }

    @Test
    public void testCachingSha2PasswordUsesUtf8() throws Exception {
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        byte[] stage1 = sha256.digest(PASSWORD.getBytes(StandardCharsets.UTF_8));
        byte[] stage2 = sha256.digest(stage1);
        sha256.update(stage2);
        sha256.update(SCRAMBLE.getBytes(StandardCharsets.UTF_8));
        byte[] expected = CommandUtils.xor(stage1, sha256.digest());

        assertEquals(new AuthenticateSHA2Command(SCRAMBLE, PASSWORD).toByteArray(), expected);
    }
}
