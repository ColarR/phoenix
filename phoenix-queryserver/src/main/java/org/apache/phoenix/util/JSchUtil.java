package org.apache.phoenix.util;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * @author colar
 * @date 2021-01-11 下午5:04
 */
public class JSchUtil {
    private static final Logger logger = LoggerFactory.getLogger(JSchUtil.class);

    public static Session getSession(String host, int port, String username, String password) throws JSchException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(username, host, port);
        Properties prop = new Properties();
        prop.setProperty("StrictHostKeyChecking", "no");
        if (password != null) {
            session.setPassword(password);
        }
        session.setConfig(prop);
        return session;
    }

    /**
     * 远程执行Shell命令
     *
     * @param host
     * @param port
     * @param username
     * @param password
     * @param cmd
     * @return
     * @throws JSchException
     */
    public static String exec(String host, int port, String username, String password, String cmd) throws JSchException, IOException {
        Session session = null;
        ChannelExec exec = null;
        InputStream in = null;
        try {
            session = getSession(host, port, username, password);
            session.connect(60 * 1000);

            if (session.isConnected()) {
                logger.info("JSch已连接至主机：{}!", session.getHost());
            }

            exec = (ChannelExec) session.openChannel("exec");
            exec.setCommand(cmd);
            in = exec.getInputStream();

            logger.info("JSch开始执行命令：{}！", cmd);
            exec.connect();

            // input
            StringBuilder inSb = new StringBuilder();
            int c;
            while ((c = in.read()) != -1) {
                inSb.append((char) c);
            }
            return String.valueOf(inSb);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error("关闭输入流失败！", e);
                }
            }

            if (exec != null) {
                exec.disconnect();
            }

            if (session != null) {
                session.disconnect();
            }
        }
    }
}
