package org.apache.phoenix.util;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author colar
 * @date 2021-01-25 下午4:33
 */
public class HttpUtil {
    /**
     * 连接超时时间
     */
    private static final int CONNECTION_TIMEOUT = 5000;
    /**
     * 请求超时时间
     */
    private static final int METHOD_TIMEOUT = 5000;

    private static final HttpClient httpClient = new HttpClient();

    /**
     * Get请求，获取String类型的Response Body
     *
     * @param url
     * @return
     */
    public static String getStringBody(String url) throws IOException {
        httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(CONNECTION_TIMEOUT);

        GetMethod getMethod = new GetMethod(url);
        getMethod.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, METHOD_TIMEOUT);
        getMethod.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

        try {
            httpClient.executeMethod(getMethod);
            return new String(getMethod.getResponseBody(), StandardCharsets.UTF_8);
        } finally {
            getMethod.releaseConnection();
        }
    }
}
