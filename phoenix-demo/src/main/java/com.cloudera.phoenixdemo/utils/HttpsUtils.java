package com.cloudera.phoenixdemo.utils;

import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.*;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.*;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Map;

/**
 * https请求工具类
 *
 * @author seer
 * @date 2018/3/6 9:12
 */
@Component
public class HttpsUtils {
    private Logger logger = LoggerFactory.getLogger(HttpsUtils.class);
    /*
     * https请求是在http请求的基础上加上一个ssl层
     */
    private RestTemplate restTemplate;

    @Value("${egsc.config.ssl.enabled:false}")
    private String sslEnabled;

    @Value("${egsc.config.ssl.pfxpath:src/main/resources}")
    private String pfxpath;

    @Value("${egsc.config.ssl.pfxpwd:123456}")
    private String pfxpwd;

    @Value("${egsc.config.api.client.readTimeout:10000}")
    private int clientReadTimeout;

    @Value("${egsc.config.api.client.connectTimeout:10000}")
    private int clientConnectTimeout;

    private String frontType;

    private String authorization;

    public static String doPost(String requestUrl, String bodyStr, Map<String, String> header, String charset, String contentType) throws Exception {
        System.out.printf("--- https post 请求地址:%s 内容:%s", requestUrl, bodyStr);
        charset = null == charset ? "utf-8" : charset;

        // 创建SSLContext
        SSLContext sslContext = SSLContext.getInstance("SSL");
        TrustManager[] trustManagers = {new X509TrustManager() {
            /*
             * 实例化一个信任连接管理器
             * 空实现是所有的连接都能访问
             */
            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

            }

            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {

            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }};
        // 初始化
        sslContext.init(null, trustManagers, new SecureRandom());
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

        URL url = new URL(requestUrl);
        HttpsURLConnection httpsURLConnection = (HttpsURLConnection) url.openConnection();
        httpsURLConnection.setSSLSocketFactory(sslSocketFactory);

        // 以下参照http请求
        httpsURLConnection.setDoOutput(true);
        httpsURLConnection.setDoInput(true);
        httpsURLConnection.setUseCaches(false);
        httpsURLConnection.setRequestMethod("POST");
        httpsURLConnection.setRequestProperty("Accept-Charset", charset);
        if (null != bodyStr) {
            httpsURLConnection.setRequestProperty("Content-Length", String.valueOf(bodyStr.length()));
        }
        if (contentType == null) {
            httpsURLConnection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
        } else {
            httpsURLConnection.setRequestProperty("Content-Type", contentType);
        }
        if (!header.isEmpty()) {
            for (Map.Entry<String, String> entry : header.entrySet()) {
                httpsURLConnection.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }
        httpsURLConnection.connect();

        // 读写内容
        OutputStream outputStream = null;
        InputStream inputStream = null;
        InputStreamReader streamReader = null;
        BufferedReader bufferedReader = null;
        StringBuffer stringBuffer;
        try {
            if (null != bodyStr) {
                outputStream = httpsURLConnection.getOutputStream();
                outputStream.write(bodyStr.getBytes(charset));
                outputStream.close();
            }

            if (httpsURLConnection.getResponseCode() >= 300) {
                throw new Exception("https post failed, response code " + httpsURLConnection.getResponseCode());
            }

            inputStream = httpsURLConnection.getInputStream();
            streamReader = new InputStreamReader(inputStream, charset);
            bufferedReader = new BufferedReader(streamReader);
            stringBuffer = new StringBuffer();
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                stringBuffer.append(line);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
            if (streamReader != null) {
                streamReader.close();
            }
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
        System.out.printf("--- https post 返回内容:%s", stringBuffer.toString());
        return stringBuffer.toString();
    }

    public static String doGet(String httpurl) throws IOException {
        HttpURLConnection connection = null;
        InputStream is = null;
        BufferedReader br = null;
        String result = null;// 返回结果字符串
        try {
            // 创建远程url连接对象
            URL url = new URL(httpurl);
            // 通过远程url连接对象打开一个连接，强转成httpURLConnection类
            connection = (HttpURLConnection) url.openConnection();
            // 设置连接方式：get
            connection.setRequestMethod("GET");
            // 设置连接主机服务器的超时时间：15000毫秒
            connection.setConnectTimeout(15000);
            // 设置读取远程返回的数据时间：60000毫秒
            connection.setReadTimeout(60000);
            // 发送请求
            connection.connect();
            // 通过connection连接，获取输入流
            if (connection.getResponseCode() == 200) {
                is = connection.getInputStream();
                // 封装输入流is，并指定字符集
                br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                // 存放数据
                StringBuffer sbf = new StringBuffer();
                String temp = null;
                while ((temp = br.readLine()) != null) {
                    sbf.append(temp);
                    sbf.append("\r\n");
                }
                result = sbf.toString();
            }
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            if (null != br) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (null != is) {
                try {
                    is.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            connection.disconnect();// 关闭远程连接
        }

        return result;
    }

    public static void main(String[] args) {
        String url = args[0];
        String content = args[1];
        try {
            HttpsUtils.doPost(url, content, null, null, "application/json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 用于访问服务的客户端
     *
     * @return RestTemplate
     */
    protected RestTemplate getRestTemplate() {
        boolean flag = false;
        try {
            if ("true".equals(sslEnabled)) {
                CloseableHttpClient httpClient = acceptsUntrustedCertsHttpClient();
                if (null != httpClient) {
                    HttpComponentsClientHttpRequestFactory clientHttpRequestFactory =
                            new HttpComponentsClientHttpRequestFactory(httpClient);

                    clientHttpRequestFactory.setReadTimeout(clientReadTimeout);// ms
                    clientHttpRequestFactory.setConnectTimeout(clientConnectTimeout);// ms

                    restTemplate = new RestTemplate(clientHttpRequestFactory);
                    flag = true;
                }
            }
        } catch (KeyManagementException | KeyStoreException | NoSuchAlgorithmException
                | UnrecoverableKeyException | CertificateException | IOException e) {
            logger.error("实例化RestTemplate异常", e);
        }

        if (!flag) {
            SimpleClientHttpRequestFactory clientHttpRequestFactory =
                    new SimpleClientHttpRequestFactory();
            clientHttpRequestFactory.setReadTimeout(clientReadTimeout);// ms
            clientHttpRequestFactory.setConnectTimeout(clientConnectTimeout);// ms
            restTemplate = new RestTemplate(clientHttpRequestFactory);
        }

        return restTemplate;
    }

    /**
     * @return CloseableHttpClient
     * @throws KeyStoreException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @throws CertificateException
     * @throws IOException
     * @throws UnrecoverableKeyException
     */
    private CloseableHttpClient acceptsUntrustedCertsHttpClient()
            throws KeyStoreException, NoSuchAlgorithmException, KeyManagementException,
            CertificateException, IOException, UnrecoverableKeyException {
        logger.debug("Start acceptsUntrustedCertsHttpClient");

        HttpClientBuilder b = HttpClientBuilder.create();

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        InputStream instream = this.getClass().getResourceAsStream(pfxpath);
        logger.debug(String.format("client cer path : %s, password: %s", pfxpath, pfxpwd));

        if (null == instream) {
            logger.error("Can't load " + pfxpath);
            return null;
        }
        try {
            keyStore.load(instream, pfxpwd.toCharArray());
            logger.debug(String.format("Load %s sucessfully", pfxpath));
        } finally {
            instream.close();
        }

        logger.debug("Load keystore into sslcontext");
        SSLContext sslcontext =
                SSLContexts.custom().loadKeyMaterial(keyStore, pfxpwd.toCharArray()).build();

        logger.debug("initialize one new sslContext and set it to http client builder");
        SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
            @Override
            public boolean isTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
                return true;
            }
        }).build();
        b.setSSLContext(sslContext);

        logger.debug("initialize a ssl socket factory");
        SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslcontext,
                new String[]{"TLSv1"}, null, SSLConnectionSocketFactory.getDefaultHostnameVerifier());

        logger.debug("initialize a socket factory registry");
        Registry<ConnectionSocketFactory> socketFactoryRegistry =
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("http", PlainConnectionSocketFactory.getSocketFactory())
                        .register("https", sslSocketFactory).build();

        logger.debug("initialize a http client connection manager with the socket factory registry");
        PoolingHttpClientConnectionManager connMgr =
                new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        connMgr.setMaxTotal(200);
        connMgr.setDefaultMaxPerRoute(100);
        b.setConnectionManager(connMgr);

        logger.debug("build a ssl http client");
        CloseableHttpClient httpClient = b.build();

        logger.debug("End acceptsUntrustedCertsHttpClient");
        return httpClient;
    }

    /**
     * 封装公共框架
     *
     * @param url
     * @return ResponseDto
     */
    public ResponseDto getForObject(String url) {

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        headers.add("Accept", MediaType.APPLICATION_JSON_VALUE);
        URI uri = URI.create(url);
        logger.info(String.format("get -> %s", uri.toString()));
        if (logger.isDebugEnabled()) {
            //logger.debug("Request: " + JsonUtil.toJsonString(requestDto));
        }

        ResponseDto response = null;
        try {
            response = getRestTemplate().getForObject(uri, ResponseDto.class);
        } catch (ResourceAccessException e) {
            logger.error(e.getMessage());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        if (logger.isDebugEnabled()) {
            //logger.debug("Response: " + JsonUtil.toJsonString(response));
        }

        return response;
    }
}