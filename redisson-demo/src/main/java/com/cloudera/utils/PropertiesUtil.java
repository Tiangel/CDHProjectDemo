package com.cloudera.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

public class PropertiesUtil implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);

    public static Properties loadProperties(String filePath) {
        Properties properties = new Properties();
        InputStream in = null;
        try {
            //文件要放到resource文件夹下
            in = PropertiesUtil.class.getClassLoader().getResourceAsStream(filePath);
            if (in == null) {
                return null;
            }
            properties.load(in);
            return properties;
        } catch (Exception e) {
            logger.error("get the config error:", e);
            e.printStackTrace();
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}