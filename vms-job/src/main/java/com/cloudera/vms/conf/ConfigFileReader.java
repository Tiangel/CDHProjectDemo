package com.cloudera.vms.conf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

//import org.apache.log4j.Logger;

/**
 * Created by Twin on 2017/11/20.
 */
public class ConfigFileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigFileReader.class);
    private String fileName;

    public ConfigFileReader(String fileName) {
        this.fileName = fileName;
        refreshConfig(fileName);
    }

    Properties pps = new Properties();
    public void refreshConfig(String fileName) {
		try {
			pps.load(ConfigFileReader.class.getResourceAsStream("/" + fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    public String getConfigValue(String key) {
        if (pps.containsKey(key)) {
            return pps.getProperty(key);
        } else {
            LOGGER.warn("无配置信息：" + key);
            return null;
        }
    }
}
