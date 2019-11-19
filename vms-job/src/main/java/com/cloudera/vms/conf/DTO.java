package com.cloudera.vms.conf;

//import org.apache.log4j.Logger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Twin on 2017/9/18.
 */
public class DTO {
    //private static final Logger LOGGER = Logger.getLogger(DTO.class);
    private static final Logger LOGGER = LoggerFactory.getLogger(DTO.class);

    public static ConfigFileReader configFileReader = new ConfigFileReader("env.properties");
    public static void main(String[] args) {
//        System.out.println(AutoConfig.getConfigValue("kafka.brokers"));
        System.out.println(DTO.configFileReader.getConfigValue("es.hosts"));


    }

}
