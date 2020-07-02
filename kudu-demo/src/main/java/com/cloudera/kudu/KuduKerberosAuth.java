package com.cloudera.kudu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class KuduKerberosAuth {
    /**
     * 初始化访问Kerberos访问
     * @param debug 是否启用Kerberos的Debug模式
     */
    public static void initKerberosENV(Boolean debug) {
        try {
               System.setProperty("java.security.krb5.conf","E:\\CDHProjectDemo\\kudu-demo\\src\\main\\resources\\krb5.conf");
            // System.setProperty("java.security.krb5.conf","/etc/krb5.conf");
            System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
            if (debug){
                System.setProperty("sun.security.krb5.debug", "true");
            }
            Configuration configuration = new Configuration();
            configuration.addResource(new Path("hdfs-client-kb/core-site.xml"));
            configuration.addResource(new Path("hdfs-client-kb/hdfs-site.xml"));
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab("admin@EVERGRANDE.COM", "E:\\CDHProjectDemo\\kudu-demo\\src\\main\\resources\\admin.keytab");
            // UserGroupInformation.loginUserFromKeytab("hdfs@EVERGRANDE.COM", "/root/hdfs.keytab");

            System.out.println(UserGroupInformation.getCurrentUser());
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
}
