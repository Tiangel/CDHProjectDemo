package com.cloudera.kudu;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class GetKuduClient {

    private static final String KUDU_MASTERS = System.getProperty("kuduMasters", "bigdata-dev-kerberos-01:7051,bigdata-dev-kerberos-02:7051");

    public static KuduClient getKuduClient() {
        KuduClient client = null;
        try {
            client = UserGroupInformation.getLoginUser().doAs(
                    new PrivilegedExceptionAction<KuduClient>() {
                        public KuduClient run() throws Exception {
                            return new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
                        }
                    }
            );

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return client;
    }
}
