package com.cloudera;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TestKudu {
    private static final String KUDU_MASTERS = "10.101.40.224,10.101.40.225,10.101.40.220";
    private static final String KUDU_TABLE_NAME = "test.users";


    /**
     * 创建表
     */
    @Test
    public void testCreateTable() throws KuduException {
        //1、创建Schema
        List<ColumnSchema> columns = new ArrayList<ColumnSchema>(2);
        columns.add(new ColumnSchema.ColumnSchemaBuilder("uid", Type.INT8).key(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("name", Type.STRING).nullable(true).build());
        columns.add(new ColumnSchema.ColumnSchemaBuilder("age", Type.INT8).build());
        Schema schema = new Schema(columns);
        //2、指定表选项
        // 2.1 建表选项
        CreateTableOptions tableOptions = new CreateTableOptions();
        //2.2 创建分区字段列表(必须是主键列)
        List<String> hashCls = new ArrayList<String>();
        hashCls.add("uid");
        int numBuckets = 6;
        //2.3 分区策略
        tableOptions.addHashPartitions(hashCls, numBuckets).setNumReplicas(1);
        //3、创建KuduClient
        KuduClient client = null;
        try {
            client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
            //4、创建表
            if (!client.tableExists(KUDU_TABLE_NAME)) {
                client.createTable(KUDU_TABLE_NAME, schema, tableOptions);
                System.out.println(".........create table success.........");
            } else {
                System.out.println(".........the table already exists .........");
            }
        } finally {
            // 5、关闭资源
            if (null != client) {
                client.shutdown();
            }
        }
    }

    /**
     * 插入数据
     */
    @Test
    public void testInsert() throws KuduException {
        //1、获得kudu客户端
        KuduClient client = null;
        //2、打开表
        KuduTable table = null;
        //3、创建会话
        KuduSession session = null;
        try {
            client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
            table = client.openTable(KUDU_TABLE_NAME);
            session = client.newSession();
            session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
            session.setFlushInterval(2000);
            //4、循环插入10行记录
            for (int i = 0; i < 100; i++) {
                //新建Insert对象
                Insert insert = table.newInsert();
                PartialRow row = insert.getRow();
                row.addByte("uid", Byte.parseByte(i + ""));
                //i是偶数
                if (i % 2 == 0) {
                    row.setNull("name");
                } else {
                    row.addString("name", "name " + i);
                }
                row.addByte("age", Byte.parseByte(i + "")); //加入session
                session.apply(insert);
            }
            //5、关闭session
            session.close();
            //判断错误数
            if (session.countPendingErrors() != 0) {
                //获得操作结果
                RowErrorsAndOverflowStatus result = session.getPendingErrors();
                if (result.isOverflowed()) {
                    System.out.println("............buffer溢出!.................");
                }
                RowError[] errs = result.getRowErrors();
                for (RowError er : errs) {
                    System.out.println(er);
                }
            }
        } finally {
            if (null != client) {
                client.shutdown();
            }
        }
    }


    /**
     * 查询数据
     */
    @Test
    public void testSelect() throws KuduException {
        //1、获得kudu客户端
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        //2、打开表
        KuduTable table = client.openTable(KUDU_TABLE_NAME);
        //3、扫描器
        KuduScanner scanner = null;
        try {
            //4、获取表结构
            Schema schema = table.getSchema();
            //5、指定查询条件
            List<String> projectColumns = new ArrayList<String>(2);
            projectColumns.add("uid");
            projectColumns.add("name");
            projectColumns.add("age");
            //age >= 0
            int lowerBound = 0;
            KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(schema.getColumn("age"), KuduPredicate.ComparisonOp.GREATER_EQUAL, lowerBound);
            //age < 10
            int upperBound = 10;
            KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(schema.getColumn("age"), KuduPredicate.ComparisonOp.LESS, upperBound);
            scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).addPredicate(lowerPred).addPredicate(upperPred).build();
            int resultCount = 0;
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    byte uid = result.getByte("uid");
                    String name = null;
                    if (result.isNull("name")) {
                        name = "不存在";
                    } else {
                        name = result.getString("name");
                    }
                    byte age = result.getByte("age");
                    System.out.printf("uid=%d, name=%s, age=%d\r\n", uid, name, age);
                    resultCount++;
                }
            }
            System.out.println("-----------------------" + resultCount);
            scanner.close();
        } finally {
            if (null != client) {
                client.shutdown();
            }
        }
    }

    /**
     * 修改表结构
     */
    @Test
    public void testAlterTable() throws Exception {
        //1、获得kudu客户端
        KuduClient client = null;
        try {
            //2、修改表选项
            AlterTableOptions ato = new AlterTableOptions();
            ato.addColumn("wage", Type.DOUBLE, 10000.000);
            //3、修改表结构
            client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
            if (client.tableExists(KUDU_TABLE_NAME)) {
                client.alterTable(KUDU_TABLE_NAME, ato);
                System.out.println("........alterTable success..........");
            }
        } finally {
            //4、关闭资源
            if (null != client) {
                client.shutdown();
            }
        }
    }

    /**
     * 修改完再次查询
     */
    @Test
    public void testSelect2() throws KuduException {
        //1、获得kudu客户端
        KuduClient client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
        //2、打开表
        KuduTable table = client.openTable(KUDU_TABLE_NAME);
        //3、扫描器
        KuduScanner scanner = null;
        try {
            //4、获取表结构
            Schema schema = table.getSchema();
            //5、指定查询条件
            List<String> projectColumns = new ArrayList<String>(2);
            projectColumns.add("uid");
            projectColumns.add("name");
            projectColumns.add("age");
            projectColumns.add("wage");
            //age >= 0
            int lowerBound = 0;
            KuduPredicate lowerPred = KuduPredicate.newComparisonPredicate(schema.getColumn("age"), KuduPredicate.ComparisonOp.GREATER_EQUAL, lowerBound);
            //age < 10
            int upperBound = 10;
            KuduPredicate upperPred = KuduPredicate.newComparisonPredicate(schema.getColumn("age"), KuduPredicate.ComparisonOp.LESS, upperBound);
            scanner = client.newScannerBuilder(table).setProjectedColumnNames(projectColumns).addPredicate(lowerPred).addPredicate(upperPred).build();
            int resultCount = 0;
            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    byte uid = result.getByte("uid");
                    String name = null;
                    if (result.isNull("name")) {
                        name = "不存在";
                    } else {
                        name = result.getString("name");
                    }
                    byte age = result.getByte("age");
                    double wage = result.getDouble("wage");
                    System.out.printf("uid=%d, name=%s, age=%d, wage=%f\r\n", uid, name, age, wage);

                    resultCount++;
                }
            }
            System.out.println("-----------------------" + resultCount);
            scanner.close();
        } finally {
            if (null != client) {
                client.shutdown();
            }
        }
    }


    /**
     * 更新数据
     */
    @Test
    public void testUpdate() throws Exception {
        //1、获得kudu客户端
        KuduClient client = null;
        //2、打开表
        KuduTable table = null;
        //3、会话
        KuduSession session = null;
        try {
            client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
            table = client.openTable(KUDU_TABLE_NAME);
            session = client.newSession();
            //4、创建并执行update操作
            Update update = table.newUpdate();
            PartialRow row = update.getRow();
            row.addByte("uid", Byte.parseByte("1" + ""));
            row.addDouble("wage", 20000.000);
            session.apply(update);
            session.close();
        } finally {
            //5、关闭资源
            if (null != client) {
                client.shutdown();
            }
        }
    }

    /**
     * 删除数据
     */
    @Test
    public void testDelete() throws Exception {
        //1、获得kudu客户端
        KuduClient client = null;
        //2、打开表
        KuduTable table = null;
        //3、会话
        KuduSession session = null;
        try {
            client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
            table = client.openTable(KUDU_TABLE_NAME);
            session = client.newSession();
            //4、新建并执行Delete操作
            Delete delete = table.newDelete();
            // 得到row
            PartialRow row = delete.getRow();
            // where key = 0
            row.addByte("uid", Byte.parseByte(3 + ""));
            session.apply(delete);
            session.close();
        } finally {
            //5、关闭资源
            if (null != client) {
                client.shutdown();
            }
        }
    }

    /**
     * 更新和插入
     * 注意：
     * Upsert主键一样则更新，否则为新增，不能为空的字段必须提供值，否则不执行。
     */
    @Test
    public void testUpsert() throws Exception {
        //1、获得kudu客户端
        KuduClient client = null;
        //2、打开表
        KuduTable table = null;
        //3、会话
        KuduSession session = null;
        try {
            client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
            table = client.openTable(KUDU_TABLE_NAME);
            session = client.newSession();
            //4、upsert
            Upsert upsert = table.newUpsert();
            PartialRow row = upsert.getRow();
            row.addByte("uid", Byte.parseByte(2 + ""));
            row.addString("name", "tomasLee");
            row.addByte("age", Byte.parseByte(35 + ""));
            row.addDouble("wage", 18000.000);
            session.apply(upsert);
            Upsert upsert1 = table.newUpsert();
            PartialRow row1 = upsert1.getRow();
            row1.addByte("uid", Byte.parseByte(1 + ""));
            row1.addByte("age", Byte.parseByte(8 + ""));
            row1.addDouble("wage", 15000.000);
            session.apply(upsert1);
            session.close();
        } finally {
            //5、关闭资源
            if (null != client) {
                client.shutdown();
            }
        }
    }

    /**
     * 删除表
     */
    @Test
    public void testDeleteTable() throws KuduException {
        //1、创建KuduClient
        KuduClient client = null;
        try {
            client = new KuduClient.KuduClientBuilder(KUDU_MASTERS).build();
            //2、删除表
            if (client.tableExists(KUDU_TABLE_NAME)) {
                client.deleteTable(KUDU_TABLE_NAME);
                System.out.println("........delete table success..........");
            }
        } finally {
            //3、关闭资源
            if (null != client) {
                client.shutdown();

            }
        }
    }
}

