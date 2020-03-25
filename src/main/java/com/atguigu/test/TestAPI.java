package com.atguigu.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * DDL
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 * <p></p>
 * DML:
 * 5.插入数据
 * 6.查数据(get)
 * 7.查数据(scan)
 * 8.删除数据
 */
public class TestAPI {
    private static Connection connection;
    private static Admin admin;

    static {
        //HBaseConfiguration configuration = new HBaseConfiguration();
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");

        //HBaseAdmin admin = new HBaseAdmin(configuration);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    //1.判断表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    //2.创建表
    public static void createTable(String tableName, String... cfs) throws IOException {
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！");
            return;
        }
        //创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : cfs) {
            //创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);

        }
        admin.createTable(hTableDescriptor);
    }

    //3.删除表
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！");
            return;
        }
        //使表下线
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //4.创建命名空间
    public static void createNameSpace(String ns) {
        NamespaceDescriptor.Builder builder = NamespaceDescriptor.create(ns);
        NamespaceDescriptor build = builder.build();
        try {
            admin.createNamespace(build);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间已存在");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //5.插入数据
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("sex"), Bytes.toBytes("male"));

        table.put(put);
        table.close();
    }

    //6.查数据(get)
    public static void getData(String tableName, String rowKey, String cf, String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        //指定获取的列族
        get.addFamily(Bytes.toBytes(cf));
        //指定列族和列
        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));
        get.setMaxVersions(5);
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {

            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    "  CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    "  Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    //7.查数据(scan)
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1002"));//左闭右开
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("RowKey:"+Bytes.toString(CellUtil.cloneRow(cell))+" CellFamily:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        "  CellQualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        "  Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }
    //8.删除数据
    public static void deleteData(String tableName,String rowKey,String cf,String cn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),1581352670299L);
        table.delete(delete);
        table.close();
    }

    public static void main(String[] args) throws IOException {
        //1 测试表是否存在
        //System.out.println(isTableExist("stu5"));
        //2.创建表测试
        //createTable("0408:stu5","info1","info2");
        //3.删除表测试
        //dropTable("stu5");
        //4.创建命名空间测试
        //createNameSpace("0408");
        //putData("stu", "1001", "info2", "name", "zhangsan");
        //System.out.println(isTableExist("stu5"));
        //6 获取单行数据
        //getData("stu2","1001","info","name");
        //7.查数据(scan)
        //scanTable("stu");
        //8.删除数据
        deleteData("stu","1009","info1","name");
        close();


    }
}
