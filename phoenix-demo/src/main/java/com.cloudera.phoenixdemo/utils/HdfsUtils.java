package com.cloudera.phoenixdemo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class HdfsUtils {
    public static boolean exists(Configuration config,
                                 String path)
            throws IOException {
        FileSystem fs = FileSystem.get(config);
        return fs.exists(new Path(path));
    }

    //��һ���ļ��������ݣ�������hdfs�д����ļ�
    public static void createFile(Configuration config,
                                  String path, byte[] buf) throws IOException {//config-->fs--->fos--->write--->close
        FileSystem fs = FileSystem.get(config);
        Path path2 = new Path(path);
        FSDataOutputStream fos = fs.create(path2);
        fos.write(buf);
        fos.close();
        fs.close();
    }

    public static void createFile(Configuration config,
                                  String path, String info) throws IOException {
        createFile(config, path, info.getBytes());
    }


    public static void copyFromLocal(Configuration config,
                                     String path1, String path2) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path p1 = new Path(path1);
        Path p2 = new Path(path2);
        fs.copyFromLocalFile(true, true, p1, p2);
        fs.close();
    }


    public static boolean deleteFile(Configuration config,
                                     String path1, boolean mark) throws IOException {//config-->fs-->delete--->close--->����
        FileSystem fs = FileSystem.get(config);
        Path p1 = new Path(path1);
        boolean flag = fs.delete(p1, mark);
        fs.close();
        return flag;
    }

    public static boolean deleteFile(Configuration config,
                                     String path1) throws IOException {//config-->fs-->delete--->close--->����
        return deleteFile(config, path1, true);
    }

    //�޸�HDFS�ϵ��ļ�����String-->PATH-->fs.renameFile
    public static boolean renameFile(Configuration config,
                                     String path1, String path2) throws IOException {//config-->fs-->delete--->close--->����
        FileSystem fs = FileSystem.get(config);
        Path oldName = new Path(path1);
        Path newName = new Path(path2);
        boolean flag = fs.rename(oldName, newName);
        fs.close();
        return flag;
    }

    //ɾ��hdfs�ϵ�һ���ļ�
    public static boolean makeDir(Configuration config,
                                  String path1) throws IOException {//config-->fs-->delete--->close--->����
        FileSystem fs = FileSystem.get(config);
        Path oldName = new Path(path1);
        boolean flag = fs.mkdirs(oldName);
        fs.close();
        return flag;
    }

    public static RemoteIterator<LocatedFileStatus> listFiles
            (Configuration config,
             String path1, boolean mark) throws IOException {//config-->fs-->delete--->close--->����
        FileSystem fs = FileSystem.get(config);
        RemoteIterator<LocatedFileStatus> rl = fs.listFiles(
                new Path(path1), mark);
        return rl;
    }

    public static RemoteIterator<LocatedFileStatus> listFiles
            (Configuration config,
             String path1) throws IOException {//config-->fs-->delete--->close--->
        return listFiles(config, path1, false);
    }

    public static FileStatus[] listStatus
            (Configuration config,
             String path1) throws IOException {//config-->fs-->delete--->close--->
        FileSystem fs = FileSystem.get(config);
        Path path = new Path(path1);
        FileStatus status[] = fs.listStatus(path);
        fs.close();
        return status;
    }

    public static String readFile(Configuration config,
                                  String path) throws IOException {
        FileSystem fs = FileSystem.get(config);
        String temp;
        InputStream is = null;
        ByteArrayOutputStream baos = null;
        Path p1 = new Path(path);
        try {
            is = fs.open(p1);
            baos = new ByteArrayOutputStream(is.available());
            IOUtils.copyBytes(is, baos, config);
            temp = baos.toString();
        } finally {
            IOUtils.closeStream(is);
            IOUtils.closeStream(baos);
            fs.close();
        }
        return temp;
    }

    public static boolean createDirectory(
            Configuration config, String path) throws IOException {
        FileSystem fs = FileSystem.get(config);
        Path p = new Path(path);
        boolean flag = fs.mkdirs(p);
        fs.close();
        return flag;
    }
}
