package org.shangu.client;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author : kean
 * @version V1.0
 * @Project: hadoop
 * @Package org.shangu.client
 * @Description: TODO
 * @date Date : 2019-04-23 22:08
 */

public class HDFSClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSClient.class);

    private static String DFSMasterName = "fs.defaultFS";

    private static String nameNodeURI = "hdfs://172.16.21.220:9000/";

    private static Configuration conf = new Configuration();

    private static FileSystem fs;

    private static void initFs() throws IOException, URISyntaxException, InterruptedException {
        // menthod1: 此方法需要编辑VM options  设置用户，为访hdfs访问文件的owner：-DHADOOP_USER_NAME=root
        // conf.set("fs.defaultFS", "hdfs://172.16.21.220:9000/");
        // fs = FileSystem.get(conf);
        // method2:
        conf.set("dfs.replication", "2");
        fs = FileSystem.get(new URI(nameNodeURI), conf, "root");
        LOGGER.info("connect hdfs:");

        FSDataInputStream fsDataInputStream = fs.open(new Path("/wordcount/output/part-r-00000"));
        String line = null;
        while (null != (line = fsDataInputStream.readLine())) {
            System.out.println(line);
        }
    }

    public static void main(String[] args)  throws IOException, URISyntaxException, InterruptedException {
        initFs();
        LOGGER.info("{}", fs.getHomeDirectory());
        LOGGER.info("{}", fs.getUri());
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        while (remoteIterator.hasNext()) {
            LOGGER.info("{}", remoteIterator.next().getPath().getName());
        }
        copyFromLocal();
        fs.close();
    }

    public static void copyFromLocal() throws IOException {
        // fs.copyFromLocalFile(new Path("D:\\java_workspace\\hadoop\\src\\main\\resources\\core-site.xml"), new Path("/hdfs/core-site.xml"));
        fs.copyFromLocalFile(new Path("D:\\java_workspace\\hadoop_spark_java\\src\\main\\java\\org\\shangu\\wordcount\\WordCountReducer.java"), new Path("/core-site4.xml"));
    }
}
