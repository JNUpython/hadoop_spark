package org.shangu.serialization;

import org.apache.commons.httpclient.URIException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author : kean
 * @version V1.0
 * @Project: hadoop_spark_java
 * @Package org.shangu.serialization
 * @Description: TODO
 * @date Date : 2019-05-03 12:38
 */

public class Data {

    private static String nameNodeURI = "hdfs://172.16.21.220:9000/";

    public static void copyLocal2Dfs(String localPath, String dfsPath) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(nameNodeURI), configuration, "root");
        fs.copyFromLocalFile(new Path(localPath), new Path(dfsPath));
    }

    public static void mkdirDfs(String fileName) throws URISyntaxException, IOException, InterruptedException  {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(nameNodeURI), configuration, "root");
        fs.mkdirs(new Path(fileName));
    }

    public static void removeDfs(String fileName) throws URISyntaxException, IOException, InterruptedException  {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(nameNodeURI), configuration, "root");
        //fs.removeAcl(new Path(fileName));
        fs.delete(new Path(fileName), true);
    }

    public static void copyDfs2Local(String dfsPath, String localPath) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI(nameNodeURI), configuration, "root");
        fs.copyToLocalFile(new Path(dfsPath), new Path(localPath));
    }

    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException  {
        //mkdirDfs("/phoneflow/input");
        //copyLocal2Dfs("D:\\java_workspace\\hadoop_spark_java\\data\\phone_data.txt", "/phoneflow/input/phone_flow1.txt");
        //copyLocal2Dfs("D:\\java_workspace\\hadoop_spark_java\\data\\phone_data.txt", "/phoneflow/input/phone_flow2.txt");
        //copyLocal2Dfs("D:\\java_workspace\\hadoop_spark_java\\data\\phone_data.txt", "/phoneflow/input/phone_flow3.txt");
        //copyLocal2Dfs("D:\\java_workspace\\hadoop_spark_java\\data\\phone_data.txt", "/phoneflow/input/phone_flow4.txt");
        ////removeDfs("/phoneflow/input");
        removeDfs("/phoneflow/output");
        //copyDfs2Local("/phoneflow/output/part-r-00000", "data/phone_flow.txt");
    }
}
