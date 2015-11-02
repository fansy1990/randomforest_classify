package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.util.Properties;

/**
 * Created by Fansy on 2015/9/24.
 */
public class HadoopUtils {
    private static String NODE=null ;
    private static  String NAMENODE = null;
    private static String RESOURCEMANAGER =null;
    private static String FRAMEWORK =null;
    private static String  SCHEDULER =null;
    private static String JOBHISTORY =null;
    private static Boolean CROSS_PLATFORM =null;

    public static String getNode(){
        Properties prop = new Properties();
        try{
            prop.load(HadoopUtils.class.getClassLoader().getResourceAsStream("hadoop.properties"));
        }catch(Exception e){
            e.printStackTrace();
        }
        NODE = prop.getProperty("node");
        NAMENODE = prop.getProperty("namenode");
        RESOURCEMANAGER = prop.getProperty("resourcemanager");
        FRAMEWORK=prop.getProperty("framework");
        SCHEDULER = prop.getProperty("scheduler");
        JOBHISTORY = prop.getProperty("jobhistory");
        CROSS_PLATFORM = Boolean.parseBoolean(prop.getProperty("cross_platform"));
        return NODE;
    }

    public static String getNamenode(){
        if(NAMENODE==null){
            getNode();
        }
        return NAMENODE;
    }

    private static Configuration conf =null;
    private static FileSystem fs ;
    public static FileSystem getFileSystem(){
        try {
            return FileSystem.get(getConf());
        }catch(Exception e){
            e.printStackTrace();
        }
        return null ;
    }
    public static Configuration getConf(){
        if(NODE==null){
            getNode();
        }
        if(conf ==null){
            conf = new Configuration();
            conf.setBoolean("mapreduce.app-submission.cross-platform",CROSS_PLATFORM);
            conf.set("fs.defaultFS", NAMENODE);// 指定namenode
            conf.set("mapreduce.framework.name",FRAMEWORK); // 指定使用yarn框架
            conf.set("yarn.resourcemanager.address",RESOURCEMANAGER); // 指定resourcemanager
            conf.set("yarn.resourcemanager.scheduler.address", SCHEDULER);// 指定资源分配器
            conf.set("mapreduce.jobhistory.address", JOBHISTORY);


        }
        return conf ;
    }


    /**
     * 上传文件
     * @param localFile
     * @param hdfsFile
     * @return
     */
    public static boolean upload(String localFile,String hdfsFile){
        try{
            getFileSystem().copyFromLocalFile(new Path(localFile),new Path(hdfsFile));
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true ;
    }

    /**
     * 删除hdfs文件
     * @param hdfsFile
     * @return
     */
    public static boolean delHDFS(String hdfsFile ){
        try {
            return getFileSystem().delete(new Path(hdfsFile), true);
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
    }
}

