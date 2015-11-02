package algorithm;

import org.apache.hadoop.util.ToolRunner;
import util.HadoopUtils;

/**
 * Created by Fansy on 2015/11/2.
 */
public class ClassifyDriverTest {
    public static void main(String[] args)throws Exception{
        // third  classify data
        // naivebayes_predict.txt
        //14.23,1.71,2.43,15.6,127,2.8,3.06,.28,2.29,5.64,1.04,3.92,1065
        // x1,x2,...x13
       String [] arg= new String[]{
                "-i",HadoopUtils.getNamenode()+"/user/Administrator/naivebayes_predict.txt",
                "-o",HadoopUtils.getNamenode()+"/user/Administrator/rf_classify_addSource",
                "-splitter",",",
                "-addSource","true",
                "-model",HadoopUtils.getNamenode()+"/user/Administrator/rf_model",
                "-dataset",HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.info"
        };
        HadoopUtils.delHDFS(HadoopUtils.getNamenode()+"/user/Administrator/rf_classify_addSource");
//        ClassifyDriver.main(arg);
        ToolRunner.run(HadoopUtils.getConf(),new ClassifyDriver(),args);

        //  classify data without source
        arg[7]="false";
        arg[3]=HadoopUtils.getNamenode()+"/user/Administrator/rf_classify";
        HadoopUtils.delHDFS(HadoopUtils.getNamenode()+"/user/Administrator/rf_classify");
//        ClassifyDriver.main(arg);
        ToolRunner.run(HadoopUtils.getConf(),new ClassifyDriver(),arg);
    }

}

