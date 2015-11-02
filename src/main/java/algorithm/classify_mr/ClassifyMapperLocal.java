package algorithm.classify_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.classifier.df.DecisionForest;
import util.DataConverter;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.Instance;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import util.HadoopUtils;

import java.io.IOException;
import java.util.Random;

import static org.apache.hadoop.mapreduce.filecache.DistributedCache.addCacheFile;

/**
 * Created by Fansy on 2015/11/2.
 */
public class ClassifyMapperLocal {
    private String splitter =null;
    private boolean addSource =false;
    private DataConverter converter;
    private DecisionForest forest;
    private Dataset dataset;
    private final Random rng = RandomUtils.getRandom();
    public static void main(String[] args) throws  IOException{
        new ClassifyMapperLocal().test();
    }

    public void test()throws IOException{
        Configuration conf = HadoopUtils.getConf();

        splitter= ",";
        addSource = conf.getBoolean("addSource",false);
        String datasetP=HadoopUtils.getNamenode()+"/user/Administrator/naivebayes.info";
        String modelPath=HadoopUtils.getNamenode()+"/user/Administrator/rf_model";
        addCacheFile(new Path(datasetP).toUri(), conf);
        addCacheFile(new Path(modelPath).toUri(), conf);
        Path[] files = HadoopUtil.getCachedFiles(conf);
        if (files.length < 2) {
            throw new IOException("not enough paths in the DistributedCache");
        }
        dataset = Dataset.load(conf, files[0]);
        converter = new DataConverter(dataset,splitter);
        forest = DecisionForest.load(conf, files[1]);

        String line ="14.23,1.71,2.43,15.6,127,2.8,3.06,.28,2.29,5.64,1.04,3.92,1065";
//        String line ="12.08,1.83,2.32,18.5,81,1.6,1.5,.52,1.64,2.4,1.08,2.27,480";
        if (!line.isEmpty()) {
            Instance instance = converter.convert(line);
            double prediction = forest.classify(dataset, rng, instance);

            String newLine = dataset.getLabelString(prediction);
            if(addSource){
                newLine= line+splitter+newLine;
            }
            System.out.println("prediction:"+prediction+"\t newLine:"+newLine);
        }
    }
}
