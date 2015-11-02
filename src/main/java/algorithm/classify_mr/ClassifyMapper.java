package algorithm.classify_mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.classifier.df.DecisionForest;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.Instance;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import util.DataConverter;

import java.io.IOException;
import java.util.Random;

/**
 * Created by Fansy on 2015/11/2.
 */
public class ClassifyMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    private String splitter =null;
    private boolean addSource =false;
    private DataConverter converter;
    private DecisionForest forest;
    private Dataset dataset;
    private final Random rng = RandomUtils.getRandom();
    private Text lKey=new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        splitter= conf.get("splitter");
        addSource = conf.getBoolean("addSource",false);
        Path[] files = HadoopUtil.getCachedFiles(conf);

        if (files.length < 2) {
            throw new IOException("not enough paths in the DistributedCache");
        }
        dataset = Dataset.load(conf, files[0]);
        converter = new DataConverter(dataset,splitter);

        forest = DecisionForest.load(conf, files[1]);
        if (forest == null) {
            throw new InterruptedException("DecisionForest not found!");
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.isEmpty()) {
            Instance instance = converter.convert(line);
            double prediction = forest.classify(dataset, rng, instance);

            String newLine = dataset.getLabelString(prediction);
            if(addSource){
                newLine= line+splitter+newLine;
            }
            lKey.set(newLine);
            context.write(lKey, NullWritable.get());
        }
    }
}
