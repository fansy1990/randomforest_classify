package util;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.mahout.classifier.df.data.Dataset;
import org.apache.mahout.classifier.df.data.Instance;
import org.apache.mahout.math.DenseVector;

/**
 * Created by Fansy on 2015/11/2.
 */
public class DataConverter {

//    private static final Pattern COMMA_SPACE = Pattern.compile("[, ]");

    private String splitter;
    private final Dataset dataset;

    public DataConverter(Dataset dataset,String splitter) {
        this.dataset = dataset;
        this.splitter=splitter;
    }

    public Instance convert(String string) {
        // all attributes (categorical, numerical, label), ignored
        // get rid of label ,the data only contains (categorical, numerical),ignored
        int nball = dataset.nbAttributes() + dataset.getIgnored().length-1;
// 把label列添加到vector中，方便直接调用forest的classify函数
        String[] tokens = string.split(splitter);
        Preconditions.checkArgument(tokens.length == nball,
                "Wrong number of attributes in the string: " + tokens.length + ". Must be " + nball);

        int nbattrs = dataset.nbAttributes();
        DenseVector vector = new DenseVector(nbattrs);

        int aId = 0;
        for (int attr = 0; attr < nball;) {
            if(dataset.getLabelId()==attr){//  label 列所在下标
                vector.set(aId++,0);// 对于label列直接赋值0
            }
            if (!ArrayUtils.contains(dataset.getIgnored(), attr)) {
                String token = tokens[attr].trim();

                if ("?".equals(token)) {
                    // missing value
                    return null;
                }

                if (dataset.isNumerical(aId)) {
                    vector.set(aId++, Double.parseDouble(token));
                } else { // CATEGORICAL
                    vector.set(aId, dataset.valueOf(aId, token));
                    aId++;
                }
                
            }
            attr++;
        }

        return new Instance(vector);
    }
}
