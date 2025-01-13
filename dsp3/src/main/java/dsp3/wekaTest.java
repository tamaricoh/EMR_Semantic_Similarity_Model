package dsp3;

import weka.core.Instances;
import weka.classifiers.Classifier;
import weka.classifiers.trees.J48;
import weka.core.converters.ConverterUtils.DataSource;

public class wekaTest {
    public static void main(String[] args) {
        try {
            DataSource source = new DataSource("data/iris.arff");
            Instances data = source.getDataSet();
            
            if (data.classIndex() == -1) {
                data.setClassIndex(data.numAttributes() - 1);
            }

            // classifier creation
            Classifier classifier = new J48();
            classifier.buildClassifier(data);

            System.out.println(classifier);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
