package dsp3;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import java.util.Random;

public class WordSimilarityClassifier {

    public static Instances loadArffFromS3(String fileKey, String localPath) throws Exception {
        AWS aws = AWS.getInstance();
        aws.downloadFromS3(Env.PROJECT_NAME, fileKey, ""); // TODO - this should be a path to local directory in an EC2?
        DataSource source = new DataSource(localPath);
        return source.getDataSet();
    }
    public static void main(String[] args) {
        try {
            // Load the dataset
            Instances data = loadArffFromS3("word_similarity.arff", "");
            
            // Set the class index to the last attribute
            data.setClassIndex(data.numAttributes() - 1);
            
            // Calculate split sizes
            int trainSize = (int) Math.round(data.numInstances() * 0.8);
            int testSize = data.numInstances() - trainSize;
            
            // Create train/test split
            Instances train = new Instances(data, 0, trainSize);
            Instances test = new Instances(data, trainSize, testSize);
            
            // Create and train the classifier (using Random Forest as an example)
            Classifier classifier = new RandomForest();
            classifier.buildClassifier(train);
            
            // Perform 10-fold cross-validation on training set
            Evaluation trainEval = new Evaluation(train);
            trainEval.crossValidateModel(classifier, train, 10, new Random(42));
            
            System.out.println("=== 10-fold Cross-validation Results (Training Set) ===");
            System.out.println("Precision (True class): " + trainEval.precision(1));
            System.out.println("Recall (True class): " + trainEval.recall(1));
            System.out.println("F1 Score (True class): " + trainEval.fMeasure(1));
            
            // Test the model on test set
            Evaluation testEval = new Evaluation(train);
            testEval.evaluateModel(classifier, test);
            
            System.out.println("\n=== Test Set Results ===");
            System.out.println("Precision (True class): " + testEval.precision(1));
            System.out.println("Recall (True class): " + testEval.recall(1));
            System.out.println("F1 Score (True class): " + testEval.fMeasure(1));
            
            // Print confusion matrix
            System.out.println("\nConfusion Matrix:");
            System.out.println(testEval.toMatrixString());
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}