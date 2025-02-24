package dsp3;

import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayes;
import weka.core.Instances;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils.DataSource;
import java.util.Random;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class wordSimilarityClassifier {

    static AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        try {
            // Instead of downloading from S3, use the path from the MapReduce step
            String localCsvPath = "/tmp";  // Use temp directory in EMR
            // String localCsvPath = "C:\\Users\\tamar\\Downloads";
            aws.downloadFromS3(Env.PROJECT_NAME, Env.csvFileLoc, localCsvPath);
            // String localCsvPath = Env.S3_BUCKET_PATH +"step3/word_similarity.csv";  // Pass the output directory as an argument
            // String localCsvPath = "/tmp/word_similarity.csv";
            // String localCsvPath = "C:\\Users\\tamar\\Downloads\\word_similarity.csv";  // Pass the output directory as an argument

            // System.out.println("[TAMAR] "+localCsvPath);
            // Load dataset from CSV
            Instances data = loadCsv(localCsvPath+"/"+Env.csvName);

            if (data == null) {
                System.out.println("Failed to load data!");
                return;
            }
            
            // Set the class index to the last column (since CSV doesn't have predefined class attribute)
            data.setClassIndex(data.numAttributes() - 1);
    
            // Split data into training and testing sets
            int trainSize = (int) Math.round(data.numInstances() * 0.8);
            int testSize = data.numInstances() - trainSize;
            Instances train = new Instances(data, 0, trainSize);
            Instances test = new Instances(data, trainSize, testSize);
    
            // Train classifier
            Classifier classifier = new NaiveBayes();
            classifier.buildClassifier(train);
    
            // Cross-validation
            Evaluation trainEval = new Evaluation(train);
            trainEval.crossValidateModel(classifier, train, 10, new Random(42));
    
            // Prepare to write results to file
            // String outputFilePath = Env.S3_BUCKET_PATH + "/classification_results.txt";
            // String outputFilePath = "C:\\Users\\tamar\\Downloads\\classification_results.txt";
            String outputFilePath = localCsvPath + "/" + Env.outputName;
            FileWriter writer = new FileWriter(outputFilePath);
    
            // Write Cross-validation results
            writer.write("=== 10-fold Cross-validation Results ===\n");
            writer.write("Precision: " + trainEval.precision(1) + "\n");
            writer.write("Recall: " + trainEval.recall(1) + "\n");
            writer.write("F1 Score: " + trainEval.fMeasure(1) + "\n");

            writer.write("\n=== Training Set Confusion Matrix ===\n");
            writer.write(trainEval.toMatrixString());

            // Evaluate on test set
            Evaluation testEval = new Evaluation(train);
            testEval.evaluateModel(classifier, test);
    
            // Write Test Set results
            writer.write("\n=== Test Set Results ===\n");
            writer.write("Precision: " + testEval.precision(1) + "\n");
            writer.write("Recall: " + testEval.recall(1) + "\n");
            writer.write("F1 Score: " + testEval.fMeasure(1) + "\n");
    
            writer.write("\nConfusion Matrix:\n");
            writer.write(testEval.toMatrixString());
    
            writer.close();  // Close the file writer
    
            // Upload the file to S3
            // String bucketName = Env.PROJECT_NAME;
            aws.uploadFileToS3(outputFilePath, Env.PROJECT_NAME);
    
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Your existing loadCsv method
    public static Instances loadCsv(String csvFilePath) {
        try {
            CSVLoader loader = new CSVLoader();
            File csvFile = new File(csvFilePath);
            
            // Add debug prints
            System.out.println("Attempting to load CSV from: " + csvFilePath);
            System.out.println("File exists: " + csvFile.exists());
            System.out.println("File absolute path: " + csvFile.getAbsolutePath());
            
            if (!csvFile.exists()) {
                throw new IOException("CSV file not found at: " + csvFilePath);
            }
            
            loader.setSource(csvFile);
            return loader.getDataSet();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}