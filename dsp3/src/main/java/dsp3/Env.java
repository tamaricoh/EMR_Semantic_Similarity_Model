package dsp3;

import software.amazon.awssdk.regions.Region;

public class Env {

    public static final String TAB = "\t";
    public static final String SPACE = "\\s"; 
    public static final String space = " "; 
    public static final String FORWARD_SLASH = "/";
    public static final String DASH = "-";
    // public static final String DEL = "$$";
    public static final String inputBucket = "Ngram";

    //to run change these:
    public static String wordRelatednessFile = "C:\\Users\\tamar\\Desktop\\B.Sc\\Semester G\\AWS\\Assignment_3\\dsp3\\src\\main\\java\\dsp3\\resources\\word-relatedness.txt";
    public static String wordSimilarityClassifier = "C:\\Users\\tamar\\Desktop\\B.Sc\\Semester G\\AWS\\Assignment_3\\dsp3\\target\\classifier.jar";
    public static String wordRelatednessKey = "word-relatedness.txt";
    public static String csvName = "word_similarity.csv";
    public static String outputName = "classification_results.txt";
    public static String csvFileLoc = "step3/" + csvName;
    public static final String PATH_TO_TARGET = "C:\\Users\\tamar\\Desktop\\B.Sc\\Semester G\\AWS\\Assignment_3\\dsp3\\target\\";
    public static final String corpusTest = "C:\\Users\\tamar\\Desktop\\B.Sc\\Semester G\\AWS\\Assignment_3\\dsp3\\test.txt";
    // public static boolean localAggregationCommand = false;
    public static final int instanceCount = 9;
    public static final String PROJECT_NAME = "semantic-similarity-full-run";
    public static final String Logs_URI = "s3://" + PROJECT_NAME + "/logs";
    // public static final String C0_SQS = "c0-" + PROJECT_NAME;


    

    public static Region region1 = Region.US_EAST_1;
    public static Region region2 = Region.US_WEST_2;
    public static String placementRegion = "us-east-1a"; 

    public static final String HADOOP_VER = "3.3.6"; //newest one so why not
    public static final String KEY_NAME_SSH = "vockey";

    // public static final String TERMINATE_JOB_FLOW_MESSAGE = "TERMINATE_JOB_FLOW";

    public static final String TEST_BUCKET_PATH = PROJECT_NAME +"-test";
    public static final String F = "count-uppercase-f";
    public static final String L = "count-upeercase-l";
    public static final String S3_BUCKET_PATH = "s3://" + PROJECT_NAME + "/";
    public static final String FEATURE_SET_PATH =  S3_BUCKET_PATH + "feature-set";
    public static final String LEMMATA_SET_PATH = S3_BUCKET_PATH + "lemmata-set";
    public static final String[] Steps_Names = {"step1", "step2", "step3"};
    public static final String[] Step_Output_Name = {"step1", "step2","step3"};

    public static String getStepJarPath(int i){
        return  getPathS3(Steps_Names[i], ".jar");
    }
    public static String getPathS3(String file_name, String file_format){
        return S3_BUCKET_PATH + file_name + file_format;
    }
    public static String[] getStepArgs(int stepNum){
        String[] args;
        switch (stepNum){
            case 0:
                args = new String[]{ 
                    "s3://biarcs/",
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0]
                };
                break;
            case 1:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0], // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1] // output
                };
                break;
            case 2:
                args = new String[]{
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1], // input
                    "s3://" + PROJECT_NAME + "/" + Step_Output_Name[2] // output
                };
                break;
            default:
                args = new String[]{};
        }
        return args;
    }
}