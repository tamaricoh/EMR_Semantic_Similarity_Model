package dsp3;

// import software.amazon.awssdk.regions.Region;

public class Env {

    public static final String TAB = "\t";
    public static final String SPACE = "\\s"; 
    public static final String FORWARD_SLASH = "/"; 
    //to run change these:
    // public static String stopWordsFile = "C:\\Users\\tamar\\Desktop\\B.Sc\\Semester G\\AWS\\Assignment_2\\dsp_2\\src\\main\\resources\\heb-stopwords.txt";
    // public static final String PATH_TO_TARGET = "C:\\Users\\tamar\\Desktop\\B.Sc\\Semester G\\AWS\\Assignment_2\\dsp_2\\target";
    // public static boolean localAggregationCommand = false;
    // public static final int instanceCount = 9;
    // public static final String PROJECT_NAME = "high-instance-count101";
    // public static final String Logs_URI = "s3://" + PROJECT_NAME + "/logs";
    // public static final String C0_SQS = "c0-" + PROJECT_NAME;


    

    // public static Region region1 = Region.US_EAST_1;
    // public static Region region2 = Region.US_WEST_2;
    // public static String placementRegion = "us-east-1a"; 

    // public static final String HADOOP_VER = "3.3.6"; //newest one so why not
    // public static final String KEY_NAME_SSH = "vockey";

    // public static final String TERMINATE_JOB_FLOW_MESSAGE = "TERMINATE_JOB_FLOW";

    // public static final String HEB_3Gram_path = "s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data";
    // public static final String S3_BUCKET_PATH = "s3://" + PROJECT_NAME + "/";
    // public static final String[] Steps_Names = {"CalcVariablesStep", "valuesJoinerStep", "probabilityCalcStep", "sortSequencesStep"};
    // public static final String[] Step_Output_Name = {"step1-output", "step2-output", "step3-output", "step4-output"};

    // public static String getStepJarPath(int i){
    //     return  getPathS3(Steps_Names[i], ".jar");
    // }
    // public static String getPathS3(String file_name, String file_format){
    //     return S3_BUCKET_PATH + file_name + file_format;
    // }
    // public static String[] getStepArgs(int stepNum){
    //     String[] args;
    //     switch (stepNum){
    //         case 0:
    //             args = new String[]{ 
    //                 HEB_3Gram_path,
    //                 "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0]
    //             };
    //             System.out.println("[DEBUG]  step output name: " + Step_Output_Name[0]);
    //             break;
    //         case 1:
    //             args = new String[]{
    //                 "s3://" + PROJECT_NAME + "/" + Step_Output_Name[0], // input
    //                 "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1] // output
    //             };
    //             break;
    //         case 2:
    //             args = new String[]{
    //                 "s3://" + PROJECT_NAME + "/" + Step_Output_Name[1], // input
    //                 "s3://" + PROJECT_NAME + "/" + Step_Output_Name[2] // output
    //             };
    //             break;
    //         case 3:
    //             args = new String[]{
    //                 "s3://" + PROJECT_NAME + "/" + Step_Output_Name[2], // input
    //                 "s3://" + PROJECT_NAME + "/" + Step_Output_Name[3] // output
    //             };
    //             break;
    //         default:
    //             args = new String[]{};
    //     }
    //     return args;
    // }
}