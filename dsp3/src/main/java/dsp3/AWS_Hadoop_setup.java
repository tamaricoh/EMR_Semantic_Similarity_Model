package dsp3;

import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;


public class AWS_Hadoop_setup { 
    static AWS aws = AWS.getInstance();
    
    public static void main(String[] args) {
        aws.createBucketIfNotExists(Env.PROJECT_NAME);
        aws.createBucketIfNotExists(Env.TEST_BUCKET_PATH);
        // aws.createSqsQueue(Env.C0_SQS);
        aws.uploadFileToS3(Env.corpusTest ,Env.TEST_BUCKET_PATH);
        aws.uploadFileToS3(Env.wordRelatednessFile, Env.PROJECT_NAME); // StopWords
        //aws.uploadFileToS3(Env.testingFiles[0], Env.PROJECT_NAME); 
        for(int i =0; i<Env.Steps_Names.length; i++){
            aws.uploadFileToS3(Env.PATH_TO_TARGET + Env.Steps_Names[i] + ".jar", Env.PROJECT_NAME);
        } 

        EmrClient emrClient = EmrClient.builder()
            .region(Env.region1)
            .build();
        
        List<HadoopJarStepConfig> hadoopJarSteps = new ArrayList<>();
        for(int i = 0; i < Env.Steps_Names.length; i++) { 
            hadoopJarSteps.add(HadoopJarStepConfig.builder()
                .jar(Env.getStepJarPath(i))        //path to jar in s3
                .mainClass(Env.Steps_Names[i])
                .args(Env.getStepArgs(i))
                .build());
            System.out.println("Created jar step config for " + Env.Steps_Names[i]);
        }

        List<StepConfig> stepConfigs = new ArrayList<>();
        for(int i = 0; i < Env.Steps_Names.length; i++) { 
            stepConfigs.add(StepConfig.builder()
                .name(Env.Steps_Names[i])
                .hadoopJarStep(hadoopJarSteps.get(i))
                .actionOnFailure(ActionOnFailure.TERMINATE_JOB_FLOW)
                .build());
            System.out.println("Created step config for " + Env.Steps_Names[i]);
        }

        JobFlowInstancesConfig instances = JobFlowInstancesConfig.builder()
            .instanceCount(Env.instanceCount)
            .masterInstanceType("m4.large")
            .slaveInstanceType("m4.large")
            .hadoopVersion(Env.HADOOP_VER)
            .ec2KeyName(Env.KEY_NAME_SSH)
            .keepJobFlowAliveWhenNoSteps(false)
            .placement(PlacementType.builder()
                .availabilityZone(Env.placementRegion)
                .build())
            .build();
        System.out.println("Created instances config.");

        RunJobFlowRequest runFlowRequest = RunJobFlowRequest.builder()
                .name(Env.PROJECT_NAME)
                .instances(instances)
                .steps(stepConfigs)
                .logUri(Env.Logs_URI)
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
                .build();
        System.out.println("Created run flow request.");
        
        RunJobFlowResponse runJobFlowResult = emrClient.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.jobFlowId();

        // AWS.getInstance().sendSQSMessage("job-completion-time", "job: " + Env.PROJECT_NAME + " started at " + System.currentTimeMillis());
        System.out.println("Ran job flow with id: " + jobFlowId);
    }
}