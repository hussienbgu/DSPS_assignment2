package com.amazonaws.dsps2021;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import software.amazon.awssdk.services.ec2.model.InstanceType;

public class Main {

	private static final AWSCredentialsProvider
			credentialsProvider =
			new AWSStaticCredentialsProvider(new ProfileCredentialsProvider()
							.getCredentials());

	public static void main(String[] args) {
		AmazonElasticMapReduce mapReduce =
				AmazonElasticMapReduceClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();

		String minPmi = args[1];
		String relMinPmi = args[2];

		HadoopJarStepConfig step0cfg = new HadoopJarStepConfig()
				.withJar("s3://hussiendsps2/StepRunner.jar")
				.withMainClass("Step0")
				.withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data" )
				.withArgs("s3://hussiendsps2//output")
				.withArgs(minPmi)
				.withArgs(relMinPmi);

		StepConfig step0 = new StepConfig()
				.withName("Step_runner")
				.withActionOnFailure("TERMINATE_JOB_FLOW")
				.withHadoopJarStep(step0cfg);


		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(5)
				.withMasterInstanceType(InstanceType.M4_LARGE.toString())
				.withSlaveInstanceType(InstanceType.M4_LARGE.toString())
				.withHadoopVersion("2.6.0")
				.withEc2KeyName("hussienkeyaws")
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
				.withName("CollocationExtraction")
				.withInstances(instances)
				.withSteps(step0)
				.withLogUri("s3n://hussiendsps2/logs/")
				.withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel("emr-5.32.0");

		RunJobFlowResult result = mapReduce.runJobFlow(runFlowRequest);
		System.out.println("JobFlow id: " + result.getJobFlowId());
	}
}
