package com.pablodc.kinesispoc.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProviderChain;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.profiles.ProfileFileSupplier;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.kinesis.model.Record;
import software.amazon.awssdk.services.kinesis.model.Shard;
import software.amazon.awssdk.services.sts.StsAsyncClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

import javax.annotation.PostConstruct;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Service
public class KinesisReader {

    @Value("${streamname}")
    private String streamName;

    @Value("${awscredentialpath}")
    private String awsCredentialPath;

    @Value("${rolearn}")
    private String roleArn;

    @Value("${profile}")
    private String profile;

    private AwsCredentialsProvider awsCredentialsProvider;

    private final Region region = Region.US_WEST_2;
    private KinesisClient kinesisClient;

    @PostConstruct
    private void init() {
        awsCredentialsProvider = getAwsCredentialsProvider();

        kinesisClient = KinesisClient.builder()
                .region(region)
                .credentialsProvider(awsCredentialsProvider)
                .build();
    }

    private AwsCredentialsProvider getAwsCredentialsProvider() {
        Path credentialsFilePath = Path.of(awsCredentialPath);

        AwsCredentialsProvider awsCredentialsProviderLocal = ProfileCredentialsProvider
                .builder()
                .profileFile(ProfileFileSupplier.reloadWhenModified(credentialsFilePath, ProfileFile.Type.CREDENTIALS))
                .profileName(profile)
                .build();

        StsAsyncClient stsAsyncClient = StsAsyncClient
                .builder()
                .credentialsProvider(awsCredentialsProviderLocal)
                .region(region)
                .build();

        AssumeRoleRequest assumeRoleRequest = AssumeRoleRequest
                .builder()
                .durationSeconds(3600)
                .roleArn(roleArn)
                .roleSessionName("Kinesis_Session")
                .build();

        Future<AssumeRoleResponse> responseFuture = stsAsyncClient.assumeRole(assumeRoleRequest);
        AssumeRoleResponse response = null;
        try {
            response = responseFuture.get();
        } catch (InterruptedException | ExecutionException ex) {
            throw new RuntimeException(ex);
        }
        Credentials creds = response.credentials();
        AwsSessionCredentials sessionCredentials =
                AwsSessionCredentials.create(creds.accessKeyId(), creds.secretAccessKey(), creds.sessionToken());

        return AwsCredentialsProviderChain.builder()
                .credentialsProviders(StaticCredentialsProvider.create(sessionCredentials))
                .build();
    }

    @Scheduled(fixedDelay = 1000)
    public void readStreamRecords() {
        String shardIterator;
        String lastShardId = null;

        DescribeStreamRequest describeStreamRequest = DescribeStreamRequest.builder()
                .streamName(streamName)
                .build();

        List<Shard> shards = new ArrayList<>();
        DescribeStreamResponse streamRes;
        do {
            streamRes = kinesisClient.describeStream(describeStreamRequest);
            shards.addAll(streamRes.streamDescription().shards());
            if (shards.size() > 0) {
                lastShardId = shards.get(shards.size() - 1).shardId();
            }
        } while (streamRes.streamDescription().hasMoreShards());

        GetShardIteratorRequest itReq = GetShardIteratorRequest.builder()
                .streamName("poc-event-bus-stream")
                .shardIteratorType("TRIM_HORIZON")
                .shardId(lastShardId)
                .build();

        GetShardIteratorResponse shardIteratorResult = kinesisClient.getShardIterator(itReq);
        shardIterator = shardIteratorResult.shardIterator();

        List<Record> records;
        GetRecordsRequest recordsRequest = GetRecordsRequest.builder()
                .shardIterator(shardIterator)
                .limit(1000)
                .build();

        GetRecordsResponse result = kinesisClient.getRecords(recordsRequest);
        records = result.records();

        for (Record record : records) {
            SdkBytes byteBuffer = record.data();
            System.out.printf("Seq No: %s - PartitionKey: %s - %s%n",
                    record.sequenceNumber(),
                    record.partitionKey(),
                    new String(byteBuffer.asByteArray()));
        }


    }
}
