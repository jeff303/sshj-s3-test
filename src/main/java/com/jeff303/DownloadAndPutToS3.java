package com.jeff303;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.google.common.base.Strings;
import net.schmizz.sshj.DefaultConfig;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.RemoteFile;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.compression.DelayedZlibCompression;
import net.schmizz.sshj.transport.compression.NoneCompression;
import net.schmizz.sshj.transport.compression.ZlibCompression;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;

public class DownloadAndPutToS3 {
  private static final Logger LOGGER = LoggerFactory.getLogger(DownloadAndPutToS3.class);

  public static void main(String[] args) throws IOException, URISyntaxException {

    final Properties properties = new Properties();
    properties.load(DownloadAndPutToS3.class.getResourceAsStream("app.properties"));

    final DefaultConfig sshConfig = new DefaultConfig();
    sshConfig.setCompressionFactories(Arrays.asList(
        new DelayedZlibCompression.Factory(),
        new ZlibCompression.Factory(),
        new NoneCompression.Factory()));

    final SSHClient sshClient = new SSHClient(sshConfig);
    sshClient.addHostKeyVerifier(new PromiscuousVerifier());

    final URI serverURI = new URI(properties.getProperty("sftp.serverUrl"));
    sshClient.connect(serverURI.getHost(), serverURI.getPort());
    final String userName = properties.getProperty("sftp.username");
    final String password = properties.getProperty("sftp.password");
    if (!Strings.isNullOrEmpty(userName)) {
      sshClient.authPassword(userName, password);
    }

    final SFTPClient sftpClient = sshClient.newSFTPClient();
    final String fileName = properties.getProperty("sftp.fileName");
    final RemoteFile remoteFile = sftpClient.open(fileName);
    final RemoteFile.ReadAheadRemoteFileInputStream inputStream = remoteFile
        .new ReadAheadRemoteFileInputStream(64);

    final TransferManager transferManager = createS3TransferManager(properties);

    final String bucketName = properties.getProperty("s3.bucketName");
    final ObjectMetadata metadata = new ObjectMetadata();
    if (System.getenv("SET_S3_CONTENT_LENGTH") != null) {
      metadata.setContentLength(remoteFile.length());
    }
    final PutObjectRequest putObjectRequest = new PutObjectRequest(
        bucketName,
        fileName,
        inputStream,
        metadata
    );

    try {
      final Upload upload = transferManager.upload(putObjectRequest);
      final String objectName = bucketName + "/" + fileName;
      upload.addProgressListener((ProgressListener) progressEvent -> {
        switch (progressEvent.getEventType()) {
          case TRANSFER_STARTED_EVENT:
            LOGGER.debug("Started uploading object {} into Amazon S3", objectName);
            break;
          case TRANSFER_COMPLETED_EVENT:
            LOGGER.debug("Completed uploading object {} into Amazon S3", objectName);
            break;
          case TRANSFER_FAILED_EVENT:
            LOGGER.debug("Failed uploading object {} into Amazon S3", objectName);
            break;
          default:
            break;
        }
      });
      final UploadResult result = upload.waitForUploadResult();
      LOGGER.info(
          "Upload result: bucket {}, etag {}, key {}",
          result.getBucketName(),
          result.getETag(),
          result.getKey()
      );
    } catch (Exception e) {
      LOGGER.error("{} during S3 upload: {}", e.getClass().getSimpleName(), e.getMessage(), e);
    } finally {
      transferManager.shutdownNow(true);
      sshClient.close();
    }
  }

  private static TransferManager createS3TransferManager(Properties properties) {
    final AWSStaticCredentialsProvider credentials = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(
            properties.getProperty("s3.accessKey.id"),
            properties.getProperty("s3.accessKey.secret")
        )
    );

    final ClientConfiguration clientConfig = new ClientConfiguration();
    final AmazonS3ClientBuilder builder = AmazonS3ClientBuilder
        .standard()
        .withCredentials(credentials)
        .withClientConfiguration(clientConfig)
        .withChunkedEncodingDisabled(true)
        .withPathStyleAccessEnabled(true)
        .withRegion(properties.getProperty("s3.region"));

    return TransferManagerBuilder
        .standard()
        .withS3Client(builder.build())
        .withExecutorFactory(() -> Executors.newCachedThreadPool())
        .withMinimumUploadPartSize(5242880l)
        .withMultipartUploadThreshold(268435456l)
        .build();
  }


}
