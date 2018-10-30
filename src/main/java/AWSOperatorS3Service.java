import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

/**
 * The Class AWSOperatorS3Service.
 *
 * @author bingo
 * @reference by: https://github.com/awsdocs/aws-doc-sdk-examples
 */
@Slf4j
public class AWSOperatorS3Service {

    /**
     * service handler
     */
    private static AWSOperatorS3Service INSTANCE;

    /**
     * AWS S3 client, all s3 operations are handled by this client
     */
    private static AmazonS3 s3Client;

    /**
     * AWS S3 bucket name
     */
    private static String bucketName;

    /**
     * private constructor
     */
    private AWSOperatorS3Service() {}

    /**
     * @param accessKey
     * @param secretKey
     * @param s3Region
     * @param s3BucketName
     * @return AWSOperatorS3Service INSTANCE
     */
    public static AWSOperatorS3Service build(String accessKey, String secretKey, String s3Region, String s3BucketName) {
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        s3Client = AmazonS3ClientBuilder.standard().withRegion(s3Region).withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();
        bucketName = s3BucketName;
        INSTANCE = new AWSOperatorS3Service();
        return INSTANCE;
    }

    /**
     * upload local file to s3
     * automatically create s3 path if not exists
     * @param localFile
     * @param s3File
     * @param overwrite boolean, overwrite if s3 file exists
     * @return true if uploaded, else false when upload err or s3 file already exists when overwrite == false
     */
    public boolean uploadFile(String localFile, String s3File, boolean overwrite) {
        boolean s3FileExists = isS3FileExists(s3File);
        if (overwrite || (!overwrite && !s3FileExists)) {
            try {
                s3Client.putObject(bucketName, s3File, new File(localFile));
            } catch (AmazonServiceException e) {
                log.error("upload file " + localFile + " error", e);
                return false;
            } catch (SdkClientException e) {
                log.error("upload file " + localFile + " error", e);
                return false;
            }
            return true;
        } else {
            // return false when s3 file exists but not overwrite
            return false;
        }

    }

    /**
     * Delete S3 file
     * Delete not exists file will cause err
     * @param s3File
     * @return successful
     */
    public boolean deleteS3File(String s3File) {
        S3Object s3FileObject = s3Client.getObject(bucketName, s3File);
        DeleteObjectRequest deleteObjectRequest = new DeleteObjectRequest(bucketName, s3FileObject.getKey());
        try {
            s3Client.deleteObject(deleteObjectRequest);
        } catch (AmazonServiceException e) {
            log.error("delete file " + s3File + " error", e);
            return false;
        } catch (SdkClientException e) {
            log.error("delete file " + s3File + " error", e);
            return false;
        }
        return true;
    }

    /**
     * Copy S3 file from s3 source folder to s3 dest folder
     * If source file not exists, will alert error
     * If target file exists, will overwrite
     * @param s3SourceFile source file
     * @param s3DestFile   dest file
     * @return successful
     */
    public boolean copyS3File(String s3SourceFile, String s3DestFile, boolean overwrite) {
        boolean s3FileExists = isS3FileExists(s3DestFile);
        if (overwrite || (!overwrite && !s3FileExists)) {
            S3Object s3SourceFileObject = s3Client.getObject(bucketName, s3SourceFile);
            CopyObjectRequest copyObjRequest = new CopyObjectRequest(bucketName, s3SourceFileObject.getKey(), bucketName, s3DestFile);
            try {
                CopyObjectResult result = s3Client.copyObject(copyObjRequest);
                if (result != null)
                    return true;
                return false;
            } catch (AmazonServiceException e) {
                log.error("copy file " + s3SourceFile + "to " + s3SourceFile + " error", e);
                return false;
            } catch (SdkClientException e) {
                log.error("copy file " + s3SourceFile + "to " + s3SourceFile + " error", e);
                return false;
            }
        } else {
            // return false when target s3 file exists but not overwrite
            return false;
        }

    }

    /**
     * Delete S3 folder
     * @param s3Folder
     * @return successful even folder not exists
     */
    public boolean deleteS3Folder(String s3Folder){
        try {
            //delete folder children
            List<S3ObjectSummary> fileList = s3Client.listObjectsV2(bucketName, s3Folder).getObjectSummaries();
            for (S3ObjectSummary file : fileList) {
                s3Client.deleteObject(bucketName, file.getKey());
            }
            //delete folder
            s3Client.deleteObject(bucketName, s3Folder);
        } catch (AmazonServiceException e) {
            log.error("delete folder " + s3Folder + " error", e);
            return false;
        } catch (SdkClientException e) {
            log.error("delete folder " + s3Folder + " error", e);
            return false;
        }
        return true;
    }

    // TODO: not complete
    public boolean copyFolder(String s3SourceFolder, String s3DestFolder){
        try {
            List<S3ObjectSummary> fileList = s3Client.listObjectsV2(bucketName, s3SourceFolder).getObjectSummaries();
            for (S3ObjectSummary file : fileList) {
                String newFileName = Paths.get(file.getKey()).getFileName().toString();
                s3Client.copyObject(bucketName, file.getKey(), bucketName, s3DestFolder + "/" + newFileName);
            }
        } catch (AmazonServiceException e) {
            log.error("copy folder " + s3SourceFolder + " error", e);
            return false;
        } catch (SdkClientException e) {
            log.error("copy folder " + s3SourceFolder + " error", e);
            return false;
        }
        return true;
    }

    /**
     * check target S3 file exists
     * @param s3File
     * @return turn if exists
     */
    public boolean isS3FileExists(String s3File) {
        try {
            s3Client.getObject(bucketName, s3File);
        } catch (AmazonServiceException e) {
            return false;
        }
        return true;

    }

}
