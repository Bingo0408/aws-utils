import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.DEFAULT)
public class AWSOperatorS3ServiceTest {

    private AWSOperatorS3Service awsOperatorS3Service;

    private String accessKey = "accessKey";

    private String secretKey = "secretKey";

    private String s3Region = "s3Region";

    private String s3BucketName = "s3BucketName";

    @Before
    public void setup() {
        awsOperatorS3Service = AWSOperatorS3Service.build(accessKey, secretKey, s3Region, s3BucketName);
    }

    @Test
    public void testUploadFile() {
        String fileRealPath = "/Users/gubin/Desktop/test.csv";
        String s3FilePath = "test/test.csv";
        assert(awsOperatorS3Service.uploadFile(fileRealPath, s3FilePath, true));
        // target s3 file already exists, will return false if not overwrite
        assert(!awsOperatorS3Service.uploadFile(fileRealPath, s3FilePath, false));
    }

    @Test
    public void testCopyS3Folder() {
        String folder1 = "test";
        String folder2 = "test1";
        assert(awsOperatorS3Service.copyFolder(folder1, folder2));
    }

    @Test
    public void testCopyS3File() {
        String from = "test/test.csv";
        String to = "test1.csv";
        assert(awsOperatorS3Service.copyS3File(from, to, true));
        // target s3 file already exists, will return false if not overwrite
        assert(!awsOperatorS3Service.copyS3File(from, to, false));
    }

    @Test
    public void testDeleteS3File() {
        String s3FilePath1 = "test/test.csv";
        String s3FilePath2 = "test1.csv";
        assert(awsOperatorS3Service.deleteS3File(s3FilePath1));
        assert(awsOperatorS3Service.deleteS3File(s3FilePath2));
    }

    @Test
    public void testDeleteS3Folder() {
        String s3FilePath1 = "test/test.csv";
        String s3FilePath2 = "test1.csv";
        String s3FilePath3 = "test";
        assert(awsOperatorS3Service.deleteS3Folder(s3FilePath1));
        assert(awsOperatorS3Service.deleteS3Folder(s3FilePath2));
        assert(awsOperatorS3Service.deleteS3Folder(s3FilePath3));
    }
}
