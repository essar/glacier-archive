package uk.co.essarsoftware.backup.upload;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.model.UploadArchiveRequest;
import software.amazon.awssdk.services.glacier.model.UploadArchiveResponse;
import uk.co.essarsoftware.backup.HashUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class SimpleUpload implements UploadResult
{

    private static final Logger _LOG = LoggerFactory.getLogger(SimpleUpload.class);

    private static final int _1MB = 1048576;

    private final GlacierClient glacier;
    private Path path;
    private String archiveId, checksum;

    private HashUtils hash = HashUtils.createSHA256Instance();

    public SimpleUpload(Path path) {

        this.path = path;
        _LOG.info("Uploading {}", path.toAbsolutePath());

        glacier = GlacierClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.builder()
                        .profileName(GlacierConfiguration.awsProfile)
                        .build())
                .build();

        _LOG.debug("Initialised Glacier client: {}", glacier);

    }

    private static int getFileSize(Path path) {

        try {

            return (int) Files.size(path);

        } catch (IOException ioe) {

            _LOG.warn("Error reading file size: {}", ioe.getMessage());
            return 0;

        }
    }

    private static byte[] readPath(Path path) throws IOException {

        ByteBuffer bytes = ByteBuffer.allocate(getFileSize(path));

        byte[] buf = new byte[4096];

        try (FileInputStream in = new FileInputStream(path.toFile())) {

            int len = in.read(buf);
            while (len > 0) {

                bytes.put(buf, 0, len);
                len = in.read(buf);

            }
        }

        return bytes.array();

    }

    private String calculateTreeHash(Path path) {

        try {

            byte[] bytes = readPath(path);
            return Hex.encodeHexString(hash.calculateTreeHash(Arrays.asList(bytes)));

        } catch(IOException ioe) {

            _LOG.error("Error calculating treeHash", ioe);
            return HashUtils.DEFAULT_HASH;

        }
    }

    public void upload() {

        // Warn if the file is bigger than 16MB
        if(getFileSize(path) > 16 * _1MB) {

            _LOG.warn("File is big! Use MultipartUpload instead");

        }

        String fileHash = calculateTreeHash(path);
        _LOG.debug("fileHash: {}", fileHash);

        UploadArchiveRequest req = UploadArchiveRequest.builder()
                .vaultName(GlacierConfiguration.vaultName)
                .archiveDescription(path.toString())
                .checksum(fileHash)
                .build();

        UploadArchiveResponse rsp = glacier.uploadArchive(req, path);
        archiveId = rsp.archiveId();
        checksum = rsp.checksum();

        _LOG.info("Completed upload");
        _LOG.info("Created archive: {}; checksum: {}", archiveId, checksum);

    }

    @Override
    public String getArchiveId() {

        return archiveId;

    }

    @Override
    public String getChecksum() {

        return checksum;

    }
}
