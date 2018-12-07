package uk.co.essarsoftware.backup.upload;

import org.apache.commons.codec.binary.Hex;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.model.*;
import uk.co.essarsoftware.backup.HashUtils;
import uk.co.essarsoftware.backup.io.ChunkedInputStream;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.stream.Collectors;

public class Upload
{

    private static final int _1MB = 1048576;

    private HashUtils hash = HashUtils.createSHA256Instance();


    private byte[] calculateTreeHash(byte[] data) throws IOException {

        // Split data into 1MB chunks
        byte[][] blocks = splitBytes(data, _1MB);

        LinkedList<byte[]> ll = new LinkedList<>(Arrays.stream(blocks).collect(Collectors.toList()));

        return hash.calculateTreeHash(ll);

    }

    private static int getFileSize(Path path) {

        try {

            return (int) Files.size(path);

        } catch(IOException ioe) {

            ioe.printStackTrace(System.err);
            return 0;
        }
    }

    private static byte[] readPath(Path path) throws IOException {

        ByteBuffer bytes = ByteBuffer.allocate(getFileSize(path));

        byte[] buf = new byte[4096];

        try (FileInputStream in = new FileInputStream(path.toFile())) {

            int len = in.read(buf);
            while(len > 0) {

                bytes.put(buf, 0, len);

                len = in.read(buf);

            }

        }

        return bytes.array();

    }

    private static byte[][] splitBytes(InputStream in, int blockSize) throws IOException {

        ArrayList<byte[]> blocks = new ArrayList<>();
        byte[] buf = new byte[blockSize];

        int len = in.read(buf);
        while(len > 0) {

            blocks.add(Arrays.copyOf(buf, len));

            len = in.read(buf);

        }

        return blocks.toArray(new byte[blocks.size()][blockSize]);

    }

    private static byte[][] splitBytes(byte[] bytes, int blockSize) throws IOException {

        return splitBytes(new ByteArrayInputStream(bytes), blockSize);

    }

    void uploadFileBasic(Path path) {

        // Set a maximum of 16MB for this approach
        if(getFileSize(path) > 16 * _1MB) {

            System.err.println("File is too big!");
            System.exit(1);

        }

        String fileHash;
        try {

            byte[] bytes = readPath(path);
            fileHash = Hex.encodeHexString(calculateTreeHash(bytes));

        } catch(IOException ioe) {

            fileHash = "";

        }
        System.out.println("TreeHash: " + fileHash);

        GlacierClient glacier = GlacierClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.builder()
                    .profileName(GlacierConfiguration.awsProfile)
                    .build())
                .build();

        UploadArchiveRequest req = UploadArchiveRequest.builder()
                .vaultName(GlacierConfiguration.vaultName)
                .archiveDescription(path.toString())
                .checksum(fileHash)
                .build();

        UploadArchiveResponse rsp = glacier.uploadArchive(req, path);

        System.out.println("Uploaded; archiveId=" + rsp.archiveId());

    }

    void uploadFileMultipart(Path path, int partSize) {

        MultipartUpload upload = new MultipartUpload(path, partSize, 2);

        upload.initialise();

        upload.upload();

        upload.complete();

        System.out.println("Uploaded; archiveId=" + upload.getArchiveId());

    }

    public static void main(String[] args) {

        new Upload().uploadFileMultipart(Paths.get("example.tar"), _1MB);

    }
}
