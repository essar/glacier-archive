package uk.co.essarsoftware.backup.upload;

import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.glacier.GlacierClient;
import software.amazon.awssdk.services.glacier.model.*;
import uk.co.essarsoftware.backup.HashUtils;
import uk.co.essarsoftware.backup.io.ChunkedInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultipartUpload implements UploadResult
{

    private static final Logger _LOG = LoggerFactory.getLogger(MultipartUpload.class);

    private static final int _1MB = 1048576;

    private final GlacierClient glacier;
    private final ExecutorService service;
    private final PartTree parts = new PartTree();

    private int chunkSize, uploadThreads;
    private Path path;
    private String archiveId, checksum, uploadId;

    private HashUtils hash = HashUtils.createSHA256Instance();

    public MultipartUpload(Path path, int chunkSize, int uploadThreads) {

        this.path = path;
        this.chunkSize = chunkSize;
        this.uploadThreads = uploadThreads;

        _LOG.info("Uploading {}; {} bytes in {}MB chunks using {} workers", path.toAbsolutePath(), getFileSize(path), (chunkSize / _1MB), uploadThreads);

        glacier = GlacierClient.builder()
                .credentialsProvider(ProfileCredentialsProvider.builder()
                        .profileName(GlacierConfiguration.awsProfile)
                        .build())
                .build();

        _LOG.debug("Initialised Glacier client: {}", glacier);

        service = Executors.newFixedThreadPool(uploadThreads);

    }

    private static int getFileSize(Path path) {

        try {

            return (int) Files.size(path);

        } catch (IOException ioe) {

            _LOG.warn("Error reading file size for {}: {}", path, ioe.getMessage());
            return 0;

        }
    }

    public void abort() {

        if (uploadId != null) {

            AbortMultipartUploadRequest abReq = AbortMultipartUploadRequest.builder()
                    .uploadId(uploadId)
                    .vaultName(GlacierConfiguration.vaultName)
                    .build();

            glacier.abortMultipartUpload(abReq);

            _LOG.warn("Aborted upload: {}", uploadId);

        }
    }

    public void complete() {

        try {

            _LOG.debug("Calculating archive has from {} parts", parts.size());

            byte[] archiveHash = hash.calculateTreeHash(parts);
            String archiveHashStr = Hex.encodeHexString(archiveHash);
            int archiveSize = getFileSize(path);

            _LOG.debug("archiveHash: {}", archiveHashStr);

            CompleteMultipartUploadRequest cpReq = CompleteMultipartUploadRequest.builder()
                    .uploadId(uploadId)
                    .vaultName(GlacierConfiguration.vaultName)
                    .archiveSize(Integer.toString(archiveSize))
                    .checksum(archiveHashStr)
                    .build();

            CompleteMultipartUploadResponse cpRsp = glacier.completeMultipartUpload(cpReq);
            archiveId = cpRsp.archiveId();
            checksum = cpRsp.checksum();

            _LOG.info("Completed upload: {}", uploadId);

        }
        catch (Exception e) {

            _LOG.error("Upload failed", e);

            service.shutdown();
            abort();

        }
    }

    public void initialise() {

        InitiateMultipartUploadRequest initReq = InitiateMultipartUploadRequest.builder()
                .vaultName(GlacierConfiguration.vaultName)
                .archiveDescription(path.toString())
                .partSize(Integer.toString(chunkSize))
                .build();

        InitiateMultipartUploadResponse initRsp = glacier.initiateMultipartUpload(initReq);

        uploadId = initRsp.uploadId();

        _LOG.info("Starting upload: {}", uploadId);

    }

    public void upload() {

        try (ChunkedInputStream in = new ChunkedInputStream(new FileInputStream(path.toFile()), chunkSize)) {

            // Create a task for each thread
            synchronized (service) {

                _LOG.debug("Creating {} upload threads", uploadThreads);

                for (int i = 0; i < uploadThreads; i++) {

                    MultipartUploadTask w = new MultipartUploadTask(in);
                    service.submit(w);

                }
            }

            // Wait for service to shutdown
            synchronized (service) {

                long waitMillis = 500L;
                while (!service.isTerminated()) {

                    _LOG.debug("Upload service running, {} parts completed", parts.size());

                    // Wait and backoff
                    service.wait(Math.min((waitMillis *= 1.5), 10000L));

                }

                _LOG.debug("Upload service finished, {} parts completed", parts.size());

            }

        } catch (Exception e) {

            _LOG.error("Upload failed", e);

            service.shutdown();
            abort();

        }
    }

    @Override
    public String getArchiveId() {

        return archiveId;

    }

    @Override
    public String getChecksum() {

        return checksum;

    }

    class MultipartUploadTask implements Runnable {

        private final ChunkedInputStream in;
        private HashUtils hash = HashUtils.createSHA256Instance();

        MultipartUploadTask(ChunkedInputStream in) {

            this.in = in;
            _LOG.debug("Initialized new upload worker");

        }

        @Override
        public void run() {

            byte[] part;
            final MultipartObject obj = new MultipartObject();

            try {

                synchronized (in) {

                    _LOG.debug("In MUT sync block");

                    part = obj.initializeAndReadBytes(in);

                    _LOG.debug("Leaving MUT sync block");

                }

            } catch (IOException ioe) {

                // Can't read input, probably not going to recover from this
                _LOG.error("Unable to read input stream", ioe);
                return;

            }

            int retryCount = 3;

            while (retryCount > 0) {

                try {

                    if (obj.getLength() > 0) {

                        obj.setUploadStartTimestamp(System.currentTimeMillis());

                        _LOG.debug("Uploading {}; {} bytes", obj.getUploadRange(), obj.getLength());

                        byte[] partHash = hash.calculateTreeHash(new ChunkedInputStream(part, _1MB));
                        String partHashStr = Hex.encodeHexString(partHash);
                        obj.setTreeHash(partHash);

                        _LOG.debug("partHash: {}", partHashStr);

                        // Upload the part
                        UploadMultipartPartRequest upReq = UploadMultipartPartRequest.builder()
                                .uploadId(uploadId)
                                .vaultName(GlacierConfiguration.vaultName)
                                .range(obj.getUploadRange())
                                .checksum(partHashStr)
                                .build();

                        UploadMultipartPartResponse upRsp = glacier.uploadMultipartPart(upReq, RequestBody.fromBytes(part));
                        String partChecksum = upRsp.checksum();

                        _LOG.debug("partChecksum: {}", partChecksum);

                        if (!partChecksum.equals(partHashStr)) {

                            _LOG.warn("Returned checksum does not match locally calculated hash, received:{}; expected:{}", partChecksum, partHashStr);

                        }

                        obj.setEndTimestamp(System.currentTimeMillis());
                        _LOG.info("Part upload complete: uploaded {}", obj);

                        parts.add(obj);

                    }

                    synchronized (service) {

                        if (!service.isShutdown()) {

                            if (obj.getLength() == 0) {

                                _LOG.debug("No data left, stopping upload service");

                                // Once there's no data to read then shutdown the executor service
                                service.shutdown();

                            } else {

                                _LOG.debug("{} bytes read, looking for next part", obj.getLength());

                                // Recursively continue
                                MultipartUploadTask w = new MultipartUploadTask(in);
                                service.submit(w);

                            }

                            // Notify the service that we completed
                            service.notifyAll();

                        }
                    }

                    // Stop process
                    retryCount = 0;

                } catch (Exception e) {

                    _LOG.error("Part upload failed", e);

                    retryCount --;

                    if (retryCount <= 0) {

                        // Giving up totally.
                        _LOG.error("Too many upload failures, giving up");

                        // Shutdown other threads and send abort
                        service.shutdown();
                        abort();

                        return;

                    }

                    _LOG.info("Retrying...");

                    // Wait a little bit then retry
                    try {

                        Thread.sleep(5000L);

                    } catch (InterruptedException ie) {

                        _LOG.warn("Thread sleep interrupted, aborting");
                        retryCount = 0;

                    }
                }
            }
        }
    }
}
