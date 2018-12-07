package uk.co.essarsoftware.backup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.essarsoftware.backup.diff.TreeDiff;
import uk.co.essarsoftware.backup.tar.Tarball;
import uk.co.essarsoftware.backup.tar.TarballIndex;
import uk.co.essarsoftware.backup.upload.MultipartUpload;
import uk.co.essarsoftware.backup.upload.UploadResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class BackupDirectory
{

    private static final Logger _LOG = LoggerFactory.getLogger(BackupDirectory.class);

    private static final int _1MB = 1048576;

    private boolean appendFlag, overwriteFlag;
    private List<Path> changedFiles, srcPaths;
    private Path metaDirectory, tarPath;
    private Tarball tar;
    private UploadResult uploadResult;

    private BackupDirectory() {

        // Default constructor

        // Set default properties
        changedFiles = new ArrayList<>();
        srcPaths = new ArrayList<>();

        appendFlag = false;
        overwriteFlag = false;

    }

    private void calculateChanges() {

        TreeDiff diff = new TreeDiff(metaDirectory);
        srcPaths.forEach(f -> {

            _LOG.info("Checking {} for changes", f);
            changedFiles.addAll(diff.listChangedDirs(f));

        });
        _LOG.info("{} directories identified with changes", changedFiles.size());

    }

    private void createIndexFile() {

        if (uploadResult == null && tar == null) {

            // Nothing to do
            return;

        }

        Path indexFilePath = (metaDirectory == null ? tarPath.toAbsolutePath().getParent() : metaDirectory).resolve(tarPath.getFileName() + ".index");
        TarballIndex index = new TarballIndex(indexFilePath);

        if (uploadResult != null) {

            index.setArchiveId(uploadResult.getArchiveId());
            index.setChecksum(uploadResult.getChecksum());

        }

        if (tar != null) {

            index.setTarEntries(tar.getEntries());

        }

        // Write index
        try {

            index.writeIndex();
            _LOG.info("Written index to {}", indexFilePath);

        } catch (IOException ioe) {

            _LOG.warn("Unable to write index file: {} ({})", ioe.getMessage(), ioe.getClass().getName());
            _LOG.debug(ioe.getClass().getName(), ioe);

        }
    }

    private void createTarball() {

        if (changedFiles.size() == 0) {

            _LOG.info("No files selected for tarball");
            return;

        }

        try {

            if (overwriteFlag) {

                Files.deleteIfExists(tarPath);

            } else if (Files.exists(tarPath)) {

                _LOG.warn("Tarball file already exists and overwrite flag not set");
                return;

            }

            if (appendFlag && Files.exists(tarPath)) {

                tar = new Tarball(tarPath.toFile(), appendFlag, srcPaths.get(0), changedFiles);

            } else {

                tar = new Tarball(Files.createFile(tarPath).toFile(), srcPaths.get(0), changedFiles);

            }
            _LOG.info("Created archive: {}", tar);

        } catch (IOException ioe) {

            _LOG.error("Unable to create tarball", ioe);

        }
    }

    private void parseArgs(String[] args) {

        LinkedList<String> argList = new LinkedList<>(Arrays.asList(args));

        while (argList.size() > 0) {

            final String arg = argList.removeFirst();
            final int argsRemaining = argList.size();

            // Look for any switches
            if ("--meta".equals(arg)) {

                metaDirectory = Paths.get(argList.removeFirst());
                continue;

            }
            if ("-a".equals(arg) || "--append".equals(arg)) {

                appendFlag = true;
                continue;

            }
            if ("-o".equals(arg) || "--overwrite".equals(arg)) {

                overwriteFlag = true;
                continue;

            }

            if (argsRemaining >= 1) {

                // Non-last args are source
                srcPaths.add(Paths.get(arg));

            } else {

                // Last arg is tar path
                tarPath = Paths.get(arg);

            }
        }

        // Validate provided args

        if (tarPath == null) {

            throw new IllegalArgumentException("Output TAR path must be specified");

        }
    }

    private void uploadMetadata() {


    }

    private void uploadTar() {

        if(tar == null) {

            _LOG.info("No tarball to upload");
            return;

        }

        // Configure an upload client based on the file size
        MultipartUpload upload;

        try {

            if (Files.size(tarPath) > (128 * _1MB)) {

                // 8 workers, 32 MB chunks
                upload = new MultipartUpload(tarPath, (32 * _1MB), 8);

            } else if(Files.size(tarPath) > (16 * _1MB)) {

                // 4 workers, 8 MB chunks
                upload = new MultipartUpload(tarPath, (8 * _1MB), 4);

            } else {

                // Single thread
                upload = new MultipartUpload(tarPath, (8 * _1MB), 2);

            }

        } catch (IOException ioe) {

            _LOG.warn("Unable to determine tarball size, using default upload configuration");
            upload = new MultipartUpload(tarPath, (8 * _1MB), 1);

        }

        upload.initialise();
        upload.upload();
        upload.complete();

        uploadResult = upload;

        _LOG.info("Uploaded archive: {}", upload.getArchiveId());

    }


    public static void main(String[] args) {

        BackupDirectory script = new BackupDirectory();

        try {

            script.parseArgs(args);

        } catch(IllegalArgumentException iae) {

            _LOG.error(iae.getMessage());
            System.exit(1);

        }

        script.calculateChanges();
        script.createTarball();
        script.uploadTar();
        script.createIndexFile();

        // Back up the tab and indexes
        //script.uploadMetadata();

    }
}
