package uk.co.essarsoftware.backup.tar;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A collection of files compressed into a tarball archive.
 * @author <steve.roberts/>
 */
public class Tarball
{

    private static final Logger _LOG = LoggerFactory.getLogger(Tarball.class);

    private final File tarFile;

    public Tarball(File tarFile, Path root, List<Path> paths) throws IOException {

        this(tarFile, false, root, paths);

    }

    public Tarball(File tarFile, boolean append, Path root, List<Path> paths) throws IOException {

        this.tarFile = tarFile;

        try (TarArchiveOutputStream out = new TarArchiveOutputStream(new FileOutputStream(tarFile, append))) {

            out.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

            for(Path p : paths) {

                if (Files.isDirectory(p)) {

                    Files.list(p).forEach(f -> addFile(out, f, root));

                } else {

                    addFile(out, p, root);

                }
            }
        }
    }

    public Tarball(File tarFile, Path root, Path... paths) throws IOException {

        this(tarFile, root, Arrays.asList(paths));

    }

    private static String getEntryName(Path path, Path root) {

        if(root != null) {

            return root.relativize(path).toString();

        }

        return path.toString();

    }

    private void addFile(TarArchiveOutputStream out, Path path, Path root) {

        _LOG.debug("Adding {}", path);

        try {

            String name = getEntryName(path, root);
            _LOG.debug("entryName: {}", name);
            File f = path.toFile();

            if (f.getAbsoluteFile().equals(tarFile.getAbsoluteFile())) {

                _LOG.debug("Not adding self to tar, skipping");

            } else {

                if (f.isFile()) {

                    ArchiveEntry entry = out.createArchiveEntry(f, name);
                    out.putArchiveEntry(entry);

                    try (FileInputStream in = new FileInputStream(f)) {

                        byte[] buf = new byte[4096];
                        int len = in.read(buf);

                        while (len > 0) {

                            out.write(buf, 0, len);
                            len = in.read(buf);

                        }

                        _LOG.debug("entrySize: {} bytes", entry.getSize());

                    }

                    out.closeArchiveEntry();

                }
            }

        } catch (IOException ioe) {

            _LOG.warn("Unable to add {} to tarball", path.getFileName(), ioe.getMessage());
            _LOG.debug(ioe.getClass().getName(), ioe);

        }
    }

    private int getEntriesCount() {

        int entryCount = 0;
        try (TarArchiveInputStream in = new TarArchiveInputStream(new FileInputStream(tarFile))) {

            while (in.getNextTarEntry() != null) {

                entryCount ++;

            }

            return entryCount;

        } catch (IOException ioe) {

            _LOG.warn("Error getting entries count: {}", ioe.getMessage());
            return 0;

        }
    }

    private File getTarFile() {

        return tarFile;

    }

    public List<String> getEntries() {

        ArrayList<String> entryNames = new ArrayList<>();

        try (TarArchiveInputStream in = new TarArchiveInputStream(new FileInputStream(tarFile))) {

            TarArchiveEntry entry = in.getNextTarEntry();
            while (entry != null) {

                if(entry.isFile()) {

                    entryNames.add(entry.getName());

                }
                entry = in.getNextTarEntry();

            }

        } catch (IOException ioe) {

            _LOG.error("Unable to read {}", tarFile, ioe);

        }

        return entryNames;

    }

    @Override
    public String toString() {

        return String.format("%s (%d entries)", getTarFile().getName(), getEntriesCount());

    }
}
