package uk.co.essarsoftware.backup.diff;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.essarsoftware.backup.HashUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TreeDiff
{

    private static Logger _LOG = LoggerFactory.getLogger(TreeDiff.class);

    private HashTab hashTab = new HashTab();
    private HashTab newHashTab = new HashTab();
    private Path metaDir;

    private HashUtils hash = HashUtils.createSHA1Instance();

    public TreeDiff() {

        // Default constructor
        this(null);

    }

    public TreeDiff(Path metaDir) {

        this.metaDir = metaDir;
        if (metaDir != null) {

            try {

                if (!Files.exists(metaDir)) {

                    Files.createDirectory(metaDir);

                }

            } catch (IOException ioe) {

                _LOG.error("Unable to create metadata directory", ioe);

            }
        }
    }

    private static String getFileModificationString(Path p) throws IOException {

        return String.format("%s|%d|%d", p.getFileName(), Files.size(p), Files.getLastModifiedTime(p).toMillis());

    }

    /**
     * Calculate a modification hash for a file or directory based on size and mtime.
     * @param path If file is a file, return its hash. If file is a directory, return a hash based on its contents.
     * @return a hex string containing the SHA-1 hash of the file metadata.
     * @throws IOException if the file metadata cannot be read.
     */
    private String calculateHash(Path path) throws IOException {

        // Path represents a file
        if (Files.isRegularFile(path)) {

            return hash.calculateHashString(getFileModificationString(path));

        }

        if (Files.isDirectory(path)) {

            StringBuffer buf = new StringBuffer();

            // Get the modification string for each file in the directory
            Files.list(path).filter(Files::isRegularFile).forEach(p -> {

                try {

                    // Ignore the hashtab file
                    if(!HashTab.TABFILE.equals(p.getFileName().toString())) {

                        buf.append(getFileModificationString(p));

                    }

                } catch (IOException ioe) {

                    _LOG.warn("Ignoring {}: {}", p, ioe.getMessage());

                }
            });

            return buf.length() == 0 ? HashUtils.DEFAULT_HASH : hash.calculateHashString(new String(buf));

        }

        // Default response, in case file does not exist
        return HashUtils.DEFAULT_HASH;

    }

    private boolean hasDirChanged(Path path, String hash) {

        // Compare with the hash from the hashTab
        boolean changed = hashTab.checkHash(path, hash);

        // Save the hash to the new hashtab
        newHashTab.putHash(path, hash);

        return changed;

    }

    private Path getTabFile(Path root) {

        if(metaDir == null) {

            return root.resolve(HashTab.TABFILE);

        }

        return metaDir.resolve(root).resolve(HashTab.TABFILE);

    }

    public List<Path> listChangedDirs(Path root) {

        if (root == null || !Files.isDirectory(root)) {

            throw new IllegalArgumentException("Provided argument must be a directory");

        }

        ArrayList<Path> changedDirs = new ArrayList<>();

        // Load the hashTab from the directory
        hashTab.load(getTabFile(root));

        try {

            // Check the hash of the root directory followed by all subdirectories recursively.
            Files.walk(root).filter(Files::isDirectory).forEach(p -> {

                Path rp = root.relativize(p);

                try {

                    // Calculate the hash of the directory
                    String hash = calculateHash(p);

                    if (hasDirChanged(rp, hash)) {

                        changedDirs.add(p);

                    }

                } catch (IOException ioe) {

                    _LOG.warn("Unable to check for changes in {}: {}", rp, ioe.getMessage());

                }
            });

        } catch (IOException ioe) {

            _LOG.error("Error checking directory for changes", ioe);

        }

        // Save the changed hashes
        newHashTab.save(getTabFile(root));

        // Add the tab file only if there are changes
        if (changedDirs.size() > 0) {

            changedDirs.add(getTabFile(root));

        }

        return changedDirs;

    }

    public static void main(String[] args) {

        TreeDiff diff = new TreeDiff(Paths.get("meta"));
        List<Path> changedDirs = diff.listChangedDirs(Paths.get("src"));
        changedDirs.forEach(System.out::println);

    }

    static class HashTab
    {

        private final HashMap<String, String> entries = new HashMap<>();
        static final String TABFILE = ".hashTab";


        void load(Path tabFile) {

            try {

                if (!Files.exists(tabFile)) {

                    // Just ignore
                    return;

                }

                try (BufferedReader r = new BufferedReader(new FileReader(tabFile.toFile()))) {

                    int lineNumber = 0;
                    String line = r.readLine();
                    while (line != null) {

                        lineNumber++;

                        String[] parts = line.split("\\s+");
                        if (parts.length == 2) {

                            entries.put(parts[0], parts[1]);

                        } else {

                            _LOG.warn("HashTab: Ignoring invalid line {} in hashtab", lineNumber);

                        }

                        line = r.readLine();

                    }
                }

            } catch (IOException ioe) {

                _LOG.error("Error loading hashtab", ioe);

            }
        }

        void save(Path tabFile) {

            try {

                Files.createDirectories(tabFile.getParent());

                try (PrintWriter w = new PrintWriter(new FileWriter(tabFile.toFile()))) {

                    entries.forEach((k, v) -> w.println(String.format("%s %s", k, v)));

                }

            } catch (IOException ioe) {

                _LOG.error("Error saving hashtab", ioe);

            }
        }

        boolean checkHash(Path p, String hash) {

            return !hash.equals(getHash(p));

        }

        String getHash(Path path) {

            return getHash(path.toString());

        }

        String getHash(String fileName) {

            if(entries.containsKey(fileName)) {

                return entries.get(fileName);

            }

            return HashUtils.DEFAULT_HASH;

        }

        void putHash(Path path, String hash) {

            putHash(path.toString(), hash);

        }

        void putHash(String fileName, String hash) {

            if(!HashUtils.DEFAULT_HASH.equals(hash)) {

                entries.put(fileName, hash);

            }
        }
    }
}
