package uk.co.essarsoftware.backup.tar;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class TarballIndex
{

    private List<String> tarEntries = new ArrayList<>();
    private String archiveId, checksum;

    private final Path indexFilePath;

    public TarballIndex(Path indexFilePath) {

        this.indexFilePath = indexFilePath;

    }

    public void setArchiveId(String archiveId) {

        this.archiveId = archiveId;

    }

    public void setChecksum(String checksum) {

        this.checksum = checksum;

    }

    public void setTarEntries(List<String> tarEntries) {

        this.tarEntries.clear();
        this.tarEntries.addAll(tarEntries);

    }

    public void writeIndex() throws IOException {

        // Open upload output
        try (PrintWriter out = new PrintWriter(new FileOutputStream(indexFilePath.toFile()))) {

            out.println(LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
            if(archiveId != null) {

                out.println(String.format("ArchiveId: %s", archiveId));

            }
            if(checksum != null) {

                out.println(String.format("Checksum: %s", checksum));

            }

            if (tarEntries.size() > 0) {

                out.println("---");
                tarEntries.forEach(out::println);

            }
        }
    }
}
