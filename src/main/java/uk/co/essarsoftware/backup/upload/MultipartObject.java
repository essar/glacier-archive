package uk.co.essarsoftware.backup.upload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.co.essarsoftware.backup.io.ChunkedInputStream;

import java.io.IOException;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Wrapper class used for holding multipart object metadata.
 * @author <steve.roberts/>
 */
class MultipartObject
{

    private static final Logger _LOG = LoggerFactory.getLogger(MultipartUpload.class);

    private byte[] treeHash;
    private int length;
    private long endTimestamp, processStartTimestamp, uploadStartTimestamp;
    private String uploadRange;


    /**
     * Calculates the duration of the upload based on start and end timestamps.
     * @return a long containing the duration in milliseconds.
     */
    private long getUploadDuration() {

        return (uploadStartTimestamp == 0 || endTimestamp == 0) ? 0 : (endTimestamp - uploadStartTimestamp);

    }

    /**
     * Calculates the speed of the upload.
     * @return a float containing the average upload speed in bytes per second.
     */
    private float getUploadSpeed() {

        return (length / (getUploadDuration() / 1000f));

    }

    /**
     * Returns the speed as a String.
     * @return the speed rendered as a String in MB/s, KB/s or B/s.
     */
    private String getUploadSpeedString() {

        float speedBps = getUploadSpeed();
        if(speedBps > 1000000) {

            return String.format("%,.2fMB/s", (speedBps / 1024f / 1024f));

        }
        if(speedBps > 1000) {

            return String.format("%,.2fKB/s", (speedBps / 1024f));
        }

        return String.format("%,.2fB/s", speedBps);

    }

    /**
     * Returns the array of bytes in this part.
     * @return a byte array containing the part data.
     */
    byte[] initializeAndReadBytes(ChunkedInputStream in) throws IOException {

        processStartTimestamp = System.currentTimeMillis();

        int rangeStart = in.getPosition();
        _LOG.debug("Input stream at {}", rangeStart);

        // Get part from stream
        byte[] bytes = in.readChunk();
        length = bytes.length;

        int rangeEnd = rangeStart + bytes.length - 1;
        uploadRange = String.format("bytes %d-%d/*", rangeStart, rangeEnd);
        _LOG.debug("Read {} bytes", bytes.length);


        return bytes;

    }

    /**
     * Returns the number of bytes in the part.
     * @return the number of bytes. Equivalent to {@code getBytes().length}.
     */
    int getLength() {

        return length;

    }

    /**
     * Returns the time this process has taken to execute.
     * @return the duraiton in milliseconds, or zero if it has not yet started.
     */
    long getProcessDuration() {

        return processStartTimestamp == 0 ? 0 :
                (endTimestamp == 0 ? System.currentTimeMillis() - processStartTimestamp :
                        endTimestamp - processStartTimestamp);

    }

    /**
     * Returns the tree hash calculated for this part.
     * @return a byte-array containing the SHA-256 tree hash.
     */
    byte[] getTreeHash() {

        return treeHash;

    }

    /**
     * Returns the upload range string used to indicate the sequence of this part in the overall archive.
     * @return a String indicating the start and end bytes of this part.
     */
    String getUploadRange() {

        return uploadRange;

    }

    /**
     * Sets the time the upload completed.
     * @param endTimestamp the end time in milliseconds.
     */
    void setEndTimestamp(long endTimestamp) {

        this.endTimestamp = endTimestamp;

    }

    /**
     * Sets the time the upload started.
     * @param processStartTimestamp the process start time in milliseconds.
     */
    void setProcessStartTimestamp(long processStartTimestamp) {

        this.processStartTimestamp = processStartTimestamp;

    }

    /**
     * Sets the tree hash calculated for this part.
     * @param treeHash a byte-array containing the SHA-256 tree hash.
     */
    void setTreeHash(byte[] treeHash) {

        this.treeHash = treeHash;

    }

    /**
     * Sets the time the upload started.
     * @param startTimestamp the upload start time in milliseconds.
     */
    void setUploadStartTimestamp(long startTimestamp) {

        this.uploadStartTimestamp = startTimestamp;

    }

    @Override
    public String toString() {

        return String.format("%,d bytes in %,.2fs (%s)", length, getUploadDuration() / 1000.0, getUploadSpeedString());

    }

    static class UploadRangeComparator implements Comparator<MultipartObject>
    {

        /**
         * Extracts the starting byte number from a range string.
         * @param rangeString the multipart range string.
         * @return the starting byte as an integer.
         */
        private static int extractStartByte(String rangeString) {

            Matcher m = Pattern.compile("(\\d+)-\\d+/\\*").matcher(rangeString);

            if (m.find()) {

                try {

                    return Integer.parseInt(m.group(1));

                } catch (IndexOutOfBoundsException | NumberFormatException e) {

                    _LOG.warn("Unable to parse range: {}", rangeString);

                }
            }

            return 0;

        }

        @Override
        public int compare(MultipartObject mo1, MultipartObject mo2) {

            return (extractStartByte(mo1.getUploadRange()) - extractStartByte(mo2.getUploadRange()));

        }
    }
}
