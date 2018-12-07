package uk.co.essarsoftware.backup.io;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * Stream providing methods for reading data in specified-length chunks.
 * @author <steve.roberts/>
 */
public class ChunkedInputStream extends InputStream
{

    private int position = 0;
    private final int chunkSize;
    private final InputStream in;

    /**
     * Convenience method for reading an array of bytes. Wraps the byte array in a {@see ByteArrayInputStream}.
     * @param bytes an array of bytes to read.
     * @param chunkSize the size of the chunks to read.
     */
    public ChunkedInputStream(byte[] bytes, int chunkSize) {

        this(new ByteArrayInputStream(bytes), chunkSize);

    }

    /**
     * Wraps an existing {@see InputStream}.
     * @param in the stream to read.
     * @param chunkSize the size of the chunks to read.
     */
    public ChunkedInputStream(InputStream in, int chunkSize) {

        this.in = in;
        this.chunkSize = chunkSize;

    }

    /**
     * Gets the current position from the start of the stream.
     * @return the number of bytes already read.
     */
    public int getPosition() {

        synchronized (this) {

            return position;

        }
    }

    @Override
    public int read() throws IOException {

        synchronized (this) {

            int b = in.read();
            if (b >= 0) {

                position++;

            }

            return b;

        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {

        synchronized (this) {

            int read = in.read(b, off, len);
            if (read >= 0) {

                position += read;

            }

            return read;

        }
    }

    /**
     * Reads a chunk of data of the specified number of bytes or until the end of the stream. This method is synchronized
     * so multiple threads may call this concurrently to retrieve sequential chunks.
     * @return a byte array read from the stream. The byte array is shrunk to the exact length read.
     * @throws IOException if an error occurs reading the data.
     */
    public byte[] readChunk() throws IOException {

        byte[] buf = new byte[chunkSize];

        synchronized (this) {

            int len = read(buf);
            if (len > 0) {

                return Arrays.copyOf(buf, len);

            }

            return new byte[0];

        }
    }
}
