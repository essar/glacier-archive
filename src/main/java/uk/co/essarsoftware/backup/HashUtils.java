package uk.co.essarsoftware.backup;

import org.apache.commons.codec.binary.Hex;
import uk.co.essarsoftware.backup.io.ChunkedInputStream;
import uk.co.essarsoftware.backup.upload.PartTree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Helper class defining hashing methods.
 * @author <steve.roberts/>
 */
public class HashUtils
{

    private final MessageDigest digest;

    private static final String _SHA1 = "SHA-1";
    private static final String _SHA256 = "SHA-256";
    public static final String DEFAULT_HASH = "0";

    /**
     * Instantiate a new instance for the specified algorithm.
     * @param algo a String containing the name of the hashing algorithm to use.
     * @see MessageDigest#getInstance(String)
     * @throws NoSuchAlgorithmException if an algorithm of the provided name cannot be loaded.
     */
    private HashUtils(String algo) throws NoSuchAlgorithmException {

        digest = MessageDigest.getInstance(algo);

    }

    /**
     * Calculate the hash of an array of bytes.
     * @param bytes the data to hash.
     * @return a byte array containing the hash result.
     */
    private byte[] calculateHashBytes(byte[] bytes) {

        return digest.digest(bytes);

    }

    /**
     * Calculate the tree hash from a list of leaf hashes.
     * @param hashes a List of hashes calculated from a set of blocks.
     * @return a byte array containing the has result.
     */
    private byte[] calculateTreeHashFromHashes(List<byte[]> hashes) {

        LinkedList<byte[]> leaves = new LinkedList<>(hashes);

        while (leaves.size() > 1) {

            LinkedList<byte[]> parents = new LinkedList<>();
            while (leaves.size() > 1) {

                // Create buffer to support 2 x 256bit hashes
                ByteBuffer buf = ByteBuffer.allocate(64);
                buf.put(leaves.removeFirst());
                buf.put(leaves.removeFirst());
                parents.add(calculateHashBytes(buf.array()));

            }

            parents.addAll(leaves);

            // Promote the parents to be leaves
            leaves.clear();
            leaves.addAll(parents);

        }

        return leaves.size() == 0 ? new byte[] { 0x00 } : leaves.getFirst();

    }

    /**
     * Calculate the hash of an array of bytes and return as a hex-encoded string.
     * @param bytes the data to hash.
     * @return a String containing the hex-encoded representation of the hash result.
     */
    private String calculateHashString(byte[] bytes) {

        return Hex.encodeHexString(calculateHashBytes(bytes));

    }

    /**
     * Calculate the hash of a string and return as a hex-encoded string.
     * @param text the data to hash.
     * @return a String containing the hex-encoded representation of the hash result.
     */
    public String calculateHashString(String text) {

        return calculateHashString(text.getBytes());

    }

    /**
     * Calculate a tree hash of data consumed from the provided stream. Consumes all data until the stream is exhausted.
     * @param in a ChunkedInputStream containing data to tree hash.
     * @return a byte array containing the tree hash result.
     * @throws IOException if the stream cannot be read.
     */
    public byte[] calculateTreeHash(ChunkedInputStream in) throws IOException {

        List<byte[]> leaves = new LinkedList<>();

        byte[] chunk;
        do {

            chunk = in.readChunk();
            if(chunk.length > 0) {

                leaves.add(calculateHashBytes(chunk));

            }

        } while(chunk.length > 0);

        return calculateTreeHashFromHashes(leaves);

    }

    /**
     * Calculate a tree hash of data from a list of byte arrays.
     * @param nodes a List of byte arrays that contain the data to tree hash.
     * @return a byte array containing the tree hash result.
     */
    public byte[] calculateTreeHash(List<byte[]> nodes) {

        // Calculate a hash for each node
        List<byte[]> leaves = nodes.stream().map(this::calculateHashBytes).collect(Collectors.toList());
        return calculateTreeHashFromHashes(leaves);

    }

    /**
     * Calculate a tree hash from a set of parts.
     * @param parts a PartTree containing an ordered set of part metadata.
     * @return a byte array containing the tree hash result.
     */
    public byte[] calculateTreeHash(PartTree parts) {

        LinkedList<byte[]> leaves = new LinkedList<>(parts.getHashes());
        return calculateTreeHashFromHashes(leaves);

    }

    /**
     * Creates a new instance using the SHA1 algorithm.
     * @return a HashUtils instance.
     */
    public static HashUtils createSHA1Instance() {

        try {

            return new HashUtils(_SHA1);

        } catch (NoSuchAlgorithmException nsae) {

            throw new ExceptionInInitializerError(nsae);

        }
    }

    /**
     * Creates a new instance using the SHA256 algorithm.
     * @return a HashUtils instance.
     */
    public static HashUtils createSHA256Instance() {

        try {

            return new HashUtils(_SHA256);

        } catch (NoSuchAlgorithmException nsae) {

            throw new ExceptionInInitializerError(nsae);

        }
    }
}
