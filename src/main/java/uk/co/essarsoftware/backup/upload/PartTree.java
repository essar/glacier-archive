package uk.co.essarsoftware.backup.upload;

import java.util.*;
import java.util.stream.Collectors;

public class PartTree extends TreeSet<MultipartObject>
{

    PartTree() {

        super(new MultipartObject.UploadRangeComparator());

    }

    public List<byte[]> getHashes() {

        return stream().map(MultipartObject::getTreeHash).collect(Collectors.toList());

    }
}
