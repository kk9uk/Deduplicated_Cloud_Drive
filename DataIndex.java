import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class DataIndex implements Serializable {

    @Serial private static final long serialVersionUID = 4180;
    public Stat stat = new Stat();
    public ConcurrentHashMap<String, List<RecipeContent>> recipe = new ConcurrentHashMap<>();
    public ConcurrentHashMap<String, IndexValue> index = new ConcurrentHashMap<>();
    public int nextContainerId = 1;
    public ConcurrentHashMap<Integer, Long> containerRefCount = new ConcurrentHashMap<>();

    public static class Stat implements Serializable {
        @Serial private static final long serialVersionUID = 1155177381;
        public byte noOfFilesStored;
        public long noOfPreDedupChunks;
        public long noOfUniqueChunks;
        public long noOfBytesOfPreDedupChunks;
        public long noOfBytesOfUniqueChunks;
        public int noOfContainers;
        public double getDedupRatio() {
            return noOfBytesOfUniqueChunks == 0 ? 0 : (double) noOfBytesOfPreDedupChunks / noOfBytesOfUniqueChunks;
        }
    }

    public static class RecipeContent implements Serializable {
        @Serial private static final long serialVersionUID = 1155174948;
        public final String hash;
        public final int id;
        public final int offset;
        public final int size;

        public RecipeContent(String hash, int id, int offset, int size) {
            this.hash = hash;
            this.id = id;
            this.offset = offset;
            this.size = size;
        }
    }

    public static class IndexValue implements Serializable {
        @Serial private static final long serialVersionUID = 1155175977;
        public final int id;
        public final int offset;
        public final int size;
        public long refCount;

        public IndexValue(int id, int offset, int size, long refCount) {
            this.id = id;
            this.offset = offset;
            this.size = size;
            this.refCount = refCount;
        }
    }

}
