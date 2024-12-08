import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MyDedupIndex implements Serializable {

    @Serial private static final long serialVersionUID = 4180;
    public Stat stat = new Stat();
    public ConcurrentHashMap<String, List<RecipeContent>> recipe = new ConcurrentHashMap<>();
    public ConcurrentHashMap<String, IndexValue> index = new ConcurrentHashMap<>();
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
        public String hash;
        public int id;
        public int offset;
        public int size;
    }

    public static class IndexValue implements Serializable {
        @Serial private static final long serialVersionUID = 1155175977;
        public int id;
        public int offset;
        public int size;
        public long refCount;
    }

}
