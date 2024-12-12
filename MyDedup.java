import java.io.*;
import java.util.*;
import java.math.BigInteger;
import java.security.MessageDigest;

public class MyDedup {

    public static void main(String[] args) {
        try {

            // 0. load metadata file
            MyDedupIndex myDedupIndex = loadMetadata();

            switch (args[0]) {

                case "upload":

                    // 1. read args & file
                    if (args.length != 5) {
                        System.out.println("[USAGE]: java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <file_to_upload>");
                        System.exit(1);
                    } else if (!isInteger(logN(Integer.parseInt(args[1]), 2))) {
                        System.out.println("[ERROR]: min_chunk has to be power of 2!");
                        System.exit(1);
                    } else if (!isInteger(logN(Integer.parseInt(args[2]), 2))) {
                        System.out.println("[ERROR]: avg_chunk has to be power of 2!");
                        System.exit(1);
                    } else if (!isInteger(logN(Integer.parseInt(args[3]), 2))) {
                        System.out.println("[ERROR]: max_chunk has to be power of 2!");
                        System.exit(1);
                    } else if (!new File(args[4]).exists()) {
                        System.out.println("[ERROR]: " + args[4] + " does not exist!");
                        System.exit(1);
                    } else if (myDedupIndex.recipe.containsKey(args[4])) {
                        System.out.println("[ERROR]: " + args[4] + " already uploaded!");
                        System.exit(1);
                    }

                    int minChunk = Integer.parseInt(args[1]);
                    int avgChunk = Integer.parseInt(args[2]);
                    int maxChunk = Integer.parseInt(args[3]);
                    File _file = new File(args[4]);
                    byte[] file = new byte[(int) _file.length()];
                    try (FileInputStream fileInputStream = new FileInputStream(args[4])) {
                        int bytesRead = 0;
                        while (bytesRead < file.length) {
                            int result = fileInputStream.read(file, bytesRead, file.length - bytesRead);
                            if (result == -1) break;
                            bytesRead += result;
                        }
                    }

                    // 2. chunk -> hash -> recipe (& buffer)
                    boolean hasNextByte = true;
                    int chunkStart = 0, chunkEnd;
                    long prevRfp = 0;

                    // precompute d^0..minChunk-1 mod avgChunk
                    int[] dToPowIndexModQ = new int[minChunk];
                    if (avgChunk != 1) {
                        dToPowIndexModQ[0] = 1;
                        for (int i = 1; i < minChunk; ++i) {
                            dToPowIndexModQ[i] = (dToPowIndexModQ[i - 1] * 257) % avgChunk;
                        }
                    }

                    MessageDigest messageDigest = MessageDigest.getInstance("MD5");
                    List<MyDedupIndex.RecipeContent> recipeContents = new ArrayList<>();
                    ByteArrayOutputStream buffer = new ByteArrayOutputStream(1 * 1024 * 1024);
                    long noOfChunksInBuffer = 0;

                    while (hasNextByte) {

                        // chunk
                        for (int noOfMisses = 0; ; ++noOfMisses) {

                            // EOF
                            if (chunkStart + noOfMisses + minChunk - 1 >= file.length - 1) {
                                chunkEnd = file.length - 1;
                                hasNextByte = false;
                                break;
                            }

                            // max chunk
                            if (minChunk + noOfMisses >= maxChunk) {
                                chunkEnd = chunkStart + maxChunk - 1;
                                break;
                            }

                            long rfp = 0;
                            // hacky modulo trick to prevent overflow
                            if (noOfMisses == 0) {
                                for (int i = 0; i < minChunk; ++i) {
                                    rfp += ((Byte.toUnsignedInt(file[chunkStart + noOfMisses + i]) % avgChunk) * dToPowIndexModQ[minChunk - (i + 1)]) % avgChunk;
                                }
                                rfp %= avgChunk;
                            } else {
                                rfp = (((257 % avgChunk) * ((prevRfp % avgChunk - (dToPowIndexModQ[minChunk - 1] * (Byte.toUnsignedInt(file[chunkStart + noOfMisses - 1]) % avgChunk)) % avgChunk) % avgChunk)) % avgChunk + Byte.toUnsignedInt(file[chunkStart + noOfMisses + minChunk - 1]) % avgChunk) % avgChunk;
                            }
                            prevRfp = rfp;

                            if ((rfp & (avgChunk - 1)) == 0) {
                                chunkEnd = chunkStart + noOfMisses + minChunk - 1;
                                break;
                            }

                        }
                        byte[] chunk = Arrays.copyOfRange(file, chunkStart, chunkEnd + 1);
                        ++myDedupIndex.stat.noOfPreDedupChunks;
                        myDedupIndex.stat.noOfBytesOfPreDedupChunks += chunk.length;

                        // hash
                        String hash = new BigInteger(1, messageDigest.digest(chunk)).toString(16);
                        messageDigest.reset();
                        if (myDedupIndex.index.containsKey(hash)) {
                            MyDedupIndex.IndexValue indexValue = myDedupIndex.index.get(hash);
                            recipeContents.add(new MyDedupIndex.RecipeContent(
                                    hash,
                                    indexValue.id,
                                    indexValue.offset,
                                    indexValue.size
                            ));
                            ++indexValue.refCount;
                        } else {

                            if (buffer.size() + chunk.length > 1 * 1024 * 1024) {
                                try (FileOutputStream fileOutputStream = new FileOutputStream("./data/" + myDedupIndex.nextContainerId)) {
                                    buffer.writeTo(fileOutputStream);
                                    myDedupIndex.containerRefCount.put(myDedupIndex.nextContainerId, noOfChunksInBuffer);
                                    ++myDedupIndex.stat.noOfContainers;
                                    ++myDedupIndex.nextContainerId;
                                    buffer.reset();
                                    noOfChunksInBuffer = 0;
                                }
                            }

                            recipeContents.add(new MyDedupIndex.RecipeContent(
                                    hash,
                                    myDedupIndex.nextContainerId,
                                    buffer.size(),
                                    chunk.length
                            ));
                            myDedupIndex.index.put(
                                    hash,
                                    new MyDedupIndex.IndexValue(
                                            myDedupIndex.nextContainerId,
                                            buffer.size(),
                                            chunk.length,
                                            1
                                    )
                            );
                            buffer.write(chunk);
                            ++noOfChunksInBuffer;
                            ++myDedupIndex.stat.noOfUniqueChunks;
                            myDedupIndex.stat.noOfBytesOfUniqueChunks += chunk.length;

                        }

                        if (!hasNextByte && buffer.size() != 0) {
                            try (FileOutputStream fileOutputStream = new FileOutputStream("./data/" + myDedupIndex.nextContainerId)) {
                                buffer.writeTo(fileOutputStream);
                                myDedupIndex.containerRefCount.put(myDedupIndex.nextContainerId, noOfChunksInBuffer);
                                ++myDedupIndex.stat.noOfContainers;
                                ++myDedupIndex.nextContainerId;
                                buffer.reset();
                                noOfChunksInBuffer = 0;
                            }
                        }

                        chunkStart = chunkEnd + 1;

                    }

                    myDedupIndex.recipe.put(args[4], recipeContents);
                    ++myDedupIndex.stat.noOfFilesStored;

                    reportStat(myDedupIndex);

                    break;

                case "download":

                    // 1. read args

                    if (args.length != 3) {
                        System.out.println("[USAGE]: java MyDedup download <file_to_download> <new_file_name>");
                        System.exit(1);
                    }

                    if (!myDedupIndex.recipe.containsKey(args[1])) {
                        System.out.println("[ERROR]: \"" + args[1] + "\" does not exist");
                        System.exit(1);
                    }

                    // 2. read chunks

                    List<MyDedupIndex.RecipeContent> chunkList = myDedupIndex.recipe.get(args[1]);

                    ByteArrayOutputStream data = new ByteArrayOutputStream();

                    // 3. loop for read and write

                    for (MyDedupIndex.RecipeContent currentChunk : chunkList) {
                        File containerFile = new File("data/" + currentChunk.id);
                        if (!containerFile.exists()) {
                            System.out.println("[ERROR]: Missing container file: " + currentChunk.id);
                            System.exit(1);
                        }

                        try (FileInputStream fileInputContainer = new FileInputStream(containerFile)) {
                            fileInputContainer.skip(currentChunk.offset);
                            byte[] containerData = new byte[currentChunk.size];

                            int bytesRead = 0;
                            while (bytesRead < containerData.length) {
                                int result = fileInputContainer.read(containerData, bytesRead, containerData.length - bytesRead);
                                if (result == -1) break;
                                bytesRead += result;
                            }

                            data.write(containerData);
                        } catch (IOException e) {
                            System.err.println("[ERROR]: Failed to read chunk from container " + currentChunk.id);
                            e.printStackTrace();
                            System.exit(1);
                        }
                    }

                    if (data.size() == 0) {
                        System.out.println("[ERROR]: No data to write. Data size == 0.");
                        System.exit(1);
                    }

                    // 4. create local output file

                    File fileOut = new File(args[2]);
                    if (fileOut.getParentFile() != null) {
                        fileOut.getParentFile().mkdirs();
                    }
                    try {
                        if (!fileOut.createNewFile()) {
                            System.out.println("[ERROR]: Failed to create output file!");
                            System.exit(1);
                        }
                    } catch (IOException e) {
                        System.err.println("[ERROR]: Could not create file " + args[2]);
                        e.printStackTrace();
                        System.exit(1);
                    }

                    try (FileOutputStream outputFile = new FileOutputStream(fileOut)) {
                        data.writeTo(outputFile);
                        System.out.println("Downloaded: " + args[2]);
                    } catch (IOException e) {
                        System.err.println("[ERROR]: Failed to write data to " + args[2]);
                        e.printStackTrace();
                    }

                    break;

                case "delete":

                    // 1. read args
                    if (args.length != 2) {
                        System.out.println("[USAGE]: java MyDedup delete <pathname>");
                        System.exit(1);
                    } else if (!myDedupIndex.recipe.containsKey(args[1])) {
                        System.out.println("[ERROR]: file " + args[1] + " does not exist!");
                        System.exit(1);
                    }

                    // 2. find & remove recipe
                    String filePath = args[1];

                    try {
                        List<MyDedupIndex.RecipeContent> fileRecipe = myDedupIndex.recipe.get(filePath); // Retrieve

                        myDedupIndex.recipe.remove(filePath); // Remove
                        // Stat Modify (stored files no.)
                        myDedupIndex.stat.noOfFilesStored--;

                        // 3. get chunk ref count -1 & check ref count == 0 -> delete index
                        for (MyDedupIndex.RecipeContent chunk : fileRecipe) {
                            String chunkHash = chunk.hash;

                            MyDedupIndex.IndexValue chunkIndex = myDedupIndex.index.get(chunkHash); // Get index entry

                            if (chunkIndex != null) {
                                chunkIndex.refCount--; // Chunk refCount -1

                                if (chunkIndex.refCount == 0) {
                                    myDedupIndex.index.remove(chunkHash); // Remove chunk with no ref

                                    // Stat Modify (unique chunks and bytes)
                                    myDedupIndex.stat.noOfUniqueChunks--;
                                    myDedupIndex.stat.noOfBytesOfUniqueChunks -= chunkIndex.size;

                                    // 4. update container ref count & check ref count == 0 -> delete physically
                                    int containerId = chunkIndex.id;
                                    myDedupIndex.containerRefCount.put(containerId, myDedupIndex.containerRefCount.get(containerId) - 1); // Container refCount -1

                                    // Remove container if refCount == 0
                                    if (myDedupIndex.containerRefCount.get(containerId) == 0) {
                                        myDedupIndex.containerRefCount.remove(containerId);
                                        // Stat Modify (containers no.)
                                        myDedupIndex.stat.noOfContainers--;

                                        // Delete container physically
                                        String storagePath = "./data/" + containerId;
                                        File containerFile = new File(storagePath);
                                        if (containerFile.exists()) {
                                            containerFile.delete();
                                        }
                                    }
                                }

                            }

                            // Stat Modify (PreDedupChunks and Bytes)
                            myDedupIndex.stat.noOfPreDedupChunks--;
                            myDedupIndex.stat.noOfBytesOfPreDedupChunks -= chunk.size;

                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("[ERROR]: Failed to delete file " + filePath + "!");
                        System.exit(1);
                    }

                    break;

                default:
                    System.out.println("[ERROR]: Invalid operation!");
                    System.exit(1);

            }

            // 0. save metadata file
            saveMetadata(myDedupIndex);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static double logN(double a, double base) {
        return Math.log(a) / Math.log(base);
    }

    private static boolean isInteger(double a) {
        return !Double.isNaN(a) && !Double.isInfinite(a) && (int) a == a;
    }

    private static MyDedupIndex loadMetadata() throws IOException, ClassNotFoundException {
        if (!new File("./mydedup.index").exists()) {
            return new MyDedupIndex();
        }

        try (FileInputStream fileInputStream = new FileInputStream("./mydedup.index"); ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream)) {
            return (MyDedupIndex) objectInputStream.readObject();
        }
    }

    private static void saveMetadata(MyDedupIndex myDedupIndex) throws IOException {
        try (FileOutputStream fileOutputStream = new FileOutputStream("./mydedup.index"); ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream)) {
            objectOutputStream.writeObject(myDedupIndex);
        }
    }

    // b^2 mod m = (b * b) mod m = (b mod m * b mod m) mod m
    private static int modularPow(int base, int exponent, int modulo) {
        if (modulo == 1) {
            return 0;
        }

        int result = 1;
        for (int e = 0; e < exponent; ++e) {
            result = (result * base) % modulo;
        }
        return result;
    }

    private static void reportStat(MyDedupIndex myDedupIndex) {
        System.out.println();
        System.out.println("Report Output:");
        System.out.println("Total number of files that have been stored: " + myDedupIndex.stat.noOfFilesStored);
        System.out.println("Total number of pre-deduplicated chunks in storage: " + myDedupIndex.stat.noOfPreDedupChunks);
        System.out.println("Total number of unique chunks in storage: " + myDedupIndex.stat.noOfUniqueChunks);
        System.out.println("Total number of bytes of pre-deduplicated chunks in storage: " + myDedupIndex.stat.noOfBytesOfPreDedupChunks);
        System.out.println("Total number of bytes of unique chunks in storage: " + myDedupIndex.stat.noOfBytesOfUniqueChunks);
        System.out.println("Total number of containers in storage: " + myDedupIndex.stat.noOfContainers);
        System.out.println("Deduplication ratio: " + String.format("%.2f", myDedupIndex.stat.getDedupRatio()));
    }

}
