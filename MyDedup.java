import java.io.*;
import java.util.*;

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
                    byte[] file;
                    try (FileInputStream fileInputStream = new FileInputStream(args[4])) {
                        file = fileInputStream.readAllBytes();
                    }

                    // 2. chunk -> hash -> recipe (& buffer)
                    boolean hasNextByte = true;
                    int chunkStart = 0, chunkEnd;
                    long prevRfp = 0;

                    // precompute d^0..minChunk-1 mod avgChunk
                    int[] dToPowIndexModQ = new int[minChunk];
                    for (int i = 0; i < minChunk; ++i) {
                        dToPowIndexModQ[i] = modularPow(257, i, avgChunk);
                    }

                    while (hasNextByte) {

                        for (int noOfMisses = 0; ; ++noOfMisses) {

                            // EOF
                            if (chunkStart + minChunk >= file.length) {
                                chunkEnd = file.length - 1;
                                hasNextByte = false;
                                break;
                            }

                            // max chunk
                            if (minChunk + noOfMisses == maxChunk) {
                                chunkEnd = chunkStart + maxChunk - 1;
                                break;
                            }

                            long rfp = 0;
                            // hacky modulo trick to prevent overflow
                            if (noOfMisses == 0) {
                                for (int i = 0; i < minChunk; ++i) {
                                    rfp += ((file[chunkStart + noOfMisses + i] % avgChunk) * dToPowIndexModQ[minChunk - (i + 1)]) % avgChunk;
                                }
                                rfp %= avgChunk;
                            } else {
                                rfp = (((257 % avgChunk) * ((prevRfp % avgChunk - (dToPowIndexModQ[minChunk - 1] * (file[chunkStart + noOfMisses - 1] % avgChunk)) % avgChunk) % avgChunk)) % avgChunk + file[chunkStart + noOfMisses + minChunk - 1] % avgChunk) % avgChunk;
                            }
                            prevRfp = rfp;

                            if ((rfp & (avgChunk - 1)) == 0) {
                                chunkEnd = chunkStart + noOfMisses + minChunk - 1;
                                break;
                            }

                        }

                        chunkStart = chunkEnd + 1;

                    }

                    break;

                case "download":

                    System.out.println("downloaded");

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

                    // 2. find & remove recipe + stat modify
                    String filePath = args[1];

                    try {
                        List<MyDedupIndex.RecipeContent> fileRecipe = myDedupIndex.recipe.get(filePath); // Retrieve
                        
                        myDedupIndex.recipe.remove(filePath); // Remove
                        // Stat Modify
                        myDedupIndex.stat.noOfFilesStored--;

                        // 3. get chunk ref count -1 & check ref count == 0 -> delete index
                        for (MyDedupIndex.RecipeContent chunk : fileRecipe) {
                            String chunkHash = chunk.hash;

                            MyDedupIndex.IndexValue chunkIndex = myDedupIndex.index.get(chunkHash); // Get index entry

                            if (chunkIndex != null) {
                                chunkIndex.refCount--; // Chunk refCount -1

                                if (chunkIndex.refCount == 0) {
                                    myDedupIndex.index.remove(chunkHash); // Remove chunk with no ref

                                    // TODO: Stat Modify


                                    // 4. update container ref count & check ref count == 0 -> delete physically
                                    int containerId = chunkIndex.id;
                                    myDedupIndex.containerRefCount.put(containerId, myDedupIndex.containerRefCount.get(containerId) - 1); // Container refCount -1

                                    // Remove container if refCount == 0
                                    if (myDedupIndex.containerRefCount.get(containerId) == 0) {
                                        myDedupIndex.containerRefCount.remove(containerId);
                                        // Stat Modify
                                        myDedupIndex.stat.noOfContainers--;

                                        // TODO: Delete container physically

                                    }


                                }
                            }

                            // TODO: Stat Modify (PreDedupChunks and Bytes)


                        }

                        // 5. Update index file
                        try (FileOutputStream fileOutIndex = new FileOutputStream("./mydedup.index");
                            ObjectOutputStream objectOutIndex = new ObjectOutputStream(fileOutIndex)){
                            objectOutIndex.writeObject(myDedupIndex);
                        } catch (Exception e) {
                            e.printStackTrace();
                            System.out.println("[ERROR]: Failed to update index!");
                            System.exit(1);
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

}
