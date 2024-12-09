import java.io.*;

public class MyDedup {

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
                    }

                    break;

                case "download":

                    System.out.println("downloaded");

                    break;

                case "delete":

                    System.out.println("deleted");

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

}
