public class MyDedup {

    public static void main(String[] args) {
        switch (args[0]) {

            case "upload":
                System.out.println("uploaded");
                break;

            case "download":
                System.out.println("downloaded");
                break;

            case "delete":
                System.out.println("deleted");
                break;

            default:
                System.out.println("[ERROR]: Invalid operation!");

        }
    }

}
