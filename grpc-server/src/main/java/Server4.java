import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server4 {
    public static void main(String[] args) throws IOException, InterruptedException {
        deleteDir(new File("d:/test/raft/4"));
        GrpcServer.main(new String[]{
                "d:/test/raft/4",
                "8094",
                "127.0.0.1:8084",
                "127.0.0.1:8084",
                "a4"
        });
    }
    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for(File file : dir.listFiles()){
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
