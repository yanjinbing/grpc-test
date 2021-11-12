import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server2 {
    public static void main(String[] args) throws IOException, InterruptedException {
        deleteDir(new File("d:/test/raft/2"));
        GrpcServer.main(new String[]{
                "d:/test/raft/2",
                "8092",
                "127.0.0.1:8082",
                "127.0.0.1:8082",
                "a2"
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
