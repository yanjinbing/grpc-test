import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server6 {
    public static void main(String[] args) throws IOException, InterruptedException {
        deleteDir(new File("d:/test/raft/6"));
        GrpcServer.main(new String[]{
                "d:/test/raft/6",
                "8096",
                "127.0.0.1:8086",
                "127.0.0.1:8086",
                "a6"
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
