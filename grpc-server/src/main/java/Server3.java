import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server3 {
    public static void main(String[] args) throws IOException, InterruptedException {
        deleteDir(new File("d:/test/raft/3"));
        GrpcServer.main(new String[]{
                "d:/test/raft/3",
                "8093",
                "127.0.0.1:8083",
                "127.0.0.1:8083",
                //"127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083",
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
