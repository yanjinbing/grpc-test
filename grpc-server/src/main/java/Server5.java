import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server5 {
    public static void main(String[] args) throws IOException, InterruptedException {
        deleteDir(new File("/tmp/raft/5"));
        GrpcServer.main(new String[]{
                "d:/test/raft/5",
                "8095",
                "127.0.0.1:8085",
                "127.0.0.1:8085",
                "a5"
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
