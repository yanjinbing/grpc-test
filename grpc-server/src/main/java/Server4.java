import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server4 {
    public static void main(String[] args) throws IOException, InterruptedException {
        deleteDir(new File("/tmp/raft/4"));
        //String peerList = "127.0.0.1:8081::100,127.0.0.1:8082::80,127.0.0.1:8083::60,127.0.0.1:8084::60";
        String peerList = "127.0.0.1:8084";
        GrpcServer.main(new String[]{
                "d:/test/raft/4",
                "8094",
                "127.0.0.1:8084",
                peerList,
                "a1"
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
