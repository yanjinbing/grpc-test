import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server2 {
    public static void main(String[] args) throws IOException, InterruptedException {
        String raftPath = "/tmp/raft/2";
        deleteDir(new File(raftPath));
        String peerList = "127.0.0.1:8081::100,127.0.0.1:8082::80,127.0.0.1:8083::60";
        //  String peerList = "127.0.0.1:8081::100";
        GrpcServer.main(new String[]{
                raftPath,
                "8092",
                "127.0.0.1:8082::80",
                peerList,
                "a1"
        });
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
