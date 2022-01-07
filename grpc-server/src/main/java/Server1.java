import org.example.GrpcServer;

import java.io.File;
import java.io.IOException;

public class Server1 {
    public static void main(String[] args) throws IOException, InterruptedException {
        String raftPath = "d:/test/raft/1";
        deleteDir(new File(raftPath));
        GrpcServer.main(new String[]{
                raftPath,
                "8091",
                "127.0.0.1:8081",
                //"127.0.0.1:8081",
                "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083",
                "a"
        });
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for(File file : dir.listFiles()){
                deleteDir(file);
            }
        }
        // 目录此时为空，可以删除
        return dir.delete();
    }
}
