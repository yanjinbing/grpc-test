import org.example.GrpcServer;

import java.io.IOException;

public class Server6 {
    public static void main(String[] args) throws IOException, InterruptedException {
        GrpcServer.main(new String[]{
                "d:/test/raft/6",
                "8096",
                "127.0.0.1:8086",
                "127.0.0.1:8086",
                "a6"
        });
    }
}
