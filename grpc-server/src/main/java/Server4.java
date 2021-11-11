import org.example.GrpcServer;

import java.io.IOException;

public class Server4 {
    public static void main(String[] args) throws IOException, InterruptedException {
        GrpcServer.main(new String[]{
                "d:/test/raft/4",
                "8094",
                "127.0.0.1:8084",
                "127.0.0.1:8084",
                "a4"
        });
    }
}
