import org.example.GrpcServer;

import java.io.IOException;

public class Server3 {
    public static void main(String[] args) throws IOException, InterruptedException {
        GrpcServer.main(new String[]{
                "d:/test/raft/3",
                "8093",
                "127.0.0.1:8083",
                "127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083"
        });
    }
}
