import org.example.GrpcServer;

import java.io.IOException;

public class Server5 {
    public static void main(String[] args) throws IOException, InterruptedException {
        GrpcServer.main(new String[]{
                "d:/test/raft/5",
                "8095",
                "127.0.0.1:8085",
                "127.0.0.1:8085",
                "a5"
        });
    }
}
