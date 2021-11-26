package org.example.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class GrpcClientSingle {
    static String defaultAddr ;
    static String[][] PEERIDTOGRPC;

    static boolean isLocalHost = true;

    static {
        if (isLocalHost) {
            defaultAddr = "127.0.0.1:8092";
            PEERIDTOGRPC = new String[][]{
                    {"127.0.0.1:8081", "127.0.0.1:8091"}

            };
        } else {
            defaultAddr = "10.81.116.79:8091";
            PEERIDTOGRPC = new String[][]{
                    {"10.81.116.77:8081", "10.81.116.77:8091"},
                    {"10.81.116.78:8081", "10.81.116.78:8091"},
                    {"10.81.116.79:8081", "10.81.116.79:8091"}
            };
        }
    }

    Map<String, String> peerIdToGrpc;
    ThreadLocal<Client> clients = new ThreadLocal<>();
    public GrpcClientSingle() {
        peerIdToGrpc = new HashMap<>();
        for (String[] peers : PEERIDTOGRPC) {
            peerIdToGrpc.put(peers[0], peers[1]);
        }
    }

    public Client flushLeader(String groupId) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(defaultAddr).usePlaintext().build();
        //创建方法存根
        HelloWorldGrpc.HelloWorldBlockingStub stub = HelloWorldGrpc.newBlockingStub(channel);
        try {
            GetLeaderRequest request = GetLeaderRequest.newBuilder()
                    .setGroupId(groupId)
                    .build();
            String grpcAddr;
            try {
                GetLeaderReply reply = stub.getLeader(request);
                grpcAddr = peerIdToGrpc.get(reply.getLeader());
            }catch (StatusRuntimeException e){
                e.printStackTrace();
                if (e.getTrailers() != null){
                    Metadata metadata = e.getTrailers();
                    System.out.println(metadata.get(Metadata.Key.of("key",Metadata.ASCII_STRING_MARSHALLER)));
                }
                grpcAddr = defaultAddr;
            }
            Client client = clients.get();
            if (client == null){
                client = new Client();
                clients.set(client);
            }
            client.channel = ManagedChannelBuilder.forTarget(grpcAddr).usePlaintext().build();
            client.stub = HelloWorldGrpc.newBlockingStub(client.channel);
            client.stub.withMaxInboundMessageSize(1024*1024*10);
            client.stub.withMaxOutboundMessageSize(1024*1024*10);
            System.out.println(groupId + " leader is " + grpcAddr);
            return client;
        } catch (Exception e) {
            e.printStackTrace();
            e.getCause().printStackTrace();
            throw e;
        }
    }


    Client getGrpcClient(String groupId){
        Client client = clients.get();
        if ( client == null )
            client = flushLeader(groupId);
        return client;
    }

    public void sendOne(String groupId, ByteString key, ByteString value) {
        try {
            // 构建消息
            HelloRequest request = HelloRequest.newBuilder()
                    .setGroupId(groupId)
                    .setKey(key)
                    .setValue(value)
                    .build();
            Client client = getGrpcClient(groupId);
            // 发送消息
            HelloReply response = client.stub.sayHello(request);
        }catch (Throwable e){
            // leader发生改变，重新刷新Leader
            flushLeader(groupId);
            System.out.println(e.getMessage());
            throw  e;
        }

    }
    public void batchTest(String[] args) throws InterruptedException {
        if ( args.length < 3) {
            System.out.println("参数错误。线程数 每线程条目数 值大小");
            System.exit(0);
        }
        int threads = Integer.parseInt(args[0]);
        int total = Integer.parseInt(args[1]);
        int batches = Integer.parseInt(args[2]);
        final Long[] start = {System.currentTimeMillis()};
        final AtomicLong[] lastC = {new AtomicLong()};



        System.out.println(String.format("线程数 %d, 每线程条目数 %d, 每批次大小 %d", threads, total, batches));

        byte[] value = new byte[batches];
        for(int n = 0; n < batches; n++){
            value[n] = (byte)(n + 71);
        }
        ByteString bsvalue = ByteString.copyFrom(value);
        AtomicLong counter = new AtomicLong(0);
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        for (int t = 0; t < threads; t++) {
            int finalT = t;
            executor.execute(() -> {
                for (int i = 0; i < total; i++) {
                    sendOne("a" + finalT, ByteString.copyFromUtf8("Key " + System.currentTimeMillis()), bsvalue);
                    long c = counter.incrementAndGet();
                    if ( System.currentTimeMillis() - start[0] > 1000*5){
                        synchronized (start[0]) {
                            if ( System.currentTimeMillis() - start[0] > 1000*5){
                                System.out.println(String.format("条目数 %d, 平均性能 %d K",
                                        c, (c - lastC[0].get()) * batches / (System.currentTimeMillis() - start[0])));
                                lastC[0].set(c);
                                start[0] = System.currentTimeMillis();
                            }
                        }
                    }
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.HOURS);

        System.out.println(String.format("条目数 %d, 平均数据量 %d",
                counter.get(), counter.get() *1000/ (System.currentTimeMillis() - start[0])));
    }

    public void addPeer(String groupId, String address, String peers) {
        try {
            // 构建消息
            PeerRequest request = PeerRequest.newBuilder()
                    .setGroupId(groupId)
                    .setAddress(address)
                    .build();
            Client client = getGrpcClient(groupId);
            // 发送消息
            PeerReply response = client.stub.addPeer(request);
        }catch (Throwable e){
            System.out.println(e.getMessage());
            throw  e;
        }
    }


    public void removePeer(String groupId, String address) {
        try {
            // 构建消息
            PeerRequest request = PeerRequest.newBuilder()
                    .setGroupId(groupId)
                    .setAddress(address)
                    .build();
            Client client = getGrpcClient(groupId);
            // 发送消息
            PeerReply response = client.stub.removePeer(request);
        }catch (Throwable e){
            System.out.println(e.getMessage());
            throw  e;
        }
    }

    class Client{
        public ManagedChannel channel = null;
        //创建方法存根
        public HelloWorldGrpc.HelloWorldBlockingStub stub = null;
    }

    public static void main(String[] args) throws InterruptedException {
        GrpcClientSingle client = new GrpcClientSingle();
        for(int i = 0; i<100; i++)
            client.sendOne("a1", ByteString.copyFromUtf8("Hello" + i), ByteString.copyFromUtf8("World" + i));
    }
}
