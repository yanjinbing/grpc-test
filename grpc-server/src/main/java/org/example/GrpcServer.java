package org.example;


import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang.StringUtils;
import org.example.grpc.*;
import org.example.rpc.RaftNodeProcessor;
import org.example.rpc.RaftRpcClient;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hello world!
 */
public class GrpcServer {
    private Server grpcServer;
    // raft状态机
    private RaftParams raftParams;

    private RaftEngine raftEngine = new RaftEngine();
    private ExecutorService executorService;
    /**
     * 启动grpc服务
     */
    private void start(int port) throws IOException {
        raftEngine.setBasePath(raftParams.dataPath);
        // 启动grpc服务
        grpcServer = ServerBuilder.forPort(port)
                .addService(new HelloWorldImpl(raftEngine))
                .maxInboundMessageSize(1024 * 1024 * 10)
                .maxInboundMetadataSize(1024 * 1024 * 10)


                .build()
                .start();


        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                GrpcServer.this.stop();
            }
        });
       raftEngine.createRaftRpcServer(raftParams.raftAddr);


        executorService = Executors.newSingleThreadExecutor();
        executorService.execute(()->{
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (StringUtils.isNotBlank(raftParams.groupId))
                raftEngine.startRaft(raftParams.groupId, raftParams.dataPath, raftParams.peersList);
        });


    }

    private void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }
    //=========================================================================================================//
    /**
     * 实现grpc服务
     */
    static class HelloWorldImpl extends HelloWorldGrpc.HelloWorldImplBase {
        private RaftEngine engine;

        public HelloWorldImpl(RaftEngine server) {
            this.engine = server;
        }

        //实现grpc方法
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> observer) {
            System.out.println("Grpc thread " + Thread.currentThread().getId());
            //创建任务，发送给其他peer
            String groupId = request.getGroupId();

            if (!engine.isLeader(groupId)) {
                observer.onError(io.grpc.Status.ABORTED.asException());
                observer.onCompleted();
                return;
            }
            try {
                // 提交raft 任务
                putTask(groupId, request.getKey().toByteArray(), request.getValue().toByteArray(),
                        new Closure() {
                            @Override
                            public void run(Status status) {
                                observer.onNext(HelloReply.newBuilder().setMessage("response " + request.getKey()).build());
                                observer.onCompleted();
                            }
                        });

           //     engine.createSnapshot(groupId, 11);
            } catch (Exception e) {
                observer.onError(io.grpc.Status.fromThrowable(e).asException());
                observer.onCompleted();
            }
        }

        /**
         * 查询Leader
         *
         * @param request
         * @param observer
         */
        public void getLeader(GetLeaderRequest request, StreamObserver<GetLeaderReply> observer) {
            String groupId = request.getGroupId();
            try {
                PeerId peerId = engine.getLeader(groupId);

                if (peerId != null) {
                    GetLeaderReply reply = GetLeaderReply.newBuilder()
                            .setLeader(peerId.toString())
                            .setGroupId(groupId)
                            .build();
                    observer.onNext(reply);
                } else {
                    Metadata metadata = new Metadata();
                    metadata.put(Metadata.Key.of("key",Metadata.ASCII_STRING_MARSHALLER), "value");
                    observer.onError(io.grpc.Status.OUT_OF_RANGE.asException(metadata));
                }
            }catch (Exception e) {
                observer.onError(io.grpc.Status.fromThrowable(e).asException());
            }
            observer.onCompleted();
        }

        /**
         * 新增raft分组
         */
        public void startRaftNode(RaftNodeRequest request,
                                  io.grpc.stub.StreamObserver<RaftNodeReply> observer) {
            String groupId = request.getGroupId();
            String peersList = request.getPeers();
            try {
                // 创建本地raft分组
                engine.startRaftNode(groupId, peersList);
                RaftNodeProcessor.Request req = new RaftNodeProcessor.Request();
                req.setGroupId(groupId);
                req.setPeersList(peersList);
                RaftRpcClient client = new RaftRpcClient();
                client.init(new RpcOptions());
                for (String address : peersList.split(",")) {
                    client.addRaftNode(JRaftUtils.getEndPoint(address), req,
                            new RaftRpcClient.ClosureAdapter<RaftNodeProcessor.Response>() {
                                @Override
                                public void run(Status status) {
                                    try {
                                        System.out.println("Start raft node " + address);
                                        //    server.getRaftNode(groupId).addPeer(JRaftUtils.getPeerId(address), null);
                                    } catch (Exception e) {
                                        System.out.println(e.getMessage());
                                    }
                                }
                            });
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
            observer.onNext(RaftNodeReply.newBuilder().build());
            observer.onCompleted();
        }

        /**
         * 新增raft分组
         */
        public void stopRaftNode(RaftNodeRequest request,
                                  io.grpc.stub.StreamObserver<RaftNodeReply> observer) {
            String groupId = request.getGroupId();
            try {
                // 创建本地raft分组
                engine.stopRaftNode(groupId);
            } catch (Exception e) {
                e.printStackTrace();
            }
            observer.onNext(RaftNodeReply.newBuilder().build());
            observer.onCompleted();
        }



        public void addPeer(PeerRequest request,
                               io.grpc.stub.StreamObserver<PeerReply> observer) {
            String groupId = request.getGroupId();
            String address = request.getAddress();
            if (!engine.isLeader(groupId)) {
                observer.onError(io.grpc.Status.PERMISSION_DENIED.asException());
                observer.onCompleted();
                return;
            }
            try {
                engine.addPeer(groupId, address, new Closure() {
                    @Override
                    public void run(Status status) {
                        System.out.println(groupId + " addPeer " + address + " " + status);
                        observer.onNext(PeerReply.newBuilder().build());
                        observer.onCompleted();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
                observer.onNext(PeerReply.newBuilder().build());
                observer.onCompleted();

            }
        }
        public void removePeer(PeerRequest request, StreamObserver<PeerReply> observer) {
            String groupId = request.getGroupId();
            String address = request.getAddress();
            if (!engine.isLeader(groupId)) {
                observer.onError(io.grpc.Status.PERMISSION_DENIED.asException());
                observer.onCompleted();
                return;
            }
            try {
                engine.removePeer(groupId, address, new Closure() {
                    @Override
                    public void run(Status status) {
                        System.out.println(groupId + " removePeer " + address + " " + status);
                        observer.onNext(PeerReply.newBuilder().build());
                        observer.onCompleted();
                    }
                });

            } catch (Exception e) {
                e.printStackTrace();
                observer.onNext(PeerReply.newBuilder().build());
                observer.onCompleted();
            }
        }
        /**
         */
        public void changePeers(PeerRequest request,
                                StreamObserver<PeerReply> observer) {
            String groupId = request.getGroupId();
            String address = request.getAddress();
            if (!engine.isLeader(groupId)) {
                observer.onError(io.grpc.Status.PERMISSION_DENIED.asException());
                observer.onCompleted();
                return;
            }
            try {
                engine.changePeers(groupId, request.getAddress(), request.getLearner(), new Closure() {
                    @Override
                    public void run(Status status) {
                        System.out.println(groupId + " changePeers " + address + " " + status);
                        observer.onNext(PeerReply.newBuilder().build());
                        observer.onCompleted();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
                observer.onNext(PeerReply.newBuilder().build());
                observer.onCompleted();

            }
        }

        public void transferLeader(PeerRequest request,
                                   io.grpc.stub.StreamObserver<PeerReply> observer) {
            String address = request.getAddress();
            String groupId = request.getGroupId();

            try {
                engine.transferLeader(groupId, address);
                observer.onNext(PeerReply.newBuilder().build());
                observer.onCompleted();
            } catch (Exception e) {
                e.printStackTrace();
                observer.onError(e);
                observer.onCompleted();
            }
        }

        public void setMode(SetModeRequest request,
                            io.grpc.stub.StreamObserver<SetModeReply> observer) {
            String groupId = request.getGroupId();
            int mode = request.getMode();
            try {
                engine.setRaftLogMode(groupId, mode == 1);
            } catch (Exception e) {
                e.printStackTrace();
            }
            observer.onNext(SetModeReply.newBuilder().build());
            observer.onCompleted();
        }

        AtomicLong logCounter = new AtomicLong(0);
        volatile long startTime = 0;
        /**
         * 流式传输测试
         *
         * @return
         */
        public io.grpc.stub.StreamObserver<ScanRequest> scan(
                io.grpc.stub.StreamObserver<ScanResponse> response) {
            startTime = System.currentTimeMillis();

            return new StreamObserver<ScanRequest>() {

                @Override
                public void onNext(ScanRequest request) {
                    String id = request.getId();
                    response.onNext(ScanResponse.newBuilder()
                            .setId(id)
                            .setData(ByteString.copyFromUtf8(id + " - reply "))
                            .build());
                    if ( logCounter.addAndGet(request.getDataCount()) > 10000000) {

                    }

                    response.onCompleted();
                }

                @Override
                public void onError(Throwable t) {
                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
                    System.out.println("onCompleted");
                }
            };
        }
        /**
         * 生成raft任务
         *
         * @param groupId
         * @param key
         * @param value
         * @param done
         * @throws Exception
         */
        protected void putTask(final String groupId, final byte[] key, final byte[] value, Closure done) throws Exception {
            Operation op = Operation.createPut(key, value);
            // 序列化
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeObject(op);
            os.close();

            // 创建并提交任务
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(bos.toByteArray()));
            task.setDone(new StoreClosure(op, done));
            this.engine.getRaftNode(groupId).apply(task);
        }

        protected void readIndex(String groupId){
            System.gc();
        }

    }

    static class RaftParams {
        public String dataPath;
        public String raftAddr;
        public String peersList;
        public String groupId;

        public RaftParams(String dataPath, String raftAddr, String peersList, String groupId) {
            this.dataPath = dataPath;
            this.raftAddr = raftAddr;
            this.peersList = peersList;
            this.groupId = groupId;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length < 5) {
            System.out.println("Useage : {dataPath} {grpcPort} {raftAddr} {peersList} {groupId}");
            System.out.println("Example:  /tmp/server1 127.0.0.1:8080 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083 g1");
            System.exit(1);
        }

        final String dataPath = args[0];
        final String grpcPort = args[1];
        final String raftAddr = args[2];
        final String peersList = args[3];
        final String groupId = args[4];

        System.out.println("Start grpc server raft addr is " + raftAddr + ", grpc port is " + grpcPort);
        final GrpcServer server = new GrpcServer();
        server.raftParams = new RaftParams(dataPath, raftAddr, peersList, groupId);
        new File(dataPath).mkdirs();
        server.start(Integer.valueOf(grpcPort));
        server.blockUntilShutdown();
    }
}
