package org.example;


import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.DefaultJRaftServiceFactory;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.storage.SnapshotStorage;
import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import io.grpc.stub.StreamObserver;
import org.example.grpc.*;
import org.example.rpc.RaftNodeProcessor;
import org.example.rpc.RaftRpcClient;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Hello world!
 */
public class GrpcServer {
    private Server grpcServer;
    // raft状态机
    private RaftParams raftParams;

    private PeerId serverId;
    private RpcServer rpcServer;
    private Map<String, Node> raftNodes = new ConcurrentHashMap<>();

    /**
     * 启动grpc服务
     */
    private void start(int port) throws IOException {
        // 启动grpc服务
        grpcServer = ServerBuilder.forPort(port)
                .addService(new HelloWorldImpl(this))
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
        createRaftRpcServer(raftParams.raftAddr);
        startRaft(raftParams.groupId, raftParams.dataPath, raftParams.peersList);
    }

    private void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
    }

    /**
     * 创建raft rpc server
     */
    private void createRaftRpcServer(String raftAddr) {
        serverId = JRaftUtils.getPeerId(raftParams.raftAddr);
        rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint());
        // 注册增加Raft node消息
        rpcServer.registerProcessor(new RaftNodeProcessor(this));
        rpcServer.init(null);
    }

    /**
     * 创建raft分组
     *
     * @param groupId
     */
    public void startRaftNode(String groupId, String peersList) {
        if (raftNodes.containsKey(groupId))
            return;
        String raftPath = raftParams.dataPath + "/" + groupId;
        new File(raftPath).mkdirs();

        startRaft(groupId, raftPath, peersList);
    }

    /**
     * 创建raft分组
     *
     * @param groupId
     */
    public void stopRaftNode(String groupId) {
        if (!raftNodes.containsKey(groupId))
            return;
        Node node = raftNodes.remove(groupId);
        node.shutdown();

    }

    /**
     * 创建raft分组
     *
     * @param dataPath  存储路
     * @param peersList 集群地址
     */
    public void startRaft(String groupId, String dataPath, String peersList) {
        System.out.println("Start raft " + groupId + " peers " + peersList);
        String logPath = dataPath + "/log/" + groupId;
        String metaPath = dataPath + "/meta/" + groupId;
        String snapPath = dataPath + "/snapshot/" + groupId;
        new File(logPath).mkdirs();
        new File(metaPath).mkdirs();
        new File(snapPath).mkdirs();
        // 创建状态机
        StateMachineImpl stateMachine = new StateMachineImpl(groupId);
        Configuration initConf = new Configuration();
        initConf.parse(peersList);
        // 设置Node参数，包括日志存储路径和状态机实例
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        // 日志路径
        nodeOptions.setLogUri(logPath);
        // raft元数据路径
        nodeOptions.setRaftMetaUri(metaPath);
        // 快照路径
        nodeOptions.setSnapshotUri(snapPath);
        // 初始集群
        nodeOptions.setInitialConf(initConf);
        // 快照时间间隔
        nodeOptions.setSnapshotIntervalSecs(10);


        nodeOptions.setServiceFactory(new DefaultJRaftServiceFactory(){
             @Override
            public SnapshotStorage createSnapshotStorage(final String uri, final RaftOptions raftOptions) {
                return new SnapshotStorageImpl(uri, raftOptions);
            }
            /*
            @Override
            public LogStorage createLogStorage(final String uri, final RaftOptions raftOptions) {
                return new LogStorageImpl(uri, raftOptions);
            }*/
        });
        // 构建raft组并启动raft
        RaftGroupService raftGroupService = new RaftGroupService(groupId, serverId, nodeOptions, rpcServer, true);
        Node raftNode = raftGroupService.start(false);
        stateMachine.setNode(raftNode);
        raftNodes.put(groupId, raftNode);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            raftNode.shutdown();
        }));


    }
    public Node getRaftNode(String groupId) throws Exception {
        if (!raftNodes.containsKey(groupId))
            throw new Exception("Do not found " + groupId);
        return raftNodes.get(groupId);
    }

    public void addPeer(String groupId, String peer, final Closure done) throws Exception {
        System.out.println(groupId + " addPeer " + peer);
        Node node = getRaftNode(groupId);
        PeerId peerId = JRaftUtils.getPeerId(peer);
        node.addPeer(peerId, done);
    }

    public void removePeer(String groupId, String peer, final Closure done) throws Exception {
        System.out.println(groupId + " removePeer " + peer);
        Node node = getRaftNode(groupId);
        PeerId peerId = JRaftUtils.getPeerId(peer);
        node.removePeer(peerId, done);
    }

    /**
     * 获取Leader
     */
    public PeerId getLeader(String groupId) throws Exception {
        return getRaftNode(groupId).getLeaderId();
    }

    public void setMode(String groupId, int mode) throws Exception {
        Node node = getRaftNode(groupId);

    }
    /**
     * 当前peer是否是leader
     */
    public boolean isLeader(String groupId) {
        try {
            return getRaftNode(groupId).isLeader();
        } catch (Exception e) {
            return false;
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
        private GrpcServer server;

        public HelloWorldImpl(GrpcServer server) {
            this.server = server;
        }

        //实现grpc方法
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> observer) {
            ByteString key = request.getKey();
            ByteString value = request.getValue();

            //创建任务，发送给其他peer
            String groupId = request.getGroupId();

            if (!server.isLeader(groupId)) {
                observer.onError(io.grpc.Status.ABORTED.asException());
                observer.onCompleted();
                return;
            }
            try {
                System.out.println("Recv data " + groupId);
                // 提交raft 任务
                putTask(groupId, request.getKey().toByteArray(), request.getValue().toByteArray(),
                        new Closure() {
                            @Override
                            public void run(Status status) {
                                observer.onNext(HelloReply.newBuilder().setMessage("response " + request.getKey()).build());
                                observer.onCompleted();
                            }
                        });
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
                PeerId peerId = server.getLeader(groupId);

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
                server.startRaftNode(groupId, peersList);
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
                server.stopRaftNode(groupId);
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
            if (!server.isLeader(groupId)) {
                observer.onError(io.grpc.Status.PERMISSION_DENIED.asException());
                observer.onCompleted();
                return;
            }
            try {
                server.addPeer(groupId, address, new Closure() {
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
            if (!server.isLeader(groupId)) {
                observer.onError(io.grpc.Status.PERMISSION_DENIED.asException());
                observer.onCompleted();
                return;
            }
            try {
                server.removePeer(groupId, address, new Closure() {
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

        public void setMode(SetModeRequest request,
                            io.grpc.stub.StreamObserver<SetModeReply> responseObserver) {
            String groupId = request.getGroupId();
            int mode = request.getMode();



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
            this.server.getRaftNode(groupId).apply(task);
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
            System.out.println("Example:  /tmp/server1 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083 g1");
            System.exit(1);
        }

        final String dataPath = args[0];
        final String grpcPort = args[1];
        final String raftAddr = args[2];
        final String peersList = args[3];
        final String groupId = args[4];
        //groupCount = 0;


        System.out.println("Start grpc server raft addr is " + raftAddr + ", grpc port is " + grpcPort);
        final GrpcServer server = new GrpcServer();
        server.raftParams = new RaftParams(dataPath, raftAddr, peersList, groupId);
        new File(dataPath).mkdirs();
        server.start(Integer.valueOf(grpcPort));
        server.blockUntilShutdown();
    }
}
