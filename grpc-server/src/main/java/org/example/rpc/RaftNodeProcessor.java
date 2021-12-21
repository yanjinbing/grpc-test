package org.example.rpc;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import org.example.GrpcServer;
import org.example.RaftEngine;

import java.io.Serializable;

/**
 * 增加新的Raft Node

 */
public class RaftNodeProcessor implements RpcProcessor<RaftNodeProcessor.Request> ,Serializable
{
    private RaftEngine server;
    public RaftNodeProcessor(RaftEngine server){
        this.server = server;
    }

    public static class Request implements Serializable {
        public String getGraphName() {
            return graphName;
        }

        public void setGraphName(String graphName) {
            this.graphName = graphName;
        }

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public void setOp(int op) { this.op = op;}
        public int getOp(){ return op;}
        String graphName;
        String groupId;
        int op;

        public String getPeersList() {
            return peersList;
        }

        public void setPeersList(String peersList) {
            this.peersList = peersList;
        }

        String peersList;
    }

    public static class Response implements Serializable{
        int code;

        public int getCode() {
            return code;
        }

        public String getErrorMsg() {
            return errorMsg;
        }

        String errorMsg;
        public Response(int code, String error){
            this.code = code;
            this.errorMsg = error;
        }
    }
    @Override
    public void handleRequest(RpcContext rpcCtx, Request request) {
        System.out.println("recv add raft node " + request.groupId + " peers " + request.getPeersList());
        server.startRaftNode(request.groupId, request.getPeersList());
        rpcCtx.sendResponse(new Response(0, request.graphName));
    }

    @Override
    public String interest() {
        return RaftNodeProcessor.Request.class.getName();
    }
}
