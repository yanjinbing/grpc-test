package org.example.rpc;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.option.CliOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.impl.cli.CliClientServiceImpl;

public class RpcClient {
    public void addRaftNode(Node leader) throws RemotingException, InterruptedException {
        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new CliOptions());
        final AddRaftNodeProcessor.Request request = new AddRaftNodeProcessor.Request();
        request.graphName = "test";
        request.groupId = "a1";
        cliClientService.getRpcClient().invokeAsync(leader.getLeaderId().getEndpoint(), request,
                new InvokeCallback() {
                    @Override
                    public void complete(Object result, Throwable err) {
                        System.out.println("recv response " + result);
                        if ( err != null )
                            err.printStackTrace();

                    }
                }, 5000);
    }
}
