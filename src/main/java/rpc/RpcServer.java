package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.*;

public class RpcServer {
    public static void main(String[] args) throws Exception {
        Server server = new RPC.Builder(new Configuration()).setProtocol(ClientProtocol.class)
                .setInstance(new ClientProtocolImpl()).setBindAddress("127.0.0.1").setPort(9010)
                .setNumHandlers(5).build();
        server.start();
    }
}
