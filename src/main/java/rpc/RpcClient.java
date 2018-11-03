package rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.net.InetSocketAddress;

public class RpcClient {

    public static final String ADDRESS = "localhost";
    public static final int PORT = 9010;

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        InetSocketAddress inetSocketAddress = new InetSocketAddress(ADDRESS, PORT);
        ClientProtocol client = RPC.getProxy(ClientProtocol.class, ClientProtocol.versionID,
                inetSocketAddress, conf);
        System.out.println(client.add(2, 3));
        System.out.println(client.echo("rpc!"));
    }
}
