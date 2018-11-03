package rpc;

import org.apache.hadoop.ipc.ProtocolSignature;

import java.io.IOException;

public class ClientProtocolImpl implements ClientProtocol {

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) {
        return ClientProtocol.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int clientMethodsHash) {
        return new ProtocolSignature(ClientProtocol.versionID, null);
    }

    public String echo(String value) throws IOException {
        return value;
    }

    public int add(int v1, int v2) throws IOException {
        return v1 + v2;
    }
}
