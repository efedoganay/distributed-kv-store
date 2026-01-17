import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*; 

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;

import java.net.InetSocketAddress;

import com.google.common.util.concurrent.Striped;

public class KeyValueHandler implements KeyValueService.Iface {
    private ConcurrentHashMap<String, String> myMap;
    
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;
    private volatile boolean iAmPrimary = false;
    
    private final Striped<Lock> keyLocks = Striped.lock(128);
    private volatile boolean incomingSyncInProgress = false;
    private volatile boolean outgoingSyncInProgress = false;

    
    private volatile InetSocketAddress backupAddr = null;

    private final Set<String> keysUpdatedDuringSync = ConcurrentHashMap.newKeySet();
    private final Set<String> keysUpdatedDuringOutgoingSync = ConcurrentHashMap.newKeySet();

    private final ThreadLocal<KeyValueService.Client> threadLocalClient = new ThreadLocal<>();
    private final ThreadLocal<TTransport> threadLocalTransport = new ThreadLocal<>();
    private final ThreadLocal<InetSocketAddress> threadLocalBackupAddr = new ThreadLocal<>();

    private int maxFrameSize = 256 * 1024 * 1024; 

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;
        this.myMap = new ConcurrentHashMap<String, String>();    
    }

    private KeyValueService.Client getPooledBackupClient(InetSocketAddress addr) throws TException {
        KeyValueService.Client client = threadLocalClient.get();
        TTransport transport = threadLocalTransport.get();
        InetSocketAddress cachedAddr = threadLocalBackupAddr.get();

        if (client == null || transport == null || !transport.isOpen() || !addr.equals(cachedAddr)) {
            if (transport != null) {
                try { transport.close(); } catch (Exception e) {}
            }
            TSocket sock = new TSocket(addr.getHostName(), addr.getPort());
            sock.setSocketTimeout(60000);
            

            transport = new TFramedTransport(sock, maxFrameSize);
            
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            client = new KeyValueService.Client(protocol);
            
            threadLocalClient.set(client);
            threadLocalTransport.set(transport);
            threadLocalBackupAddr.set(addr);
        }
        return client;
    }
    
    public KeyValueService.Client getNewBackupClient(InetSocketAddress addr) throws TException {
        TSocket sock = new TSocket(addr.getHostName(), addr.getPort());
        sock.setSocketTimeout(60000);
        TTransport transport = new TFramedTransport(sock, maxFrameSize);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
    }

    public void initialSyncFromPrimary(InetSocketAddress primaryAddr) throws TException {
        KeyValueService.Client c = null;
        TTransport transport = null;
        Map<String, String> all = null;
        
        incomingSyncInProgress = true;
        keysUpdatedDuringSync.clear();

        try {
            c = getNewBackupClient(primaryAddr);
            transport = c.getInputProtocol().getTransport();
            all = c.dump();
        } catch (Exception e) {
            throw e;
        } finally {
            if (transport != null) transport.close();
        }

            for (Map.Entry<String, String> entry : all.entrySet()) {
                String key = entry.getKey();
                if (!keysUpdatedDuringSync.contains(key)) {
                    myMap.put(key, entry.getValue());
                }
            }
            incomingSyncInProgress = true;
    }

    public void setPrimary(boolean iAmPrimary){
        this.iAmPrimary = iAmPrimary;
    }

    public void setBackupAddr(InetSocketAddress addr){
        this.backupAddr = addr;
    }


    public String get(String key) throws org.apache.thrift.TException {
        if(!iAmPrimary) throw new TException("Not Primary");
        
        String ret = myMap.get(key);
        return (ret == null) ? "" : ret;
    }

    public void put(String key, String value) throws org.apache.thrift.TException {
        if(!iAmPrimary) throw new TException("Not Primary");

            Lock keyLock = keyLocks.get(key);
            keyLock.lock();
            try {
                if(!iAmPrimary) throw new TException("Not Primary");

                if (outgoingSyncInProgress) {
                    keysUpdatedDuringOutgoingSync.add(key);
                }

                InetSocketAddress b = backupAddr;
                if (b != null) {
                    try {
                        KeyValueService.Client backupClient = getPooledBackupClient(b);
                        backupClient.replicate(key, value);
                    } catch (Exception e) {
                        threadLocalClient.remove();
                        threadLocalTransport.remove();
                        threadLocalBackupAddr.remove();
                        backupAddr = null; 
                    }
                }
                myMap.put(key, value);
            } finally {
                keyLock.unlock();
            }
        } 

    public void replicate(String key, String value) throws org.apache.thrift.TException {
        Lock keyLock = keyLocks.get(key);
        keyLock.lock();
        try {
            if (incomingSyncInProgress) {
                keysUpdatedDuringSync.add(key);
            }
            myMap.put(key, value);
        } finally {
            keyLock.unlock();
        }
    }

    public Map<String, String> dump() throws org.apache.thrift.TException {
        return new HashMap<>(myMap);
    }
}