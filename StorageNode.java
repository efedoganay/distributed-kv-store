import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.transport.layered.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;
import org.apache.curator.framework.api.*;

import org.apache.log4j.*;

public class StorageNode {
    static Logger log;

    public static void setupWatcher(CuratorFramework curClient, String parentPath, String myPath, Logger log, KeyValueHandler handler) throws Exception {
        curClient.sync();
        
        List<String> children = curClient.getChildren().forPath(parentPath);
        Collections.sort(children);
        String myNode = ZKPaths.getNodeFromPath(myPath);
        int myIndex = children.indexOf(myNode);
        
        if (myIndex == -1) {
            Thread.sleep(100);
            curClient.sync();
            children = curClient.getChildren().forPath(parentPath);
            Collections.sort(children);
            myIndex = children.indexOf(myNode);
            if (myIndex == -1) {
                throw new Exception("My node not found in ZooKeeper children list");
            }
        }
        
        CuratorWatcher childrenWatcher = new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) throws Exception {
                if (event.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                    try {
                        setupWatcher(curClient, parentPath, myPath, log, handler);
                    } catch (Exception e) {
                        //log.error("Error in setupWatcher after children change", e);
                    }
                }
            }
        };
        
        if (myIndex == 0) {
            handler.setPrimary(true);

            if (children.size() > 1) {
                String backupChild = children.get(1);
                String backupPath = parentPath + "/" + backupChild;
                byte[] data = curClient.getData().forPath(backupPath);
                String strData = new String(data);
                String[] parts = strData.split(":");
                InetSocketAddress backupAddr = new InetSocketAddress(parts[0], Integer.parseInt(parts[1]));

                handler.setBackupAddr(backupAddr);
                //log.info("Backup address set to: " + backupAddr);
            } else {
                handler.setBackupAddr(null);
                //log.info("No backup available yet");
            }

            curClient.getChildren().usingWatcher(childrenWatcher).forPath(parentPath);
            return;
        }

        handler.setPrimary(false);
        //log.info("I AM BACKUP");
        
        InetSocketAddress primaryAddr = null;
        boolean syncSuccess = false;
        
        for (int i = 0; i < myIndex; i++) {
            String candidateChild = children.get(i);
            String candidatePath = parentPath + "/" + candidateChild;
            
            try {
                byte[] pdata = curClient.getData().forPath(candidatePath);
                String pstr = new String(pdata);
                String[] pparts = pstr.split(":");
                InetSocketAddress candidateAddr = new InetSocketAddress(pparts[0], Integer.parseInt(pparts[1]));
                
                try {
                    //log.info("Attempting to sync from candidate " + candidateAddr);
                    handler.initialSyncFromPrimary(candidateAddr);
                    //log.info("Synced full state from primary " + candidateAddr);
                    
                    primaryAddr = candidateAddr;
                    syncSuccess = true;
                    break;
                } catch (Exception e) {
                    //log.warn("Could not sync from candidate " + candidateAddr + " (might be dead): " + e.getMessage());
                }
            } catch (Exception e) {
                //log.warn("Could not get data for candidate " + candidatePath + ": " + e.getMessage());
                continue;
            }
        }

        curClient.getChildren().usingWatcher(childrenWatcher).forPath(parentPath);
        
        if (syncSuccess && primaryAddr != null) {
            String predecessorPath = null;
            for (int i = 0; i < myIndex; i++) {
                String candidateChild = children.get(i);
                String candidatePath = parentPath + "/" + candidateChild;
                try {
                    byte[] pdata = curClient.getData().forPath(candidatePath);
                    String pstr = new String(pdata);
                    String[] pparts = pstr.split(":");
                    InetSocketAddress candidateAddr = new InetSocketAddress(pparts[0], Integer.parseInt(pparts[1]));
                    if (candidateAddr.equals(primaryAddr)) {
                        predecessorPath = candidatePath;
                        break;
                    }
                } catch (Exception e) { }
            }
            
            if (predecessorPath != null) {
                final String finalPredecessorPath = predecessorPath;
                //log.info("I AM BACKUP, watching " + finalPredecessorPath);

                curClient.getData().usingWatcher((CuratorWatcher) event -> {
                    if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                        //log.info(finalPredecessorPath + " deleted. Rechecking...");
                        try {
                            setupWatcher(curClient, parentPath, myPath, log, handler);
                        } catch (Exception e) {
                            //log.error("Error in setupWatcher after predecessor delete", e);
                        }
                    }
                }).forPath(finalPredecessorPath);
            }
        }
    }

    public static void main(String [] args) throws Exception {
        BasicConfigurator.configure();
        log = Logger.getLogger(StorageNode.class.getName());
        Logger.getLogger("org.apache.zookeeper").setLevel(Level.WARN);
        Logger.getLogger("org.apache.curator").setLevel(Level.WARN);
        Logger.getLogger("org.apache.thrift").setLevel(Level.WARN);
    
        if (args.length != 4) {
            //System.err.println("Usage: java StorageNode host port zkconnectstring zknode");
            System.exit(-1);
        }
    
        CuratorFramework curClient =
            CuratorFrameworkFactory.builder()
            .connectString(args[2])
            .retryPolicy(new RetryNTimes(10, 1000))
            .connectionTimeoutMs(5000)
            .sessionTimeoutMs(1500)
            .build();
    
        curClient.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                curClient.close();
            }
        });
    
        KeyValueHandler handler = new KeyValueHandler(args[0], Integer.parseInt(args[1]), curClient, args[3]);
        KeyValueService.Processor<KeyValueService.Iface> processor = new KeyValueService.Processor<>(handler);
        TServerSocket socket = new TServerSocket(Integer.parseInt(args[1]));
        TThreadPoolServer.Args sargs = new TThreadPoolServer.Args(socket);
        sargs.protocolFactory(new TBinaryProtocol.Factory());
        
        int maxFrameSize = 256 * 1024 * 1024; 
        sargs.transportFactory(new TFramedTransport.Factory(maxFrameSize));
        
        sargs.processorFactory(new TProcessorFactory(processor));
        
        sargs.maxWorkerThreads(64); 
        
        TServer server = new TThreadPoolServer(sargs);
    
        new Thread(new Runnable() {
            public void run() {
                server.serve();
            }
        }).start();
    
        String myPath = curClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(args[3] + "/n_", (args[0] + ":" + args[1]).getBytes());
        //log.info("Registered in ZooKeeper: " + myPath);
        
        setupWatcher(curClient, args[3], myPath, log, handler);
    }
}