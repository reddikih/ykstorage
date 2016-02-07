package jp.ac.titech.cs.de.ykstorage.frontend;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import jp.ac.titech.cs.de.ykstorage.service.StorageService;
import jp.ac.titech.cs.de.ykstorage.storage.DumStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrontEnd {

    private static Logger logger = LoggerFactory.getLogger(FrontEnd.class);

    private final Executor executor;
    private final ServerSocket socket;
    private final StorageService storageService;
    private final StorageManager storageManager;

    private volatile boolean isCanceled = false;

    public static FrontEnd getInstance(
            int port,
            StorageManager storageManager,
            StorageService storageService) throws IOException {
        return new FrontEnd(port, storageManager, storageService);
    }

    private FrontEnd(
            int port,
            StorageManager storageManager,
            StorageService storageService) throws IOException {
//        this.executor = Executors.newCachedThreadPool();
        this.executor = Executors.newFixedThreadPool(8);

        this.socket = new ServerSocket(port);
        this.storageService = storageService;
        this.storageManager = storageManager;

        logger.info("Host:{} Port:{}",
                socket.getInetAddress().toString(),
                socket.getLocalPort());
    }

    public void start() {
        if (this.socket == null)
            throw new IllegalStateException("Socket is not opened.");

        logger.trace("into the while loop");

        while (!isCanceled) {
            try {
                final Socket conn = this.socket.accept();
                logger.debug("connection accepted.");
                Runnable task = getTask(conn);
                executor.execute(task);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private Runnable getTask(Socket conn) throws IOException {
        Runnable result = null;

        ClientRequest request = parseRequest(conn);
        if (request == null)
            throw new IllegalStateException("couldn't parse client request.");

        if (request.getCommand().equals(RequestCommand.READ))
            result = new ClientReadTask(conn, request, this.storageManager);
        else if (request.getCommand().equals(RequestCommand.WRITE))
            result = new ClientWriteTask(conn, request, this.storageManager);
        else if (request.getCommand().equals(RequestCommand.EXIT)) {
            this.storageManager.shutdown();
            this.storageService.exit();
        }
        else
            throw new IllegalStateException("request command is invalid.: " + request.getCommand());

        return result;
    }

    private ClientRequest parseRequest(Socket conn) {
        ClientRequest result = null;
        try {
            result = new ClientRequest(conn);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public void cancel() {
        this.isCanceled = true;
    }
}
