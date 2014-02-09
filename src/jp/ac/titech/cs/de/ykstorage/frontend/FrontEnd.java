package jp.ac.titech.cs.de.ykstorage.frontend;

import jp.ac.titech.cs.de.ykstorage.storage.DumStorageManager;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class FrontEnd {

    private static Logger logger = LoggerFactory.getLogger(FrontEnd.class);

    private final Executor service;
    private final ServerSocket socket;
    private final StorageManager storageManager;

    public static FrontEnd getInstance(int port, StorageManager storageManager) throws IOException {
        return new FrontEnd(port, storageManager);
    }

    private FrontEnd(int port, StorageManager storageManager) throws IOException {
        this.service = Executors.newCachedThreadPool();
        this.socket = new ServerSocket(port);
        this.storageManager = storageManager;

        logger.info("Host:{} Port:{}",
                socket.getInetAddress().toString(),
                socket.getLocalPort());
    }

    public void start() {
        if (this.socket == null)
            throw new IllegalStateException("Socket is not opend.");

        logger.info("into the while loop");
        while (true) {
            try {
                final Socket conn = this.socket.accept();
                logger.debug("connection accepted.");
                Runnable task = getTask(conn);
                service.execute(task);
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
            result = new ReadTask(conn, request);
        else if (request.getCommand().equals(RequestCommand.WRITE))
            result = new WriteTask(conn, request);
        else
            throw new IllegalStateException("request command is invalid.: " + request.getCommand());

        return result;
    }

    private ClientRequest parseRequest(Socket conn) {
        ClientRequest result = null;
        RequestHeader header = new RequestHeader(conn);

        if (header.getCommand().equals(RequestCommand.READ) ||
            header.getCommand().equals(RequestCommand.DELETE))
            return new ClientRequest(header, null);

        // extract payload for write request
        try {
            InputStream in = conn.getInputStream();

            int length = header.getLength();
            byte[] payload = new byte[length];
            int readBytes = in.read(payload);
            if (readBytes != payload.length)
                throw new IOException("request payload is incorrected.");

            result = new ClientRequest(header, payload);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    private class WriteTask implements Runnable {
        private final Socket conn;
        private final ClientRequest request;

        protected WriteTask(Socket conn, ClientRequest request) {
            this.conn = conn;
            this.request = request;
        }
        @Override
        public void run() {
            logger.info("starting write task Request Key:{}", request.getKey());

            try {
                boolean result = storageManager.write(
                        this.request.getKey(), this.request.getPayload());

                OutputStream out = conn.getOutputStream();
                ByteBuffer responseBuf = ByteBuffer.allocate(14);
                if (result) responseBuf.putShort((short)200);
                else responseBuf.putShort((short)500);

                responseBuf.putLong(this.request.getKey())
                           .putInt(this.request.getLength());
                out.write(responseBuf.array());
                out.flush();
                logger.info("finished Write task Request Key:{}", request.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    this.conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class ReadTask implements Runnable {
        private final Socket conn;
        private final ClientRequest request;

        protected ReadTask(Socket conn, ClientRequest request) {
            this.conn = conn;
            this.request = request;
        }
        @Override
        public void run() {
            logger.info("starting Read task Request Key:{}", request.getKey());

            try {
                byte[] result = storageManager.read(this.request.getKey());
                if (result == null) result = new byte[0];

                OutputStream out = conn.getOutputStream();

                ByteBuffer responseBuf = ByteBuffer.allocate(14 + result.length);
                if (result.length > 0) responseBuf.putShort((short)200);
                else responseBuf.putShort((short)500);
                responseBuf.putLong(this.request.getKey())
                           .putInt(result.length)
                           .put(result);
                out.write(responseBuf.array());
                out.flush();
                logger.info("finished Read task Request Key:{}", request.getKey());
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    this.conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int port = 9999;
        FrontEnd server = FrontEnd.getInstance(port, new DumStorageManager(null,null,null,null));
        logger.info("Starting FrontEnd as a DumServer");
        server.start();
    }
}
