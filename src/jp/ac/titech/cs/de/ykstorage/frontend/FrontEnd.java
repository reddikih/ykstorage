package jp.ac.titech.cs.de.ykstorage.frontend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class FrontEnd {

    private static Logger logger = LoggerFactory.getLogger(FrontEnd.class);

    private final int NUM_THREADS;
    private final Executor service;
    private final ServerSocket socket;

    public static FrontEnd getInstance(int threads, int port) throws IOException {
        return new FrontEnd(threads, port);
    }

    private FrontEnd(int threads, int port) throws IOException {
        this.NUM_THREADS = threads;
        this.service = Executors.newFixedThreadPool(this.NUM_THREADS);
        this.socket = new ServerSocket(port);
        logger.info("Host:{} Port:{} ThreadPool:{}",
                socket.getInetAddress().toString(),
                socket.getLocalPort(),
                threads);
    }

    public void start() {
        if (this.socket == null)
            throw new IllegalStateException("Socket is not opend.");

        logger.info("into the while loop");
        while (true) {
            try {
                final Socket conn = this.socket.accept();
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
                OutputStream out = conn.getOutputStream();
                byte[] payload = this.request.getPayload();

                ByteBuffer respBuff = ByteBuffer.allocate(14 + this.request.getLength());
                respBuff.putShort((short)200);
                respBuff.putLong(this.request.getKey());
                respBuff.putInt(this.request.getLength());
                if (this.request.getLength() > 0)
                    respBuff.put(this.request.getPayload());

                out.write(respBuff.array());
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
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
                OutputStream out = conn.getOutputStream();
                byte[] bytes = new byte[16];
                Arrays.fill(bytes, (byte)96);
                ByteBuffer buf = ByteBuffer.allocate(14 + 16);
                buf.putShort((short)200)
                   .putLong(this.request.getKey())
                   .putInt(16)

                   .put(bytes);
                out.write(buf.array());
                out.flush();
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int thread = 1;
        int port = 9999;
        FrontEnd server = FrontEnd.getInstance(1, 9999);
        logger.info("Starting FrontEnd");
        server.start();
    }
}
