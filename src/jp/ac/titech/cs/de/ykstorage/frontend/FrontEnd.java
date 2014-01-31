package jp.ac.titech.cs.de.ykstorage.frontend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
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
                byte[] response;
                if (payload != null) {
                    response = new byte[]{0x00,(byte)0xc8,0x00,0x00,0x00,0x00};
                } else {
                    response = new byte[]{0x01,(byte)0xf4,0x00,0x00,0x00,0x00};
                }
                out.write(response);
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

        }
    }

    public enum RequestCommand {
        READ,
        WRITE,
        DELETE,
    }

    private class RequestHeader {
        private RequestCommand command;
        private long key;
        private int length;

        RequestHeader(Socket sock) {
            parseHeader(sock);
        }

        public RequestCommand getCommand() {return this.command;}
        public long getKey() {return this.key;}
        public int getLength() {return this.length;}

        private void parseHeader(Socket sock) {
            try {
                InputStream in = sock.getInputStream();

                byte[] command = new byte[2];
                int readByte = in.read(command);
                if (readByte != command.length)
                    throw new IOException("couldn't read request command");

                if (command[0] == 0x00) {
                    if (command[1] == 0x01) this.command = RequestCommand.READ;
                    if (command[1] == 0x10) this.command = RequestCommand.WRITE;
                } else if (command[0] == 0x01) {
                    if (command[1] == 0x00) this.command = RequestCommand.DELETE;
                } else {
                    throw new IllegalStateException("request command is invalid.: " + ((int)command[0] + (int)command[1]));
                }

                logger.debug("Requested Command: {}", this.command);

                // get key(8bytes)
                long key = 0L;
                int k = 64, offset = 8;
                byte[] keyVal = new byte[8];
                readByte = in.read(keyVal);
                if (readByte != keyVal.length)
                    throw new IOException("couldn't read request command");

                for (int i = 0; i < keyVal.length; i++)
                    this.key = (this.key << 8) + (keyVal[i] & 0xff);

                logger.debug("Request Key: {}", this.key);

                //get length(4bytes)
                k = 32;
                byte[] lengthVal = new byte[4];
                readByte = in.read(lengthVal);
                if (readByte != lengthVal.length)
                    throw new IOException("couldn't read request command");

                for (int i = 0; i < lengthVal.length; i++)
                    this.length = (this.length << 8) + (lengthVal[i] & 0xff);

                logger.debug("Request Length: {}", this.length);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private class ClientRequest {
        private final RequestHeader header;
        private byte[] payload;

        protected ClientRequest(RequestHeader header, byte[] payload) {
            this.header = header;
            this.payload = payload;
        }

        public RequestCommand getCommand() { return this.header.getCommand(); }
        public long getKey() { return this.header.getKey(); }
        public int getLength() { return this.header.getLength(); }
        public byte[] getPayload() { return this.payload; }
    }

    public static void main(String[] args) throws IOException {
        int thread = 1;
        int port = 9999;
        FrontEnd server = FrontEnd.getInstance(1, 9999);
        logger.info("Starting FrontEnd");
        server.start();
    }
}
