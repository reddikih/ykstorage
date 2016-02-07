package jp.ac.titech.cs.de.ykstorage.frontend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;

public class RequestHeader {

    private static final Logger logger = LoggerFactory.getLogger(RequestHeader.class);

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
                if (command[1] == 0x01) {
                    this.command = RequestCommand.READ;
                } else if (command[1] == 0x10) {
                    this.command = RequestCommand.WRITE;
                } else if (command[1] == 0x11) {
                    this.command = RequestCommand.EXIT;
                    return;
                }
            } else if (command[0] == 0x01) {
                if (command[1] == 0x00) this.command = RequestCommand.DELETE;
            } else {
                throw new IllegalStateException("request command is invalid.: " + ((int)command[0] + (int)command[1]));
            }

            byte[] keyVal = new byte[8];
            if (in.available() < 1) throw new IOException("reading [read request key value] is not available.");
            readByte = in.read(keyVal);
            if (readByte != keyVal.length)
                throw new IOException("couldn't read request key value");
            for (byte b : keyVal)
                this.key = (this.key << 8) + (b & 0xff);

            byte[] lengthVal = new byte[4];
            if (in.available() < 1) throw new IOException("reading [request length] is not available.");
            readByte = in.read(lengthVal);
            if (readByte != lengthVal.length)
                throw new IOException("couldn't read request length");
            for (byte b : lengthVal)
                this.length = (this.length << 8) + (b & 0xff);

            logger.trace("Request command:{}, key:{}, length:{}", getCommand(), getKey(), getLength());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
