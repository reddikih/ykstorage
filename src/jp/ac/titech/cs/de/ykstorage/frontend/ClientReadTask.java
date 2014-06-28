package jp.ac.titech.cs.de.ykstorage.frontend;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientReadTask implements Runnable {

    private final static Logger logger = LoggerFactory.getLogger(ClientReadTask.class);

    private final Socket conn;
    private final ClientRequest request;
    private final StorageManager storageManager;

    public ClientReadTask(
            Socket conn,
            ClientRequest request,
            StorageManager storageManager) {
        this.conn = conn;
        this.request = request;
        this.storageManager = storageManager;
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
