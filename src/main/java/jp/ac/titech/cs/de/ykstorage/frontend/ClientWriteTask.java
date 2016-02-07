package jp.ac.titech.cs.de.ykstorage.frontend;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import jp.ac.titech.cs.de.ykstorage.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientWriteTask implements Runnable {

    private final Logger logger = LoggerFactory.getLogger(ClientWriteTask.class);

    private final Socket conn;
    private final ClientRequest request;
    private final StorageManager storageManager;

    public ClientWriteTask(
            Socket conn,
            ClientRequest request,
            StorageManager storageManager) {
        this.conn = conn;
        this.request = request;
        this.storageManager = storageManager;
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
        } catch (Exception e) {
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
