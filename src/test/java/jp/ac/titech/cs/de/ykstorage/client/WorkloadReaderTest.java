package jp.ac.titech.cs.de.ykstorage.client;

import jp.ac.titech.cs.de.ykstorage.cli.Request;
import jp.ac.titech.cs.de.ykstorage.cli.WorkloadReader;

import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class WorkloadReaderTest {

    private static final String TEST_DATA = "WorkloadReaderTest.csv";

    private WorkloadReader workload;

    @Before
    public void setUp() {
        String fixturePath = this.getClass().getPackage().getName().replace('.','/') + "/" + TEST_DATA;
        workload = new WorkloadReader(this.getClass().getClassLoader().getResource(fixturePath).getPath());
    }

    @Test
    public void getArrivalTimeTest() {
        Request request = workload.getRequest();
        assertThat(request.getDelay(), is(1000L));

        request = workload.getRequest();
        assertThat(request.getDelay(), is(3000L));

        request = workload.getRequest();
        assertThat(request.getDelay(), is(500L));

        request = workload.getRequest();
        assertThat(request.getDelay(), is(1000L));
    }

    @Test
    public void getRequestTypeTest() {
        byte[] request = workload.getRequest().getRequest();
        // assert get(read) request type
        assertThat(request[0], is((byte) 0x00));
        assertThat(request[1], is((byte) 0x10));

        request = workload.getRequest().getRequest();
        // assert put(write) request type
        assertThat(request[0], is((byte) 0x00));
        assertThat(request[1], is((byte) 0x01));

        request = workload.getRequest().getRequest();
        // assert get(read) request type
        assertThat(request[0], is((byte) 0x00));
        assertThat(request[1], is((byte) 0x10));

        request = workload.getRequest().getRequest();
        // assert put(write) request type
        assertThat(request[0], is((byte) 0x00));
        assertThat(request[1], is((byte) 0x01));
    }
}
