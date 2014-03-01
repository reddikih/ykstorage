package test.jp.ac.titech.cs.de.ykstorage.client;

import jp.ac.titech.cs.de.ykstorage.cli.Request;
import jp.ac.titech.cs.de.ykstorage.cli.WorkloadReader;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


@RunWith(JUnit4.class)
public class WorkloadReaderTest {
	private WorkloadReader workload;
	
	@Before
	public void setUp() {
		workload = new WorkloadReader("./test/test/jp/ac/titech/cs/de/ykstorage/client/WorkloadReaderTest.csv");
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
		assertThat(request[0], is((byte) 0x00));
		assertThat(request[1], is((byte) 0x10));
		
		request = workload.getRequest().getRequest();
		assertThat(request[0], is((byte) 0x00));
		assertThat(request[1], is((byte) 0x01));
		
		request = workload.getRequest().getRequest();
		assertThat(request[0], is((byte) 0x00));
		assertThat(request[1], is((byte) 0x10));
		
		request = workload.getRequest().getRequest();
		assertThat(request[0], is((byte) 0x00));
		assertThat(request[1], is((byte) 0x01));
	}
	
}
