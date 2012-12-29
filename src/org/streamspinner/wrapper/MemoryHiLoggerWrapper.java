package org.streamspinner.wrapper;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.streamspinner.DataTypes;
import org.streamspinner.StreamSpinnerException;
import org.streamspinner.engine.OnMemoryTupleSet;
import org.streamspinner.engine.Schema;
import org.streamspinner.engine.Tuple;
import org.streamspinner.engine.TupleSet;
import org.streamspinner.query.ORNode;


public class MemoryHiLoggerWrapper extends Wrapper implements Runnable {

	public static String PARAMETER_HOSTNAME = "hostname";	// MemoryHiLogger��IP�A�h���X
	public static String PARAMETER_PORT = "port";	// MemoryHiLogger�̃|�[�g�ԍ�
	public static String PARAMETER_INTERVAL = "interval";	// �������n�C���K�[�̑���Ԋu(10, 50, 100ms)
	
	public static final int INTERVAL2 = 1000;	// �f�[�^�̎擾�Ԋu(TODO) �x�������Ȃ�����悤�ɓ��I�ɐݒ�
	public static final int DATANUM = 100;	// (FIXME) xml�̃p�����[�^���瓮�I�ɁB50�̎�20�B10�̎�100
	
	// �v������`�����l�����A���j�b�g��
	// �ύX����ꍇ�̓��K�[���[�e�B���e�B�Ń������n�C���K�[���Đݒ肷��K�v������
	// ���j�b�g���ɕʁX�̃`�����l������ݒ�ł��Ȃ�
	static final int MAX_CH = 14;	// �ő�14ch
	static final int MAX_UNIT = 6;	// �ő�6unit
	
	private String[] table = null;
	private String hostname = null;
	private int port = 0;
	private long interval = 0;
	private int datanum = 1;	// 1��̎擾�f�[�^���B�ő�255(0xff)
	private Schema schema = null;
	private ArrayList<Schema> schemaList = new ArrayList<Schema>();
	private Thread monitor = null;
	private boolean connecting = false;
	private Socket theSocket = null;
	private InputStream is = null;
	private InputStreamReader isr = null;
	private BufferedReader br = null;
	private BufferedInputStream bis;
	private OutputStream os = null;
	
	private long getDataNum = 0;
	
	private ArrayList<ArrayList> volt = new ArrayList<ArrayList>();	// �擾�����d��
	private ArrayList<ArrayList> power = new ArrayList<ArrayList>();	// �d������v�Z��������d��
	
	private byte[] req;	// �f�[�^�v���R�}���h
	private byte[] raw;	// �d���̐��f�[�^

	
	public MemoryHiLoggerWrapper(String name) throws StreamSpinnerException {
		super(name);
		
		// �X�L�[�}�̍쐬
		table = new String[MAX_UNIT];
		for(int i = 0; i < MAX_UNIT; i++) {
			table[i] = "Unit" + (i + 1);
			volt.add(new ArrayList<Double>());
			power.add(new ArrayList<Double>());
		}
		
		for(int unit = 0; unit < MAX_UNIT; unit++) {
//			String[] attrs = new String[MAX_CH / 2 + 1];//TODO attrs�̃T�C�Y��unit��*7�i�f�B�X�N���j�傫��
//			String[] types = new String[MAX_CH / 2 + 1];//��Ɠ��l
			String[] attrs = new String[(MAX_CH / 2) * MAX_UNIT + 1];//attrs�̃T�C�Y��unit��*7�i�f�B�X�N���j�傫��
			String[] types = new String[(MAX_CH / 2) * MAX_UNIT + 1];//��Ɠ��l
			attrs[0] = table[unit] + ".Timestamp";
			types[0] = DataTypes.LONG;
			for(int i = 1; i < attrs.length; i++) {
				attrs[i] = table[unit] + ".Power" + i;
				types[i] = DataTypes.DOUBLE;
			}
			schemaList.add(new Schema(table[unit], attrs, types));
			schemaList.get(unit).setTableType(Schema.STREAM);
		}
	}

	// �X���b�h�X�^�[�g��
	public void run() {
		Thread current = Thread.currentThread();
		
		// �ڑ��J�n
		while(monitor != null && monitor.equals(current) && connecting == false) {
			try {
				startConnection();
			} catch(Exception err) {
				System.err.println(err.getMessage());
				stopConnection();
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		
		// �^�v���ǉ�
		while(monitor != null && monitor.equals(current) && connecting == true) {
			try {
				long executiontime = System.currentTimeMillis();
				long before = executiontime;
				byte[] rec = command(req);	// �f�[�^�v���R�}���h
				
				for(int i = 0; i < datanum; i++) {
					getData();
				}
				
				//System.out.println("---------------");
				// �f�B�X�N�䐔���̏���d�͂��^�v���ɒǉ�����
				// �O���Ń^�v�������
				// ���j�b�g���Ƃ̃^�C���X�^���v�����낦��K�v�����邩������Ȃ�
				OnMemoryTupleSet ts = new OnMemoryTupleSet(schemaList.get(0));//TODO get(unit)->unit=0�ł�邱�Ƃɂ���
				for(int i = 0; i < DATANUM; i++) {
					addTuple(executiontime, ts);
					executiontime += interval;
				}
				ts.beforeFirst();
				deliverTupleSet(executiontime - interval, table[0], ts);//TODO table[unit]->unit=0�ł�邱�Ƃɂ���
				
				getDataNum += DATANUM;
				long after = System.currentTimeMillis();
				
				//System.out.println("memory: " + getNumData(rec));
				//System.out.println("get: " + getDataNum);
				//System.out.println("INTERVAL2: " + INTERVAL2);
				//System.out.println("after: " + after);
				//System.out.println("before: " + executiontime);
				//System.out.println("sleeptime: " + (INTERVAL2 - (after - before)));

				// �x������
				if(getNumData(rec) < getDataNum + DATANUM) {	// ���������f�[�^���Ȃ��Ȃ�̂�h�����߂�1�b�͕K���x���
					Thread.sleep(INTERVAL2);
					//System.out.println("sleeptime: " + INTERVAL2);
				}else {
					Thread.sleep(INTERVAL2 - (after - before));
					//System.out.println("sleeptime: " + (INTERVAL2 - (after - before)));
				}
			}catch(Exception err) {
				try {
					System.err.println(err.getMessage());
					stopConnection();
					Thread.sleep(10000);
				} catch(Exception e) {
					e.printStackTrace();
					break;
				}
			}
		}
	}
	
	
	private void addTuple(long executiontime, OnMemoryTupleSet ts) throws StreamSpinnerException, InterruptedException {
		// log
		System.out.print(executiontime);
		//OnMemoryTupleSet ts = new OnMemoryTupleSet(schemaList.get(0));
		
		Tuple t = new Tuple(schemaList.get(0).size());//TODO unit�̊O. schemaList�̃T�C�Y��傫��
		t.setTimestamp(table[0], executiontime);//TODO �O��table[0]
		t.setLong(0, executiontime);//TODO �O
		int rowIndex = 0;
		
		for(int unit = 0; unit < MAX_UNIT; unit++) {
			//System.out.println("---" + i);
			
//			Tuple t = new Tuple(schemaList.get(unit).size());//unit�̊O. schemaList�̃T�C�Y��傫��
//			t.setTimestamp(table[unit], executiontime);//�O��table[0]
//			t.setLong(0, executiontime);//�O
			
			for(int disk = 0; disk < MAX_CH / 2; disk++) {
//				t.setDouble(disk + 1, (Double) power.get(unit).get(0));//TODO row index
				t.setDouble(rowIndex + 1, (Double) power.get(unit).get(0));//TODO row index
				
				// log
				System.out.print("," + power.get(unit).get(0));
				
				power.get(unit).remove(0);
				rowIndex++;// TODO
			}
			
//			ts.appendTuple(t);//unit�̊O
			
			
		}
		ts.appendTuple(t);//TODO unit�̊O
		
		//ts.beforeFirst();
		//deliverTupleSet(executiontime, table[0], ts);
		// log
		System.out.println("");
		
		//Thread.sleep(10);	// ���Ԃɕ���<-����?
	}
	
	
	// �R�}���h�����s
	private byte[] command(byte[] msg) {		
		sendMsg(msg);
		return getMsg();
	}
	
	
	// MemoryHiLogger�ɃR�}���h�𑗐M
	private void sendMsg(byte[] msg) {
		try{
			os.write(msg);
			os.flush();
		}catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	// MemoryHiLogger����R�}���h����M
	private byte[] getMsg() {
		try{
			while(is.available() == 0);
			byte[] rec = new byte[is.available()];
			is.read(rec);
			return rec;
		}catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
			return null;
		}
	}
	
	
	// MemoryHiLogger����d���f�[�^���󂯎�����d�͂��v�Z
	private void getData() throws IOException {
		try {
			//command(req);
			bis.read(raw);
			getVolt(raw);
			getPower();
			incRequireCommand();
		}catch(Exception e) {
			e.printStackTrace();
			stopConnection();
		}
	}
	
	
	// ���f�[�^����d�����X�g���擾
	private void getVolt(byte[] rec) {
		String raw = "";
		int index = 21;

		if(rec[0] == 0x01 && rec[1] == 0x00 && rec[2] == 0x01) {	// �f�[�^�]���R�}���h
			for(int unit = 1; unit < 9; unit++) {
				for(int ch = 1; ch < 17; ch++) {
					for(int i = 0; i < 4; i++) {	// �X�̓d��
						if(ch <= MAX_CH && unit <= MAX_UNIT) {
							raw += String.format("%02x", rec[index]);
						}
						index++;
					}
					if(ch <= MAX_CH && unit <= MAX_UNIT) {
						// �d���l�ɕϊ�(�X���C�hp47)
						// �d���������W
						// �����F 1(V/10DIV)
						// ���K�[���[�e�B���e�B�F 100 mv f.s. -> 0.1(V/10DIV)???
						volt.get(unit - 1).add(((double) Integer.parseInt(raw, 16) - 32768.0) * 0.1 / 20000.0);
					}
					raw = "";
				}
			}
		}else {	// �f�[�^�]���R�}���h�łȂ��ꍇ
			System.out.println("NULL");
			volt = null;
		}
	}
	
	
	// �d�����X�g�������d�̓��X�g���擾
	private void getPower() {
		for(int unit = 0; unit < MAX_UNIT; unit++) {
			int voltListSize = volt.get(unit).size();
			
			if(voltListSize % 2 != 0) {
				voltListSize--;
			}
			for(int i = 0; i < voltListSize; i += 2) {
				// (TODO) �ǂ����̃`�����l����12V��5V���𔻕ʂł���悤�ɂ���K�v������
				// ch1����5V���Ach2����12V��
				power.get(unit).add(Math.abs((Double) volt.get(unit).get(i)) * 50.0 + Math.abs((Double) volt.get(unit).get(i + 1)) * 120.0);
			}
			volt.get(unit).clear();
		}
	}
	
	// ���������f�[�^�����擾
	private long getNumData(byte[] rec) {
		String raw = "";
		if(rec[0] == 0x02 && rec[1] == 0x01) {
			switch(rec[2]) {
			case 0x55:
				for(int i = 13; i < 21; i++) {
					raw += String.format("%02x", rec[i]);
				}
				return Long.parseLong(raw, 16);
			}
		}
		return -1;
	}
		
	// MemoryHiLogger�̏�Ԃ��擾
	private byte getState(byte[] rec) {
		if(rec[0] == 0x02 && rec[1] == 0x01) {
			switch(rec[2]) {
			case 0x50:	// �X�^�[�g�R�}���h
			case 0x51:	// �X�g�b�v�R�}���h
			case 0x57:	// �����ԗv���R�}���h
			case 0x58:	// �A�v���V�X�e���g���K�R�}���h
				return rec[5];
			}
		}
		return (byte) 0xff;	// �s���ȃR�}���h
	}

	
	// �f�[�^�v���R�}���h�̃T���v�����O�ԍ��̃C���N�������g
	private void incRequireCommand() {
		for(int i = 12; i > 5; i--) {
			if(req[i] == 0xffffffff) {
				req[i] = 0x00000000;
				req[i - 1]++;
				if(req[i - 1] != 0xffffffff) {
					break;
				}
			}else if(req[5] == 0xffffffff) {
				return;
			}else {
				req[12]++;
				break;
			}
		}
	}
	
	
	// ���b�p�[������
	// �p�����[�^���󂯎��
	public void init() throws StreamSpinnerException {
		hostname = getParameter(PARAMETER_HOSTNAME);
		port = Integer.parseInt(getParameter(PARAMETER_PORT));
		interval = Long.parseLong(getParameter(PARAMETER_INTERVAL));
		datanum = INTERVAL2 / (int) interval;
	}

	// �e�[�u�����擾
	public String[] getAllTableNames() {
		return table;
	}

	// �X�L�[�}�̎擾
	public Schema getSchema(String tablename) {
		if(tablename.equals(table[0])) {
			return schemaList.get(0);
		}else if(tablename.equals(table[1])) {
			return schemaList.get(1);
		}else if(tablename.equals(table[2])) {
			return schemaList.get(2);
		}else if(tablename.equals(table[3])) {
			return schemaList.get(3);
		}else if(tablename.equals(table[4])) {
			return schemaList.get(4);
		}else if(tablename.equals(table[5])) {
			return schemaList.get(5);
		}else {
			return null;
		}
	}

	// �^�v���̎擾
	public TupleSet getTupleSet(ORNode node) throws StreamSpinnerException {
		return null;
	}

	// StreamSpinner�̋N���A�܂��̓��b�p�[�ǉ���
	public void start() throws StreamSpinnerException {
		if(interval <= 0) {
			throw new StreamSpinnerException("interval is not set");
		}
		monitor = new Thread(this);
		monitor.start();
	}

	// StreamSpinner�̃V���b�g�_�E���A�܂��̓��b�p�[�폜��
	public void stop() throws StreamSpinnerException {
		try {
			stopConnection();
		} catch(Exception e) {
			throw new StreamSpinnerException(e);
		}
		monitor = null;
	}
	
	// �ڑ��J�n
	private void startConnection() throws UnknownHostException, IOException, InterruptedException {
		if(connecting == false && theSocket == null) {
			theSocket = new Socket(hostname, port);
			theSocket.setSoTimeout((int) interval + 1000);
			
			is = theSocket.getInputStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			bis = new BufferedInputStream(is);
			
			// �\�P�b�g�I�[�v�����̉�������
			is = theSocket.getInputStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			while(is.available() == 0);
			char[] line = new char[is.available()];
			br.read(line);
			
			os = theSocket.getOutputStream();
			
			// �f�[�^�̎擾�Ԋu��ݒ�
			if(interval == 10) {
				command(Command.SAMP_10ms);
			}else if(interval == 50) {
				command(Command.SAMP_50ms);
			}else {
				command(Command.SAMP_100ms);
			}
			
			// �X�^�[�g
			command(Command.START);
			
			// log
			//System.out.println("[start]");
			System.out.print("time");
			for(int unit = 0; unit < MAX_UNIT; unit++) {
				for(int disk = 0; disk < MAX_CH / 2; disk++) {
					System.out.print(",power" + unit + "-" + disk);
				}
			}
			System.out.println("");
			
			Thread.sleep(1000);	// ��Ԃ��ω�����̂�҂�
			byte rec = (byte) 0xff;
			while(rec != 65){
				rec = getState(command(Command.REQUIRE_STATE));
			}
			rec = (byte) 0xff;
			
			// �V�X�e���g���K�[
			command(Command.SYSTRIGGER);
			Thread.sleep(1000);
			while(rec != 35){
				rec = getState(command(Command.REQUIRE_STATE));
			}
			rec = (byte) 0xff;
			
			// �f�[�^�v��
			req = Command.REQUIRE_DATA;
			command(req);
			while(is.available() == 0);
			raw = new byte[is.available()];
			bis.read(raw);

			Thread.sleep(INTERVAL2);
			
			// 1��̃f�[�^�擾����ݒ�
			req[20] = (byte) datanum;
		}
		connecting = true;
	}

	// �ڑ��I��
	private void stopConnection() {
		// �X�g�b�v
		command(Command.STOP);
		//System.out.println("[stop]");
		
		try {
			if(connecting == true && theSocket != null) {
				theSocket.close();
			}
		}catch(IOException e) {
			e.printStackTrace();
		}
		theSocket = null;
		is = null;
		isr = null;
		br = null;
		os = null;
		connecting = false;
	}

}
