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

	public static String PARAMETER_HOSTNAME = "hostname";	// MemoryHiLoggerのIPアドレス
	public static String PARAMETER_PORT = "port";	// MemoryHiLoggerのポート番号
	public static String PARAMETER_INTERVAL = "interval";	// メモリハイロガーの測定間隔(10, 50, 100ms)
	
	public static final int INTERVAL2 = 1000;	// データの取得間隔(TODO) 遅延を少なくするように動的に設定
	public static final int DATANUM = 100;	// (FIXME) xmlのパラメータから動的に。50の時20。10の時100
	
	// 計測するチャンネル数、ユニット数
	// 変更する場合はロガーユーティリティでメモリハイロガーを再設定する必要がある
	// ユニット毎に別々のチャンネル数を設定できない
	static final int MAX_CH = 14;	// 最大14ch
	static final int MAX_UNIT = 6;	// 最大6unit
	
	private String[] table = null;
	private String hostname = null;
	private int port = 0;
	private long interval = 0;
	private int datanum = 1;	// 1回の取得データ数。最大255(0xff)
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
	
	private ArrayList<ArrayList> volt = new ArrayList<ArrayList>();	// 取得した電圧
	private ArrayList<ArrayList> power = new ArrayList<ArrayList>();	// 電圧から計算した消費電力
	
	private byte[] req;	// データ要求コマンド
	private byte[] raw;	// 電圧の生データ

	
	public MemoryHiLoggerWrapper(String name) throws StreamSpinnerException {
		super(name);
		
		// スキーマの作成
		table = new String[MAX_UNIT];
		for(int i = 0; i < MAX_UNIT; i++) {
			table[i] = "Unit" + (i + 1);
			volt.add(new ArrayList<Double>());
			power.add(new ArrayList<Double>());
		}
		
		for(int unit = 0; unit < MAX_UNIT; unit++) {
//			String[] attrs = new String[MAX_CH / 2 + 1];//TODO attrsのサイズをunit数*7（ディスク数）大きく
//			String[] types = new String[MAX_CH / 2 + 1];//上と同様
			String[] attrs = new String[(MAX_CH / 2) * MAX_UNIT + 1];//attrsのサイズをunit数*7（ディスク数）大きく
			String[] types = new String[(MAX_CH / 2) * MAX_UNIT + 1];//上と同様
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

	// スレッドスタート時
	public void run() {
		Thread current = Thread.currentThread();
		
		// 接続開始
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
		
		// タプル追加
		while(monitor != null && monitor.equals(current) && connecting == true) {
			try {
				long executiontime = System.currentTimeMillis();
				long before = executiontime;
				byte[] rec = command(req);	// データ要求コマンド
				
				for(int i = 0; i < datanum; i++) {
					getData();
				}
				
				//System.out.println("---------------");
				// ディスク台数分の消費電力をタプルに追加する
				// 外側でタプルを作る
				// ユニットごとのタイムスタンプをそろえる必要があるかもしれない
				OnMemoryTupleSet ts = new OnMemoryTupleSet(schemaList.get(0));//TODO get(unit)->unit=0でやることにする
				for(int i = 0; i < DATANUM; i++) {
					addTuple(executiontime, ts);
					executiontime += interval;
				}
				ts.beforeFirst();
				deliverTupleSet(executiontime - interval, table[0], ts);//TODO table[unit]->unit=0でやることにする
				
				getDataNum += DATANUM;
				long after = System.currentTimeMillis();
				
				//System.out.println("memory: " + getNumData(rec));
				//System.out.println("get: " + getDataNum);
				//System.out.println("INTERVAL2: " + INTERVAL2);
				//System.out.println("after: " + after);
				//System.out.println("before: " + executiontime);
				//System.out.println("sleeptime: " + (INTERVAL2 - (after - before)));

				// 遅延解消
				if(getNumData(rec) < getDataNum + DATANUM) {	// メモリ内データがなくなるのを防ぐために1秒は必ず遅れる
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
		
		Tuple t = new Tuple(schemaList.get(0).size());//TODO unitの外. schemaListのサイズを大きく
		t.setTimestamp(table[0], executiontime);//TODO 外でtable[0]
		t.setLong(0, executiontime);//TODO 外
		int rowIndex = 0;
		
		for(int unit = 0; unit < MAX_UNIT; unit++) {
			//System.out.println("---" + i);
			
//			Tuple t = new Tuple(schemaList.get(unit).size());//unitの外. schemaListのサイズを大きく
//			t.setTimestamp(table[unit], executiontime);//外でtable[0]
//			t.setLong(0, executiontime);//外
			
			for(int disk = 0; disk < MAX_CH / 2; disk++) {
//				t.setDouble(disk + 1, (Double) power.get(unit).get(0));//TODO row index
				t.setDouble(rowIndex + 1, (Double) power.get(unit).get(0));//TODO row index
				
				// log
				System.out.print("," + power.get(unit).get(0));
				
				power.get(unit).remove(0);
				rowIndex++;// TODO
			}
			
//			ts.appendTuple(t);//unitの外
			
			
		}
		ts.appendTuple(t);//TODO unitの外
		
		//ts.beforeFirst();
		//deliverTupleSet(executiontime, table[0], ts);
		// log
		System.out.println("");
		
		//Thread.sleep(10);	// 順番に並ぶ<-何が?
	}
	
	
	// コマンドを実行
	private byte[] command(byte[] msg) {		
		sendMsg(msg);
		return getMsg();
	}
	
	
	// MemoryHiLoggerにコマンドを送信
	private void sendMsg(byte[] msg) {
		try{
			os.write(msg);
			os.flush();
		}catch(Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	
	// MemoryHiLoggerからコマンドを受信
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
	
	
	// MemoryHiLoggerから電圧データを受け取り消費電力を計算
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
	
	
	// 生データから電圧リストを取得
	private void getVolt(byte[] rec) {
		String raw = "";
		int index = 21;

		if(rec[0] == 0x01 && rec[1] == 0x00 && rec[2] == 0x01) {	// データ転送コマンド
			for(int unit = 1; unit < 9; unit++) {
				for(int ch = 1; ch < 17; ch++) {
					for(int i = 0; i < 4; i++) {	// 個々の電圧
						if(ch <= MAX_CH && unit <= MAX_UNIT) {
							raw += String.format("%02x", rec[index]);
						}
						index++;
					}
					if(ch <= MAX_CH && unit <= MAX_UNIT) {
						// 電圧値に変換(スライドp47)
						// 電圧軸レンジ
						// 資料： 1(V/10DIV)
						// ロガーユーティリティ： 100 mv f.s. -> 0.1(V/10DIV)???
						volt.get(unit - 1).add(((double) Integer.parseInt(raw, 16) - 32768.0) * 0.1 / 20000.0);
					}
					raw = "";
				}
			}
		}else {	// データ転送コマンドでない場合
			System.out.println("NULL");
			volt = null;
		}
	}
	
	
	// 電圧リストから消費電力リストを取得
	private void getPower() {
		for(int unit = 0; unit < MAX_UNIT; unit++) {
			int voltListSize = volt.get(unit).size();
			
			if(voltListSize % 2 != 0) {
				voltListSize--;
			}
			for(int i = 0; i < voltListSize; i += 2) {
				// (TODO) どっちのチャンネルが12Vか5Vかを判別できるようにする必要がある
				// ch1が赤5V線、ch2が黄12V線
				power.get(unit).add(Math.abs((Double) volt.get(unit).get(i)) * 50.0 + Math.abs((Double) volt.get(unit).get(i + 1)) * 120.0);
			}
			volt.get(unit).clear();
		}
	}
	
	// メモリ内データ数を取得
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
		
	// MemoryHiLoggerの状態を取得
	private byte getState(byte[] rec) {
		if(rec[0] == 0x02 && rec[1] == 0x01) {
			switch(rec[2]) {
			case 0x50:	// スタートコマンド
			case 0x51:	// ストップコマンド
			case 0x57:	// 測定状態要求コマンド
			case 0x58:	// アプリシステムトリガコマンド
				return rec[5];
			}
		}
		return (byte) 0xff;	// 不明なコマンド
	}

	
	// データ要求コマンドのサンプリング番号のインクリメント
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
	
	
	// ラッパー生成時
	// パラメータを受け取る
	public void init() throws StreamSpinnerException {
		hostname = getParameter(PARAMETER_HOSTNAME);
		port = Integer.parseInt(getParameter(PARAMETER_PORT));
		interval = Long.parseLong(getParameter(PARAMETER_INTERVAL));
		datanum = INTERVAL2 / (int) interval;
	}

	// テーブル名取得
	public String[] getAllTableNames() {
		return table;
	}

	// スキーマの取得
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

	// タプルの取得
	public TupleSet getTupleSet(ORNode node) throws StreamSpinnerException {
		return null;
	}

	// StreamSpinnerの起動、またはラッパー追加時
	public void start() throws StreamSpinnerException {
		if(interval <= 0) {
			throw new StreamSpinnerException("interval is not set");
		}
		monitor = new Thread(this);
		monitor.start();
	}

	// StreamSpinnerのシャットダウン、またはラッパー削除時
	public void stop() throws StreamSpinnerException {
		try {
			stopConnection();
		} catch(Exception e) {
			throw new StreamSpinnerException(e);
		}
		monitor = null;
	}
	
	// 接続開始
	private void startConnection() throws UnknownHostException, IOException, InterruptedException {
		if(connecting == false && theSocket == null) {
			theSocket = new Socket(hostname, port);
			theSocket.setSoTimeout((int) interval + 1000);
			
			is = theSocket.getInputStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			bis = new BufferedInputStream(is);
			
			// ソケットオープン時の応答処理
			is = theSocket.getInputStream();
			isr = new InputStreamReader(is);
			br = new BufferedReader(isr);
			while(is.available() == 0);
			char[] line = new char[is.available()];
			br.read(line);
			
			os = theSocket.getOutputStream();
			
			// データの取得間隔を設定
			if(interval == 10) {
				command(Command.SAMP_10ms);
			}else if(interval == 50) {
				command(Command.SAMP_50ms);
			}else {
				command(Command.SAMP_100ms);
			}
			
			// スタート
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
			
			Thread.sleep(1000);	// 状態が変化するのを待つ
			byte rec = (byte) 0xff;
			while(rec != 65){
				rec = getState(command(Command.REQUIRE_STATE));
			}
			rec = (byte) 0xff;
			
			// システムトリガー
			command(Command.SYSTRIGGER);
			Thread.sleep(1000);
			while(rec != 35){
				rec = getState(command(Command.REQUIRE_STATE));
			}
			rec = (byte) 0xff;
			
			// データ要求
			req = Command.REQUIRE_DATA;
			command(req);
			while(is.available() == 0);
			raw = new byte[is.available()];
			bis.read(raw);

			Thread.sleep(INTERVAL2);
			
			// 1回のデータ取得数を設定
			req[20] = (byte) datanum;
		}
		connecting = true;
	}

	// 接続終了
	private void stopConnection() {
		// ストップ
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
