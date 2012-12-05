package jp.ac.titech.cs.de.ykstorage.service;

/**
 * 起動パラメータ用の一時的なクラス．最終的にはXMLやプロパティファイル
 * から起動時にパラメータを読み込むようにさせる．
 * 出来ればコマンドライン引数でも指定出来るようにし，コマンドライン引数
 * で指定されたパラメータはその値が優先的に使われるようにしたい．
 * 
 * @author hikida
 *
 */
public class Parameter {
	
	/**
	 * A number of data disks.
	 */
	public static final int NUMBER_OF_DATADISK = 8;
	
	/**
	 * Capacity of cache memory. It's unit is byte.
	 */
	public static final int CAPACITY_OF_CACHEMEMORY = 10;
	
	public static final double MEMORY_THRESHOLD = 1.0;
	
	/**
	 * The disk spin down threshold time.
	 */
	public static final double SPIN_DOWN_THRESHOLD = 10.0;
	
	public static final String[] DATA_DISK_PATHS = {
		"./disk01/",
		"./disk02/"
	};
}
