package jp.ac.titech.cs.de.ykstorage.util;

import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import jp.ac.titech.cs.de.ykstorage.service.Parameter;

public class StorageLogger {

	private static final Logger logger = Logger.getLogger(Parameter.LOGGER_NAME);
	private static final Level level = Level.ALL;

	static {
		try {
			FileHandler handler = new FileHandler(Parameter.LOG_DIR + "/" + Parameter.LOG_FILE_NAME);
			handler.setLevel(level);
			handler.setFormatter(new SimpleFormatter());
			logger.addHandler(handler);
			logger.setLevel(level);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}

	public static Logger getLogger() {
		return logger;
	}

}
