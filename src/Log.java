import java.util.logging.Logger;
import java.io.IOException;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class Log {
	private static final boolean USE_FILE_LOG = false;
	private static final String LOG_NAME = "MyStockLog";

	private static final Logger log = Logger.getLogger(LOG_NAME);
	private static final Level level = Level.ALL;

	static {
		log.setUseParentHandlers(false);

		ConsoleHandler consoleHandler = new ConsoleHandler();
		consoleHandler.setLevel(level);
		consoleHandler.setFormatter(new MyLogHander());
		log.addHandler(consoleHandler);

		if (USE_FILE_LOG) {
			try {
				Handler fh = new FileHandler(LOG_NAME + ".log");
				fh.setFormatter(new MyLogHander());
				fh.setLevel(level);
				log.addHandler(fh);
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	public static void err(String msg) {
		log.severe(msg);
	}

	public static void warn(String msg) {
		log.warning(msg);
	}

	public static void info(String msg) {
		log.info(msg);
	}

	public static void dbg(String msg) {
		log.config(msg);
	}

	public static void trace(String msg) {
		log.fine(msg);
	}

	public static void verbose(String msg) {
		log.finer(msg);
	}

	public static void finest(String msg) {
		log.finest(msg);
	}
}

class MyLogHander extends Formatter {
	@Override
	public String format(LogRecord record) {
		//return new Date(record.getMillis()).toString() + " " + record.getLevel() + ":" + record.getMessage() + "\n";
		return record.getLevel() + ":" + record.getMessage() + "\n";
	}
}
