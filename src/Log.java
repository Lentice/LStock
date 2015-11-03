import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Log {
	private static final byte ERROR = 0;
	private static final byte WARN = 1;
	private static final byte INFO = 2;
	private static final byte DEBUG = 3;
	private static final byte TRACE = 4;
	private static final byte VERBOSE = 5;

	private static final byte level = TRACE;
	protected static SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS: ");
	
	protected static String getTimeStamp() {
		return sdf.format(Calendar.getInstance().getTime());
	}

	public static void err(String msg) {
		if (level >= ERROR)
			System.out.println(getTimeStamp() + msg);
	}

	public static void warn(String msg) {
		if (level >= WARN)
			System.out.println(getTimeStamp() + msg);
	}

	public static void info(String msg) {
		if (level >= INFO)
			System.out.println(getTimeStamp() + msg);
	}

	public static void dbg(String msg) {
		if (level >= DEBUG)
			System.out.println(getTimeStamp() + msg);
	}

	public static void trace(String msg) {
		if (level >= TRACE)
			System.out.println(getTimeStamp() + msg);
	}

	public static void verbose(String msg) {
		if (level >= VERBOSE)
			System.out.println(getTimeStamp() + msg);
	}
	
	public static void err_(String msg) {
		if (level >= ERROR)
			System.out.print(msg);
	}

	public static void warn_(String msg) {
		if (level >= WARN)
			System.out.print(msg);
	}

	public static void info_(String msg) {
		if (level >= INFO)
			System.out.print(msg);
	}

	public static void dbg_(String msg) {
		if (level >= DEBUG)
			System.out.print(msg);
	}

	public static void trace_(String msg) {
		if (level >= TRACE)
			System.out.print(msg);
	}

	public static void verbose_(String msg) {
		if (level >= VERBOSE)
			System.out.print(msg);
	}
}