import java.sql.*;
import java.util.Calendar;
import java.util.Properties;

public class MyDB {
	public Connection conn;

	MyDB() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");

		Properties p = new Properties();
		p.put("characterEncoding", "utf8"); // UTF8
		p.put("useUnicode", "TRUE");
		p.put("user", PrivateInfo.USERNAME);
		p.put("password", PrivateInfo.PASSWORD);

		Log.trace("Connect to SQL...");
		conn = DriverManager.getConnection(PrivateInfo.SQL_LINK, p);
		Log.trace("SQL Connected.");
	}

	public void close() throws SQLException {
		conn.close();
	}

	public Calendar getLastTradeDate() throws Exception {
		Calendar cal;
		ResultSet result;
		Statement stmt = conn.createStatement();

		result = stmt.executeQuery("SELECT COUNT(Date) FROM daily");
		result.next();
		if (result.getInt("COUNT(Date)") == 0) {
			cal = Calendar.getInstance();
			cal.set(2004, 1, 11); // 最早可以取得的資料日期
			Log.trace(cal.getTime().toString());
			return cal;
		}
		result = stmt.executeQuery("SELECT MAX(Date) FROM daily");
		result.next();
		cal = Calendar.getInstance();
		cal.setTime(result.getDate("MAX(Date)"));
		Log.trace("Last Trade Date: " + cal.getTime().toString());
		return cal;
	}

	public Calendar getLastRevenue() throws Exception {
		Calendar cal;
		ResultSet result;
		Statement stmt = conn.createStatement();

		result = stmt.executeQuery("SELECT COUNT(Date) FROM monthly");
		result.next();
		if (result.getInt("COUNT(Date)") == 0) {
			cal = Calendar.getInstance();
			cal.set(2013, 0, 1); // 最早可以取得的資料日期
			Log.trace(cal.getTime().toString());
			return cal;
		}
		result = stmt.executeQuery("SELECT MAX(Date) FROM monthly");
		result.next();
		cal = Calendar.getInstance();
		cal.setTime(result.getDate("MAX(Date)"));
		Log.trace("Last Trade Date: " + cal.getTime().toString());
		return cal;
	}

	public static void main(String[] args) {
		try {
			// MyDB db = new MyDB();
			// Calendar cal = db.getLastRevenue();
			// db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
