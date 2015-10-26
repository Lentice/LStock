import java.sql.*;
import java.util.Calendar;
import java.util.Properties;

public class MyDB {
	public Connection conn;
	public Statement stmt;

	MyDB()  throws SQLException, ClassNotFoundException {
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

	public ResultSet executeQuery(String sql) throws SQLException {
		stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}
	
	static public Calendar getLastTradeDate() throws Exception {
		Calendar cal;
		ResultSet result;
		MyDB db = new MyDB();
		Statement stmt = db.conn.createStatement();
		
		result = stmt.executeQuery("SELECT COUNT(Date) FROM daily");
		result.next();
		if (result.getInt("COUNT(Date)") == 0) {
			cal = Calendar.getInstance();
			cal.set(2004, 1, 11); //最早可以取得的資料日期
			Log.trace(cal.getTime().toString());
			return cal;
		}
		result = stmt.executeQuery("SELECT MAX(Date) FROM daily");
		result.next();
		cal = Calendar.getInstance();
		cal.setTime(result.getDate("MAX(Date)"));
		Log.trace(cal.getTime().toString());
		return cal;
	}

	public static void main(String[] args) {

		
		try {
			Calendar cal = getLastTradeDate();
			Log.info(cal.getTime().toString());
//			MyDB db = new MyDB();
//			
//			ResultSet rs;
//			rs = db.executeQuery("SELECT * FROM cds");
//			while (rs.next()) {
//				String lastName = rs.getString("titel");
//				System.out.println(lastName);
//			}
//			db.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
