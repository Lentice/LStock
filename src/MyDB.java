import java.sql.*;
import java.util.Calendar;
import java.util.Properties;

import org.jsoup.nodes.Element;

class HtmlParser {
	public static String getPercent(Element el) {
		String text = getText(el);
		if (text.isEmpty())
			return null;
		
		text = text.replace("%", "");

		float value = Float.parseFloat(text) / 100;
		return String.valueOf(value);
	}

	public static String getText(Element el) {
		String text = trim(el.text().trim().replaceAll(",", ""));
		if (text.isEmpty())
			return null;
		
		return text;
	}
	
	public static String trim(String s) {
		if (s.isEmpty())
			return s;

		int begin = s.length();
		int end = -1;

		for (int i = 0; i < s.length(); i++) {
			if (!Character.isSpaceChar(s.charAt(i))) {
				begin = i;
				break;
			}
		}
		s = s.substring(begin, s.length());
		if (s.isEmpty())
			return s;

		for (int i = s.length() - 1; i >= 0; i--) {
			if (!Character.isSpaceChar(s.charAt(i))) {
				end = i;
				break;
			}
		}

		return s.substring(0, end + 1);
	}
}


public class MyDB {
	public static boolean isUseIFRSs( int year) {
		if (year < 2013)
			return false;
		else
			return true;
	}

	public static boolean isUseIFRSs(int StockNum, int year) {
		if (year < 2013)
			return false;

		if (year == 2013 || year == 2014) {
			if (StockNum == 1107 || StockNum == 1408 || StockNum == 1606 || StockNum == 2381 || StockNum == 2396
					|| StockNum == 2523)
				return false;
			else
				return true;
		}

		return true;
	}

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
	
	public int getLastDailyTaiExDate() throws Exception {
		Calendar cal;
		ResultSet rs;
		Statement stmt = conn.createStatement();

		rs = stmt.executeQuery("SELECT MIN(Date) AS Date FROM daily_summary WHERE 開盤指數 is NULL");
		if (!rs.first() || rs.getDate("Date") == null) {
			rs = stmt.executeQuery("SELECT MAX(Date) AS Date FROM daily_summary");
			if (!rs.first() || rs.getDate("Date") == null) {
				Log.trace("Last TaiEx Date: 2004_02");
				return 200402;
			}
		}

		cal = Calendar.getInstance();
		cal.setTime(rs.getDate("Date"));
		
		int year =  cal.get(Calendar.YEAR);
		int month = cal.get(Calendar.MONTH) + 1;
		Log.trace("Last TaiEx Date: " + year + "_" + month);
		return year * 100 + month;
	}

	public Calendar getLastDailyTradeDate() throws Exception {
		Calendar cal = Calendar.getInstance();
		ResultSet rs;
		Statement stmt = conn.createStatement();

		rs = stmt.executeQuery("SELECT MAX(Date) FROM daily");
		if (!rs.first() || rs.getDate("MAX(Date)") == null) {
			cal.set(2004, 1, 11); // 最早可以取得的資料日期
			Log.trace(cal.getTime().toString());
			return cal;
		}
		cal.setTime(rs.getDate("MAX(Date)"));
		Log.trace("Last Trade Date: " + cal.getTime().toString());
		return cal;
	}

	public int getLastMonthlyRevenue() throws Exception {
		ResultSet rs;
		Statement stmt = conn.createStatement();

		rs = stmt.executeQuery("SELECT MAX(YearMonth) FROM monthly");
		if (!rs.first() || rs.getInt("MAX(YearMonth)") == 0)
			return 200401;
		
		int value = rs.getInt("MAX(YearMonth)");
		Log.trace("Last Month: " + value);

		return value;
	}
	
	public int getLastQuarterlyRevenue() throws Exception {
		ResultSet rs;
		Statement stmt = conn.createStatement();

		rs = stmt.executeQuery("SELECT MAX(YearQuarter) FROM quarterly");
		if (!rs.first() || rs.getInt("MAX(YearQuarter)") == 0)
			return 200401;
		int value = rs.getInt("MAX(YearQuarter)");
		Log.trace("Last Quarter: " + value);

		return value;
	}
	
	public int getLastAnnualRevenue() throws Exception {
		ResultSet rs;
		Statement stmt = conn.createStatement();

		rs = stmt.executeQuery("SELECT MAX(Year) FROM annual");
		if (!rs.first() || rs.getInt("MAX(Year)") == 0)
			return 2004;
		int value = rs.getInt("MAX(Year)");
		Log.trace("Last Year: " + value);

		return value;
	}
}

class MyStatement {
	static final int BATCH_SIZE = 1;
	
	PreparedStatement stm;
	int batchCount;
	Connection conn;
	int index = 1;
	
	MyStatement(Connection conn) throws SQLException {
		batchCount = 0;
		this.conn = conn;
	}
	
	MyStatement(Connection conn, String stmString) throws SQLException {
		stm = conn.prepareStatement(stmString);
		batchCount = 0;
	}
	
	public void setInsertIgnoreStatement(String table, String... columeNames) throws SQLException {
		String insert = "INSERT IGNORE INTO " + table + " ";
		String columes = "(";
		String values = "VALUES (";
		for (String name : columeNames) {
			columes = columes + name + ", ";
			values = values + "?, ";
		}
		
		// remove last ", "
		columes = columes.substring(0, columes.length() - 2) + ") ";
		values = values.substring(0, values.length() - 2) + ") ";
		
		String stmString = insert + columes + values;
		stm = conn.prepareStatement(stmString);
	}
	
	public void setInsertOnDuplicateStatement(String table, String... columeNames) throws SQLException {
		String insert = "INSERT INTO " + table + " ";
		String columes = "(";
		String values = "VALUES (";
		String onDuplicate = "ON DUPLICATE KEY UPDATE ";
		for (String name : columeNames) {
			columes = columes + name + ", ";
			values = values + "?, ";
			onDuplicate = onDuplicate + name + " = VALUES(" + name + "), ";
		}
		
		// remove last ", "
		columes = columes.substring(0, columes.length() - 2) + ") ";
		values = values.substring(0, values.length() - 2) + ") ";
		onDuplicate = onDuplicate.substring(0, onDuplicate.length() - 2);
		
		String stmString = insert + columes + values + onDuplicate;
		stm = conn.prepareStatement(stmString);
	}
	
	public void setUpdateStatement(String table, String where, String... columeNames) throws SQLException {
		String update = "UPDATE " + table + " ";
		String columes = "SET ";
		for (String name : columeNames) {
			columes = columes + name + "=?, ";
		}
		
		// remove last ", "
		columes = columes.substring(0, columes.length() - 2) + " ";
		String stmString = update + columes + "WHERE " + where;
		stm = conn.prepareStatement(stmString);
	}
	
	public void addBatch() throws SQLException {
		stm.addBatch();
		index = 1;
		
		if (++batchCount % BATCH_SIZE == 0) {
			stm.executeBatch();
		}
	}
	
	public void close() throws SQLException {
		stm.executeBatch();
		stm.close();
	}
	
	public int[] executeBatch() throws SQLException {
		return stm.executeBatch();
	}
	
	public void setBigInt(int index, long data) throws SQLException {
		stm.setLong(index, data);
	}
	
	public void setBigInt(int index, String data) throws SQLException {
		if (data == null || data.length() == 0)
			stm.setNull(index, java.sql.Types.BIGINT);
		else
			stm.setLong(index, Long.parseLong(data));
	}
	
	public void setBlob(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.BLOB);
		else
			stm.setString(index, data);
	}
	
	public void setChar(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.CHAR);
		else
			stm.setString(index, data);
	}
	
	public void setDate(int index, java.sql.Date date) throws SQLException {
		if (date == null)
			stm.setNull(index, java.sql.Types.DATE);
		else
			stm.setDate(index, date);
	}
	
	public void setDecimal(int index, String data) throws SQLException {
		if (data == null || data.length() == 0)
			stm.setNull(index, java.sql.Types.DECIMAL);
		else
			stm.setFloat(index, Float.parseFloat(data));
	}
	
	public void setDouble(int index, String data) throws SQLException {
		if (data == null || data.length() == 0)
			stm.setNull(index, java.sql.Types.DOUBLE);
		else
			stm.setDouble(index, Double.parseDouble(data));
	}
	
	public void setFloat(int index, float data) throws SQLException {
		stm.setFloat(index, data);
	}
	
	public void setFloat(int index, String data) throws SQLException {
		if (data == null || data.length() == 0)
			stm.setNull(index, java.sql.Types.FLOAT);
		else
			stm.setFloat(index, Float.parseFloat(data));
	}
	
	public void setInt(int index, int data) throws SQLException {
		stm.setInt(index, data);
	}
	
	public void setInt(int index, String data) throws SQLException {
		if (data == null || data.length() == 0)
			stm.setNull(index, java.sql.Types.INTEGER);
		else
			stm.setInt(index, Integer.parseInt(data));
	}
	
	public void setTinyInt(int index, int data) throws SQLException {
		stm.setInt(index, data);
	}

	public void setTinyInt(int index, String data) throws SQLException {
		if (data == null || data.length() == 0)
			stm.setNull(index, java.sql.Types.TINYINT);
		else
			stm.setInt(index, Integer.parseInt(data));
	}
	
	public void setObject(int index, Object data) throws SQLException {
			stm.setObject(index, data);
	}
	
	public void setObject(Object data) throws SQLException {
		stm.setObject(index++, data);
}
}
