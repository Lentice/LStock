package lstockv2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MyDB {
	private static final Logger log = LogManager.getLogger(Downloader.class.getName());

	public static final String USERNAME = "root";
	public static final String PASSWORD = "";
	public static final String URL = "jdbc:mysql://127.0.0.1:3306/stock_v2";
	private static final String MAX_POOL = "500";

	public Connection conn;

	MyDB() throws SQLException, ClassNotFoundException {
		Class.forName("com.mysql.jdbc.Driver");

		Properties p = new Properties();
		p.put("characterEncoding", "utf8"); // UTF8
		p.put("useUnicode", "TRUE");
		p.put("user", USERNAME);
		p.put("password", PASSWORD);
		p.put("rewriteBatchedStatements", "TRUE");
		p.put("MaxPooledStatements", MAX_POOL);

		log.trace("Connect to " + URL);
		conn = DriverManager.getConnection(URL, p);
		log.trace("SQL Connected.");

		conn.setAutoCommit(false);
	}

	public void close() throws SQLException {
		conn.commit();
		conn.close();
	}

	public int getLastDailyTaiExDate() {
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT MAX(Date) AS Date FROM daily_summary WHERE 開盤指數 is NOT NULL");
			if (!rs.first() || rs.getDate("Date") == null) {
				log.trace("Last TaiEx Date: 2004_02");
				return 200402;
			}

			Calendar cal = Calendar.getInstance();
			cal.setTime(rs.getDate("Date"));

			int year = cal.get(Calendar.YEAR);
			int month = cal.get(Calendar.MONTH) + 1;
			log.trace("Last TaiEx Date: " + year + "_" + month);

			return year * 100 + month; // return formated data such as 201602

		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return 0;
	}

	public Calendar getLastDailyTradeDate() {
		Calendar cal = Calendar.getInstance();
		
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT MAX(Date) FROM daily");
			if (!rs.first() || rs.getDate("MAX(Date)") == null) {
				cal.set(2004, 1, 11); // 最早可以取得的資料日期
				log.trace(cal.getTime().toString());
				return cal;
			}
			cal.setTime(rs.getDate("MAX(Date)"));
			log.trace("Last Trade Date: " + cal.getTime().toString());

		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		return cal;
	}

	public int getLastMonthlyRevenue() {
		int value = 0; 
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT MAX(YearMonth) FROM monthly");
			if (!rs.first() || rs.getInt("MAX(YearMonth)") == 0)
				return 200401;

			value = rs.getInt("MAX(YearMonth)");
			log.trace("Last Month: " + value);

		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return value;
	}

	public int getLastQuarterlyRevenue() {
		int value = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT MAX(YearQuarter) FROM quarterly");
			if (!rs.first() || rs.getInt("MAX(YearQuarter)") == 0)
				return 200401;
			value = rs.getInt("MAX(YearQuarter)");
			log.trace("Last Quarter: " + value);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return value;
	}

	public int getLastAnnualRevenue() {
		int value = 0;
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT MAX(Year) FROM annual");
			if (!rs.first() || rs.getInt("MAX(Year)") == 0)
				return 2004;
			value = rs.getInt("MAX(Year)");
			log.trace("Last Year: " + value);

		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}

		return value;
	}
	
	
	public static ArrayList<HashMap<String, Object>> processResultSet(final ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		int columns = md.getColumnCount();
		ArrayList<HashMap<String, Object>> list = new ArrayList<HashMap<String, Object>>();
		HashMap<String, Object> row;
		while (rs.next()) {
			row = new HashMap<String, Object>(columns);
			for (int i = 1; i <= columns; ++i) {
				row.put(md.getColumnName(i), rs.getObject(i));
			}
			list.add(row);
		}
		
		return list;
	}
}
