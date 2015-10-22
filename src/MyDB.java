import java.sql.*;
import java.util.Properties;

public class MyDB {
	

	public Connection conn;
	public Statement stmt;

	MyDB() {
	}

	public void connect() throws SQLException {
		Properties p = new Properties();
		p.put("characterEncoding", "utf8"); // UTF8
		p.put("useUnicode", "TRUE");
		p.put("user", PrivateInfo.USERNAME);
		p.put("password", PrivateInfo.PASSWORD);

		Log.trace("Connect to SQL...");
		conn = DriverManager.getConnection(PrivateInfo.SQL_LINK, p);
	}

	public void disconnect() throws SQLException {
		conn.close();
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}

	public static void main(String[] args) {

		MyDB db = new MyDB();
		try {
			db.connect();
			ResultSet rs;
			rs = db.executeQuery("SELECT * FROM cds");
			while (rs.next()) {
				String lastName = rs.getString("titel");
				System.out.println(lastName);
			}
			db.disconnect();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
