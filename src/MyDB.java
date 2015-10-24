import java.sql.*;
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

	public void disconnect() throws SQLException {
		conn.close();
	}

	public ResultSet executeQuery(String sql) throws SQLException {
		stmt = conn.createStatement();
		return stmt.executeQuery(sql);
	}

	public static void main(String[] args) {

		
		try {
			MyDB db = new MyDB();
			
			ResultSet rs;
			rs = db.executeQuery("SELECT * FROM cds");
			while (rs.next()) {
				String lastName = rs.getString("titel");
				System.out.println(lastName);
			}
			db.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
