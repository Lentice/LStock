package lstockv2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyStatement {
	
	private int batchSize = 1000;

	private PreparedStatement stm;
	private int batchCount = 0;
	private Connection connection;
	private int index = 1;
	
	private final Object lock = new Object();

	MyStatement(Connection connection) {
		this.connection = connection;
	}

	MyStatement(Connection connection, String stmString) {
		this.connection = connection;
		
		try {
			stm = connection.prepareStatement(stmString);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	public Object AcquireLock() {
		return lock;
	}
	
	public void setBatchSize(int size) {
		batchSize = size;
	}

	public void setStatementInsertIgnore(String table, String... columnNames) {
		batchSize = 32768 / columnNames.length;
		String insert = "INSERT IGNORE INTO " + table + " ";
		String columns = "(";
		String values = "VALUES (";
		for (String name : columnNames) {
			columns = columns + name + ", ";
			values = values + "?, ";
		}

		// remove last ", "
		columns = columns.substring(0, columns.length() - 2) + ") ";
		values = values.substring(0, values.length() - 2) + ") ";

		String stmString = insert + columns + values;
		try {
			stm = connection.prepareStatement(stmString);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void setStatementInsertAndUpdate(String table, String... columnNames) {
		String insert = "INSERT INTO " + table + " ";
		String columns = "(";
		String values = "VALUES (";
		String onDuplicate = "ON DUPLICATE KEY UPDATE ";
		for (String name : columnNames) {
			columns = columns + name + ", ";
			values = values + "?, ";
			onDuplicate = onDuplicate + name + " = VALUES(" + name + "), ";
		}

		// remove last ", "
		columns = columns.substring(0, columns.length() - 2) + ") ";
		values = values.substring(0, values.length() - 2) + ") ";
		onDuplicate = onDuplicate.substring(0, onDuplicate.length() - 2);

		String stmString = insert + columns + values + onDuplicate;
		try {
			stm = connection.prepareStatement(stmString);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void setStatementUpdate(String table, String where, String... columnNames) {
		String update = "UPDATE " + table + " ";
		String columns = "SET ";
		for (String name : columnNames) {
			columns = columns + name + "=?, ";
		}

		// remove last ", "
		columns = columns.substring(0, columns.length() - 2) + " ";
		String stmString = update + columns + "WHERE " + where;
		
		try {
			stm = connection.prepareStatement(stmString);
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void addBatch() throws SQLException {
		//log.trace(stm);
		stm.addBatch();
		if (++batchCount % batchSize == 0) {
			stm.executeBatch();
			connection.commit();
		}
		
		// reset index
		index = 1;
	}

	public void close(){
		try {
			stm.executeBatch();
			stm.close();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
			System.exit(-1);
		}
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
