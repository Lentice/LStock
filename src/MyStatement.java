import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;

public class MyStatement {
	static final int BATCH_SIZE = 1000;
	
	PreparedStatement stm;
	int batchCount;
	
	MyStatement(Connection conn, String stmString) throws SQLException {
		stm = conn.prepareStatement(stmString);
		batchCount = 0;
	}
	
	public void setInt(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.INTEGER);
		else
			stm.setInt(index, Integer.parseInt(data));
	}
	
	public void setInt(int index, int data) throws SQLException {
		stm.setInt(index, data);
	}
	
	public void setBigInt(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.BIGINT);
		else
			stm.setLong(index, Long.parseLong(data));
	}
	
	public void setFloat(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.FLOAT);
		else
			stm.setFloat(index, Float.parseFloat(data));
	}
	
	public void setDouble(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.DOUBLE);
		else
			stm.setDouble(index, Double.parseDouble(data));
	}
	
	public void setChar(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.CHAR);
		else
			stm.setString(index, data);
	}
	
	public void setBlob(int index, String data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.BLOB);
		else
			stm.setString(index, data);
	}
	
	public void setDate(int index, java.sql.Date data) throws SQLException {
		if (data == null)
			stm.setNull(index, java.sql.Types.DATE);
		else
			stm.setDate(index, data);
	}
	
	public void addBatch() throws SQLException {
		stm.addBatch();
		
		if (++batchCount % BATCH_SIZE == 0) {
			stm.executeBatch();
		}
	}
	
	public int[] executeBatch() throws SQLException {
		return stm.executeBatch();
	}

	public void close() throws SQLException {
		stm.executeBatch();
		stm.close();
	}
	
}
