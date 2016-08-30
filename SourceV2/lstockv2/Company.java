package lstockv2;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Company {
	public String code;
	public int stockNum;
	public String name;
	public String category;
	public Date lastUpdateDate;
	public int lastUpdateInt;
	public float 本益比;

	int lastYear;
	int lastYearQuarter;
	int lastYearMonth;

	MyDB db;

	public Company(MyDB db, String code, int stockNum, String name, String category, Date lastUpdateDate, float 本益比)
	        throws Exception {
		this.db = db;
		this.code = code;
		this.stockNum = stockNum;
		this.name = name;
		this.category = category;
		this.本益比 = 本益比;
		lastUpdateInt = Integer.parseInt(lastUpdateDate.toString().replaceAll("-", ""));
	}

	public boolean isValidQuarter(int year, int quarter) {
		int lastYear = lastUpdateInt / 10000;
		int lastMonth = lastUpdateInt / 100 % 100;
		int lastDay = lastUpdateInt % 100;

		int lastQuarter = 0;
		if (lastMonth > 11 || (lastMonth == 11 && lastDay > 15))
			lastQuarter = 3;
		else if (lastMonth > 8 || (lastMonth == 8 && lastDay > 15))
			lastQuarter = 2;
		else if (lastMonth > 5 || (lastMonth == 5 && lastDay > 15))
			lastQuarter = 1;

		int lastYearQuarter = lastYear * 100 + lastQuarter;
		int targetYearQuarter = year * 100 + quarter;

		if (lastYearQuarter >= targetYearQuarter) {
			return true;
		}

		return false;
	}

	public boolean isValidYear(int year) {
		int lastYear = lastUpdateInt / 10000;
		int lastMonth = (lastUpdateInt / 100) % 100;

		if (year < lastYear - 1)
			return true;
		else if (year == lastYear - 1 && (lastMonth > 3))
			return true;

		return false;
	}

	public boolean isFinancial() {
		if (category == null)
			return false;
		else if (category.equals("金融保險業") || category.equals("金融保險"))
			return true;
		else
			return false;
	}

	float getlatestPrice() throws Exception {
		Statement stm = db.conn.createStatement();
		String lastValid = "(select Date from daily WHERE StockNum = " + stockNum
		        + " AND 收盤價 IS NOT NULL ORDER BY Date DESC LIMIT 1)";
		String query = "SELECT 收盤價 FROM daily WHERE Date = " + lastValid + " AND StockNum = " + stockNum;
		ResultSet rs = stm.executeQuery(query);

		if (!rs.first()) {
			throw new Exception(stockNum + " " + name + " has no latest price");
		}

		float price = rs.getFloat("收盤價");
		stm.close();

		return price;
	}

	// 取得當前本益比
	float getLatestPER() throws SQLException {
		ResultSet rs;
		Statement stm = db.conn.createStatement();
		rs = stm.executeQuery(
		        "SELECT 本益比 AS val FROM daily WHERE Date = (SELECT MAX(Date) FROM daily) AND StockNum = " + stockNum);
		rs.first();
		float curRat = rs.getFloat("val");

		rs.close();
		return curRat;
	}

	static public Company[] getAllCompanies() throws Exception {
		return getAllCompanies(false);
	}

	static public Company[] getAllValidCompanies() throws Exception {
		return getAllCompanies(true); 
	}

	static private Company[] getAllCompanies(boolean valid) throws Exception {
		MyDB db = new MyDB();
		Statement compST = db.conn.createStatement();
		String query;
		if (valid)
			query = "SELECT StockNum,Code,Name,產業別,last_update,本益比 FROM company WHERE last_update = (SELECT MAX(Date) FROM daily) AND 本益比 <> 0";
		else
			query = "SELECT StockNum,Code,Name,產業別,last_update,本益比 FROM company WHERE 本益比 <> 0";
		ResultSet companySet = compST.executeQuery(query);
		
		// Get number of company
		companySet.last();
		int numCompany = companySet.getRow();
		companySet.beforeFirst();
		if (numCompany == 0) {
			db.close();
			return null;
		}

		Company[] companies = new Company[numCompany];
		int idx = 0;
		while (companySet.next()) {
			String code = companySet.getString("Code");
			companies[idx] = new Company(db, code, companySet.getInt("StockNum"), companySet.getString("Name"),
			        companySet.getString("產業別"), companySet.getDate("last_update"), companySet.getFloat("本益比"));
			idx++;
		}

		compST.close();
		db.close();
		return companies;
	}
}
