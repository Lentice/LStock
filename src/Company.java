import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Company {
	public String code;
	public int stockNum;
	public String name;
	public String category;
	public Date lastUpdateDate;
	public int lastUpdateInt;

	float latestPrice;
	MonthlyData[] mData;
	QuarterlyData[] qData;
	AnnualData[] yData;
	
	MyDB db;

	public Company(MyDB db) {
		this.db = db;
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
	
	void fetchAllBasicData() throws Exception {
		latestPrice = getlatestPrice();
		fetchAllMonth();
		fetchAllQuarter();
		fetchAllYear();
	}

	void fetchAllMonth() throws SQLException {
		mData = MonthlyData.getAllData(db, stockNum);
	}

	void fetchAllQuarter() throws SQLException {
		qData = QuarterlyData.getAllData(db, stockNum);
	}

	void fetchAllYear() throws SQLException {
		yData = AnnualData.getAllData(db, stockNum);
	}

	float getlatestPrice() throws Exception {
		Statement stm = db.conn.createStatement();
		String lastValid = "(select Date from daily WHERE StockNum = " + stockNum + " AND 收盤價 IS NOT NULL ORDER BY Date DESC LIMIT 1)";
		String query = "SELECT 收盤價 FROM daily WHERE Date = " + lastValid +" AND StockNum = " + stockNum;
		ResultSet rs = stm.executeQuery(query);

		if (!rs.first()) {
			throw new Exception("no latest price");
		}

		float price = rs.getFloat("收盤價");
		stm.close();
	 
		return price;
	}
	
	DailyData getDData(Date date) throws Exception {
		return DailyData.queryData(db, date, stockNum);
	}
	
	MonthlyData getMData(int year, int month) throws SQLException {
		return MonthlyData.getData(mData, year, month);
	}
	
	QuarterlyData getQData(int year, int quarter) throws SQLException {
		return QuarterlyData.getData(qData, year, quarter);
	}
	
	AnnualData getYData(int year) throws SQLException {
		return AnnualData.getData(yData, year);
	}
	
	static public Company[] getAllCompanies(MyDB db) throws Exception {
		return getAllCompanies(db, false);
	}
	
	static public Company[] getAllValidCompanies(MyDB db) throws Exception {
		return getAllCompanies(db, true);
	}

	static private Company[] getAllCompanies(MyDB db, boolean valid) throws Exception {
		
		Statement compST = db.conn.createStatement();
		String query;
		if (valid)
			query = "SELECT * FROM company WHERE last_update = (SELECT MAX(Date) FROM daily)";
		else
			query = "SELECT * FROM company";
		ResultSet companySet = compST.executeQuery(query);
		companySet.last();
		int numRow = companySet.getRow();
		companySet.beforeFirst();

		if (numRow == 0)
			return null;

		List<Company> companyList = new ArrayList<>();
		while (companySet.next()) {
			Company info = new Company(db);
			info.code = companySet.getString("Code");
			info.stockNum = Integer.parseInt(info.code);
			info.name = companySet.getString("Name");
			info.category = companySet.getString("產業別");
			info.lastUpdateDate = companySet.getDate("last_update");
			info.lastUpdateInt = Integer.parseInt(info.lastUpdateDate.toString().replaceAll("-", ""));
			companyList.add(info);
		}

		compST.close();
		return companyList.toArray(new Company[0]);
	}
}
