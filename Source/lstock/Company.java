package lstock;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Company {
	public String code;
	public int stockNum;
	public String name;
	public String category;
	public int lastUpdateInt;
	public int firstUpdateInt;
	public float 本益比;
	public boolean isOldCashflowUpdated;

	int lastYear;
	int lastYearQuarter;
	int lastYearMonth;


	public Company(String code, int stockNum, String name, String category, Date firstUpdateDate, 
			Date lastUpdateDate, float 本益比, boolean isOldCashflowUpdated)
	        throws Exception {
		this.code = code;
		this.stockNum = stockNum;
		this.name = name;
		this.category = category;
		this.本益比 = 本益比;
		this.isOldCashflowUpdated = isOldCashflowUpdated;
		lastUpdateInt = Integer.parseInt(lastUpdateDate.toString().replaceAll("-", ""));
		firstUpdateInt = Integer.parseInt(firstUpdateDate.toString().replaceAll("-", ""));
	}

	public boolean isValidQuarter(int year, int quarter) {
		
		// 2841 do not have valid quarterly data in this period
		if (this.stockNum == 2841 && year == 2004 || (year == 2005 && quarter < 3)) {
			return false;
		} else if (this.stockNum == 1227 && year == 2005 && (quarter == 3 || quarter == 4)) {
			return false;
		} else if (this.stockNum == 2363 && year == 2007 && quarter == 1) {
			return false;
		}
		
		int lastYear = lastUpdateInt / 10000;
		int lastMonth = lastUpdateInt / 100 % 100;
		int lastDay = lastUpdateInt % 100;
		
		if (quarter == 4) {
			if (year >= lastYear) {
				return false;
			} else if (year == lastYear - 1) {
				if (lastMonth <= 3) {
					return false;
				}
			}
		} else if (year > lastYear) {
			return false;
		} else if (year == lastYear) {
			if (quarter == 3 && (lastMonth < 11 || (lastMonth == 11 && lastDay <= 14))) {
				return false;
			} else if (quarter == 2 &&(lastMonth < 8 || (lastMonth == 8 && lastDay <= 14))) {
				return false;
			} else if (quarter == 1 && (lastMonth < 5 || (lastMonth == 5 && lastDay <= 15))) {
				return false;
			}
		}

		int firstYear = firstUpdateInt / 10000;
		int firstMonth = firstUpdateInt / 100 % 100;
		
		if (year < firstYear) {
			return false;
		} else if (year == firstYear) {
			if (quarter == 1 && firstMonth > 3)
				return false;
			else if (quarter == 2 && firstMonth > 6)
				return false;
			else if (quarter == 3 && firstMonth > 9)
				return false;
		}		

		return true;
	}

	public boolean isValidYear(int year) {
		int lastYear = lastUpdateInt / 10000;
		int lastMonth = (lastUpdateInt / 100) % 100;

		if (year >= lastYear)
			return false;
		else if (year == lastYear - 1 && (lastMonth <= 3))
			return false;
		
		int firstYear = firstUpdateInt / 10000;
		
		if (year < firstYear)
			return false;

		return true;
	}

	public boolean isFinancial() {
		if (category == null)
			return false;
		else if (category.equals("金融保險業") || category.equals("金融保險"))
			return true;
		else
			return false;
	}

	float getlatestPrice(MyDB db) throws Exception {
		Statement stm = db.conn.createStatement();
		ResultSet rs = stm.executeQuery(
		        "SELECT 現價 AS val FROM company WHERE StockNum = " + stockNum);
		rs.first();
		float price = rs.getFloat("val");

		rs.close();
		return price;
		
//		Statement stm = db.conn.createStatement();
//		String lastValid = "(select Date from daily WHERE StockNum = " + stockNum
//		        + " AND 收盤價 IS NOT NULL ORDER BY Date DESC LIMIT 1)";
//		String query = "SELECT 收盤價 FROM daily WHERE Date = " + lastValid + " AND StockNum = " + stockNum;
//		ResultSet rs = stm.executeQuery(query);
//
//		if (!rs.first()) {
//			throw new Exception(stockNum + " " + name + " has no latest price");
//		}
//
//		float price = rs.getFloat("收盤價");
//		stm.close();
//
//		return price;
	}

	// 取得當前本益比
	float getLatestPER(MyDB db) throws SQLException {
		Statement stm = db.conn.createStatement();
		ResultSet rs = stm.executeQuery(
		        "SELECT 本益比 AS val FROM company WHERE StockNum = " + stockNum);
		rs.first();
		float per = rs.getFloat("val");

		rs.close();
		return per;
		
//		ResultSet rs;
//		Statement stm = db.conn.createStatement();
//		rs = stm.executeQuery(
//		        "SELECT 本益比 AS val FROM daily WHERE Date = (SELECT MAX(Date) FROM daily) AND StockNum = " + stockNum);
//		rs.first();
//		float curRat = rs.getFloat("val");
//
//		rs.close();
//		return curRat;
	}

	static public Company[] getAllCompanies() throws Exception {
		return getAllCompanies(false);
	}

	static public Company[] getAllValidCompanies() throws Exception {
		return getAllCompanies(true); 
	}
	
	static public Company[] getAllCompaniesWith(String condition) throws Exception {
		MyDB db = new MyDB();
		Statement compST = db.conn.createStatement();
		String query;
			query = "SELECT StockNum,Code,Name,產業別,first_update,last_update,本益比,isOldCashflowUpdated FROM company "
					+ "WHERE StockNum > 1000 AND " + condition;
		ResultSet companySet = compST.executeQuery(query);
		
		// Get number of company
		int numCompany = MyDB.getNumRow(companySet);
		if (numCompany == 0) {
			db.close();
			return null;
		}

		Company[] companies = new Company[numCompany];
		int idx = 0;
		while (companySet.next()) {
			String code = companySet.getString("Code");
			companies[idx] = new Company(code, companySet.getInt("StockNum"), companySet.getString("Name"),
			        companySet.getString("產業別"), companySet.getDate("first_update"), companySet.getDate("last_update"), 
			        companySet.getFloat("本益比"), companySet.getBoolean("isOldCashflowUpdated"));
			idx++;
		}

		compST.close();
		db.close();
		return companies;
	}

	static private Company[] getAllCompanies(boolean valid) throws Exception {
		MyDB db = new MyDB();
		Statement compST = db.conn.createStatement();
		String query;
		if (valid)
			query = "SELECT StockNum,Code,Name,產業別,first_update,last_update,本益比,isOldCashflowUpdated FROM company "
					+ "WHERE last_update = (SELECT MAX(Date) FROM daily) AND StockNum > 1000";
		else
			query = "SELECT StockNum,Code,Name,產業別,first_update,last_update,本益比,isOldCashflowUpdated FROM company "
					+ "WHERE StockNum > 1000";
		ResultSet companySet = compST.executeQuery(query);
		
		// Get number of company
		int numCompany = MyDB.getNumRow(companySet);
		if (numCompany == 0) {
			db.close();
			return null;
		}

		Company[] companies = new Company[numCompany];
		int idx = 0;
		while (companySet.next()) {
			String code = companySet.getString("Code");
			companies[idx] = new Company(code, companySet.getInt("StockNum"), companySet.getString("Name"),
			        companySet.getString("產業別"), companySet.getDate("first_update"), companySet.getDate("last_update"), 
			        companySet.getFloat("本益比"), companySet.getBoolean("isOldCashflowUpdated"));
			idx++;
		}

		compST.close();
		db.close();
		return companies;
	}
}
