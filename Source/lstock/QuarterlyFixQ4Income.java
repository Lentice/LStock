package lstock;

import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lstock.ui.RebuildUI;


public class QuarterlyFixQ4Income implements Runnable {
	private static final Logger log = LogManager.getLogger(QuarterlyFixQ4Income.class.getName());
	
	private static final Object uiLock = new Object();
	private static int totalImportCount;
	private static int oldPercentage;
	private static int importedCount;
	
	private static MyDB db;
	private static MyStatement stUpdate;
	
	private Company company;
	private int startYear;
	private int endYear;
	
	QuarterlyFixQ4Income(Company company, int startYear, int endYear) {
		this.company = company;
		this.startYear = startYear;
		this.endYear = endYear;
	}
	
	public void run() {

		final String uiMessage = String.format("修正第四季損益: %d %04d To %04d", company.stockNum, startYear, endYear);
		try {
			RebuildUI.addProcess(uiMessage);

			Statement stOriginal = db.conn.createStatement();
			String query = "SELECT YearQuarter,營收,成本,毛利,營業利益,業外收支,稅前淨利,稅後淨利,綜合損益,母公司業主淨利,母公司業主綜合損益,EPS,第四季損益需更正"
			        + " FROM quarterly WHERE " + "StockNum = " + company.stockNum + " AND "
			        + String.format("YearQuarter BETWEEN %d01 AND %d04", startYear, endYear)
			        + " ORDER BY YearQuarter ASC ";
	
			ResultSet rs = stOriginal.executeQuery(query);
	
			final int COLUMNS = 10;
			long sum[] = new long[COLUMNS];
			long value[] = new long[COLUMNS];
			float eps, sumEps = 0;
	
			int dataYearOld = 0;
			while (rs.next()) {
				
				int dataYearQuarter = rs.getObject("YearQuarter", Integer.class);
				value[0] = rs.getObject("營收", Long.class);
				value[1] = rs.getObject("成本", Long.class);
				value[2] = rs.getObject("毛利", Long.class);
				value[3] = rs.getObject("營業利益", Long.class);
				value[4] = rs.getObject("業外收支", Long.class);
				value[5] = rs.getObject("稅前淨利", Long.class);
				value[6] = rs.getObject("稅後淨利", Long.class);
				value[7] = rs.getObject("綜合損益", Long.class);
				value[8] = rs.getObject("母公司業主淨利", Long.class);
				value[9] = rs.getObject("母公司業主綜合損益", Long.class);
				eps = rs.getObject("EPS", Float.class);
				boolean 第四季損益需更正 = rs.getObject("第四季損益需更正", Boolean.class);
	
				int dataYear = dataYearQuarter / 100;
				if (dataYear != dataYearOld) {
					dataYearOld = dataYear;
					for (int i = 0; i < sum.length; i++)
						sum[i] = 0;
					sumEps = 0;
				}
	
				if (第四季損益需更正) {
					int index = 1;
					synchronized (stUpdate.AcquireLock()) {
						stUpdate.setBigInt(index++, value[0] - sum[0]); // 營收
						stUpdate.setBigInt(index++, value[1] - sum[1]); // 成本
						stUpdate.setBigInt(index++, value[2] - sum[2]); // 毛利
						stUpdate.setBigInt(index++, value[3] - sum[3]); // 營業利益
						stUpdate.setBigInt(index++, value[4] - sum[4]); // 業外收支
						stUpdate.setBigInt(index++, value[5] - sum[5]); // 稅前淨利
						stUpdate.setBigInt(index++, value[6] - sum[6]); // 稅後淨利
						stUpdate.setBigInt(index++, value[7] - sum[7]); // 綜合損益
						stUpdate.setBigInt(index++, value[8] - sum[8]); // 母公司業主淨利
						stUpdate.setBigInt(index++, value[9] - sum[9]); // 母公司業主綜合損益
						stUpdate.setFloat(index++, eps - sumEps); // EPS
						stUpdate.setObject(index++, Boolean.FALSE); // 第四季損益需更正
						stUpdate.setInt(index++, dataYearQuarter); // YearQuarter
						stUpdate.setInt(index++, company.stockNum); // StockNum
						stUpdate.addBatch();
					}
					
					synchronized (uiLock) {
						importedCount++;
					}
					
				} else {
					for (int i = 0; i < COLUMNS; i++) {
						sum[i] += value[i];
					}
					sumEps += eps;
				}
			}
			
			stOriginal.close();

		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			RebuildUI.removeProcess(uiMessage);
			synchronized (uiLock) {
				int percentage = importedCount * 100 / totalImportCount;
				if (oldPercentage != percentage) {
					oldPercentage = percentage;
					RebuildUI.updateProgressBar(percentage);
				}
			}
		}
	}
	
	public static void fixIncome(MyDB db) throws Exception {
		totalImportCount = db.incomeNeedBeFixed();
		if (totalImportCount == 0) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		importedCount = 0;
		
		QuarterlyFixQ4Income.db = db;
		
		QuarterlyFixQ4Income.stUpdate = new MyStatement(db.conn);
		stUpdate.setStatementUpdate("quarterly", "YearQuarter=? AND StockNum=?", "營收", "成本", "毛利", "營業利益", "業外收支",
		        "稅前淨利", "稅後淨利", "綜合損益", "母公司業主淨利", "母公司業主綜合損益", "EPS", "第四季損益需更正");
	
		int startYear = db.getFirstYearOfCheckItemFromQuarterly("第四季損益需更正", 1);
	
		int[] currentQuarter = QuarterlyData.getLastAvalibleQuarter();
		int endYear = currentQuarter[0];
	
		MyThreadPool threadPool = new MyThreadPool();
		
		Company[] companies = Company.getAllValidCompanies();
		
		for (Company company : companies) {
			log.debug("修正第四季損益表 " + company.stockNum);
			threadPool.add(new QuarterlyFixQ4Income(company, startYear, endYear));
		}
		
		threadPool.waitFinish();
	
		stUpdate.close();
	}

	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		
		fixIncome(db);

		db.close();
		log.info("Done!!");
	}
}
