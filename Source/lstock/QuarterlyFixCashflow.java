package lstock;

import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import lstock.ui.RebuildUI;

public class QuarterlyFixCashflow implements Runnable {
	
	private static final Logger log = LogManager.getLogger(QuarterlyFixCashflow.class.getName());
	
	private static final Object uiLock = new Object();
	private static int totalImportCount;
	private static int oldPercentage;
	private static int importedCount;
	
	private static MyDB db;
	private static MyStatement stUpdate;
	
	private Company company;
	private int startYear;
	private int endYear;
	
	QuarterlyFixCashflow(Company company, int startYear, int endYear) {
		this.company = company;
		this.startYear = startYear;
		this.endYear = endYear;
	}
	
	public void run() {
		
		final String uiMessage = String.format("修正現金流量表: %d %04d To %04d", company.stockNum, startYear, endYear);
		try {
			RebuildUI.addProcess(uiMessage);
			
			Statement stOriginal = db.conn.createStatement();
			String query = "SELECT YearQuarter,利息費用,營業現金流,投資現金流,融資現金流,自由現金流,淨現金流,現金流累計需更正 FROM quarterly WHERE "
			        + "StockNum = " + company.stockNum + " AND "
			        + String.format("YearQuarter BETWEEN %d01 AND %d04", startYear, endYear)
			        + " ORDER BY YearQuarter ASC ";
	
			ResultSet rs = stOriginal.executeQuery(query);
	
			long sum[] = new long[6];
			long value[] = new long[6];
	
			int dataYearOld = 0;
			while (rs.next()) {
				int dataYearQuarter = rs.getObject("YearQuarter", Integer.class);
				value[0] = rs.getObject("利息費用", Long.class);
				value[1] = rs.getObject("營業現金流", Long.class);
				value[2] = rs.getObject("投資現金流", Long.class);
				value[3] = rs.getObject("融資現金流", Long.class);
				value[4] = rs.getObject("自由現金流", Long.class);
				value[5] = rs.getObject("淨現金流", Long.class);
				boolean 現金流累計需更正 = rs.getObject("現金流累計需更正", Boolean.class);
	
				int dataYear = dataYearQuarter / 100;
				if (dataYear != dataYearOld) {
					dataYearOld = dataYear;
					for (int j = 0; j < sum.length; j++)
						sum[j] = 0;
				}
	
				if (現金流累計需更正) {
					for (int j = 0; j < sum.length; j++)
						value[j] -= sum[j];
				}
	
				for (int j = 0; j < sum.length; j++) {
					sum[j] += value[j];
				}
	
				if (現金流累計需更正) {
					int index = 1;
					synchronized (stUpdate.AcquireLock()) {
						stUpdate.setBigInt(index++, value[0]); // 利息費用
						stUpdate.setBigInt(index++, value[1]); // 營業現金流
						stUpdate.setBigInt(index++, value[2]); // 投資現金流
						stUpdate.setBigInt(index++, value[3]); // 融資現金流
						stUpdate.setBigInt(index++, value[4]); // 自由現金流
						stUpdate.setBigInt(index++, value[5]); // 淨現金流
						stUpdate.setObject(index++, Boolean.FALSE); // 現金流累計需更正
						stUpdate.setInt(index++, dataYearQuarter); // YearQuarter
						stUpdate.setInt(index++, company.stockNum); // StockNum
						stUpdate.addBatch();
					}
					
					synchronized (uiLock) {
						importedCount++;
					}
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

	public static void fixCashflow(MyDB db) throws Exception {
		totalImportCount = db.cashflowNeedBeFixed();
		if (totalImportCount == 0) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		importedCount = 0;
		
		QuarterlyFixCashflow.db = db;
		QuarterlyFixCashflow.stUpdate = new MyStatement(db.conn);
		
		stUpdate.setStatementUpdate("quarterly", "YearQuarter=? AND StockNum=?", "利息費用", "營業現金流", "投資現金流", "融資現金流",
		        "自由現金流", "淨現金流", "現金流累計需更正");
	
		int startYear = db.getFirstYearOfCheckItemFromQuarterly("現金流累計需更正", 1);
	
		int[] currentQuarter = QuarterlyData.getLastAvalibleQuarter();
		int endYear = currentQuarter[0];
	
		MyThreadPool threadPool = new MyThreadPool();
		
		Company[] companies = Company.getAllValidCompanies();
		for (Company company : companies) {
			log.debug("修正現金流量表累計: " + company.stockNum);
	
			threadPool.add(new QuarterlyFixCashflow(company, startYear, endYear));
		}
		
		threadPool.waitFinish();
	
		stUpdate.close();
	}
	
	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		
		fixCashflow(db);

		db.close();
		log.info("Done!!");
	}
}
