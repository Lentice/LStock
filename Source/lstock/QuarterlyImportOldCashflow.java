package lstock;

import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import lstock.ui.RebuildUI;

public class QuarterlyImportOldCashflow implements Runnable {
	private static final Logger log = LogManager.getLogger(QuarterlyImportOldCashflow.class.getName());
	
	private static final int MAX_DOWNLOAD_RETRY = 20;

	// 10 ~ 14 seconds
	private static final long MIN_DOWNLOAD_GAP = 10000;
	private static final long DOWNLOAD_GAP_RANDOM_SHIFT = 4000;
	private static final Downloader downloader = new Downloader(MIN_DOWNLOAD_GAP, DOWNLOAD_GAP_RANDOM_SHIFT);
	
	private static final Object uiLock = new Object();
	private static int totalImportCount;
	private static int oldPercentage;
	private static int importedCount;
	
	private static MyStatement stQuarterlyCashflow;
	private static MyStatement stAnnualCashflow;
	private static MyStatement stOldCashflowUpdated;
	
	private Company company;
	
	QuarterlyImportOldCashflow(Company company) {
		this.company = company;
	}
	
	private boolean downloadGoodInfoCashflowSummary() throws Exception {
		final String folder = DataPath.CASHFLOW_SUM;
		final String url = "http://goodinfo.tw/StockInfo/StockCashFlow.asp?";

		downloader.enableProxy();
		try {
			for (int idv = 0; idv <= 1; idv++) {
				String fullFilePath, reportCat;
				if (idv == 0) {
					fullFilePath = folder + String.format("%d.html", company.stockNum);
					reportCat = "M_QUAR";
				} else {
					fullFilePath = folder + String.format("%d_idv.html", company.stockNum);
					reportCat = "QUAR";
				}

				File file = new File(fullFilePath);
				if (file.isFile() && file.length() > 4096) {
					log.debug("Cashflow file already exist: " + file.getPath());
					continue;
				}

				String getData = String.format("STOCK_ID=%d&RPT_CAT=%s", company.stockNum, reportCat);

				int retry = 0;
				while (retry < MAX_DOWNLOAD_RETRY) {
					downloader.httpGet(url + getData, fullFilePath);
					log.info(String.format("Download[%d] to %s", retry, fullFilePath));
					RebuildUI.addDownload("retry[" + retry + "]: " + fullFilePath);

					file = new File(fullFilePath);
					if (file.isFile() && file.length() > 1024) {
						break;
					} else {
						retry++;
						file.delete();
						downloader.nextProxy();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		downloader.disableProxy();
		downloader.setMinDownloadGap(MIN_DOWNLOAD_GAP);
		return true;
	}
	
	private static boolean parseGoodInfoCashflowHtml(final boolean idv, final Company company,
	        HashMap<String, Float[]> quarterDataList) throws Exception {
		File file;
		if (idv)
			file = new File(DataPath.CASHFLOW_SUM + String.format("%d_idv.html", company.stockNum));
		else
			file = new File(DataPath.CASHFLOW_SUM + String.format("%d.html", company.stockNum));
		if (!file.isFile()) {
			return false;
		}

		Document doc = Jsoup.parse(file, "UTF-8");
		Elements tbody = doc.select("#divDetail > table > tbody");
		if (tbody.isEmpty())
			return false;

		Elements rows = tbody.select(":root > tr");
		Float[] cashflow; // 營業活動現金流, 投資活動現金流, 融資活動現金流
		for (int i = 0; i < rows.size(); i++) {
			Elements columns = rows.get(i).select(":root > td");
			if (columns.size() < 17)
				continue;

			String yearQuarter = HtmlUtil.getText(columns.get(0));
			if (yearQuarter == null || !yearQuarter.contains("Q"))
				continue;

			yearQuarter = yearQuarter.replace("Q", "0");
			cashflow = quarterDataList.get(yearQuarter);
			if (cashflow == null) {
				cashflow = new Float[3];
				quarterDataList.put(yearQuarter, cashflow);
			}

			Float 營業活動現金流 = HtmlUtil.getFloat(columns.get(9));
			Float 投資活動現金流 = HtmlUtil.getFloat(columns.get(10));
			Float 融資活動現金流 = HtmlUtil.getFloat(columns.get(11));
			if (cashflow[0] == null || cashflow[0] == 0)
				cashflow[0] = 營業活動現金流 == null ? null : 營業活動現金流 * 100000;
			if (cashflow[1] == null || cashflow[1] == 0)
				cashflow[1] = 投資活動現金流 == null ? null : 投資活動現金流 * 100000;
			if (cashflow[2] == null || cashflow[2] == 0)
				cashflow[2] = 融資活動現金流 == null ? null : 融資活動現金流 * 100000;

		}

		return true;
	}
	
	public void run() {
		
		final String uiMessage = String.format("匯入舊現金流量表: %d", company.stockNum);
		
		try {
			RebuildUI.addProcess(uiMessage);
			
			if (!downloadGoodInfoCashflowSummary()) {
				synchronized (uiLock) {
					int percentage = ++importedCount * 100 / totalImportCount;
					if (oldPercentage != percentage) {
						oldPercentage = percentage;
						RebuildUI.updateProgressBar(percentage);
					}
				}
				return;
			}

			HashMap<String, Float[]> quarterDataList = new HashMap<>();
			parseGoodInfoCashflowHtml(false, company, quarterDataList);
			parseGoodInfoCashflowHtml(true, company, quarterDataList);

			int year = MyDB.DEFAULT_FIRST_YEAR;
			int quarter = 1;
			Long sum營業現金流 = new Long(0);
			Long sum投資現金流 = new Long(0);
			Long sum融資現金流 = new Long(0);
			while (year < 2013) {
				if (company.isValidQuarter(year, quarter)) {
					
					String key = String.format("%d%02d", year, quarter);
					Float[] cashflow = quarterDataList.get(key);
					if (cashflow != null) {
						Long 營業現金流 = cashflow[0].longValue();
						Long 投資現金流 = cashflow[1].longValue();
						Long 融資現金流 = cashflow[2].longValue();
						Long 自由現金流 = null;
						Long 淨現金流 = null;
						if (營業現金流 != null && 投資現金流 != null) {
							自由現金流 = 營業現金流 - 投資現金流;
							if (融資現金流 != null)
								淨現金流 = 自由現金流 + 融資現金流;
						}
	
						if (營業現金流 != null)
							sum營業現金流 += 營業現金流;
						if (投資現金流 != null)
							sum投資現金流 += 營業現金流;
						if (融資現金流 != null)
							sum融資現金流 += 營業現金流;
	
						if (營業現金流 != null || 投資現金流 != null || 融資現金流 != null) {
							
							synchronized (stQuarterlyCashflow.AcquireLock()) {
								stQuarterlyCashflow.setObject(Integer.valueOf(year * 100 + quarter)); // YearQuarter
								stQuarterlyCashflow.setObject(Integer.valueOf(company.stockNum)); // StockNum
								stQuarterlyCashflow.setObject(營業現金流); // 營業現金流
								stQuarterlyCashflow.setObject(投資現金流); // 投資現金流
								stQuarterlyCashflow.setObject(融資現金流); // 融資現金流
								stQuarterlyCashflow.setObject(自由現金流); // 自由現金流
								stQuarterlyCashflow.setObject(淨現金流); // 淨現金流
								stQuarterlyCashflow.setObject(Boolean.FALSE); // 現金流累計需更正
								stQuarterlyCashflow.addBatch();
							}
	
							if (quarter == 4) {
								Long sum自由現金流 = null, sum淨現金流 = null;
								if (sum營業現金流 != null && sum投資現金流 != null) {
									sum自由現金流 = sum營業現金流 - sum投資現金流;
									if (sum融資現金流 != null)
										sum淨現金流 = sum自由現金流 + sum融資現金流;
								}
	
								synchronized (stAnnualCashflow.AcquireLock()) {
									stAnnualCashflow.setObject(Integer.valueOf(year)); // Year
									stAnnualCashflow.setObject(Integer.valueOf(company.stockNum)); // StockNum
									stAnnualCashflow.setObject(sum營業現金流); // 營業現金流
									stAnnualCashflow.setObject(sum投資現金流); // 投資現金流
									stAnnualCashflow.setObject(sum融資現金流); // 融資現金流
									stAnnualCashflow.setObject(sum自由現金流); // 自由現金流
									stAnnualCashflow.setObject(sum淨現金流); // 淨現金流
									stAnnualCashflow.addBatch();
								}
							}
						}
					}
				}
				if (++quarter == 5) {
					quarter = 1;
					year++;
					sum營業現金流 = new Long(0);
					sum投資現金流 = new Long(0);
					sum融資現金流 = new Long(0);
				}
			}
			
			setOldCashflowUpdated(company.stockNum);
			RebuildUI.removeProcess(uiMessage);
			
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	private static void setOldCashflowUpdated(int stockNum) throws SQLException {
		
		synchronized (stOldCashflowUpdated.AcquireLock()) {
			stOldCashflowUpdated.setObject(Boolean.TRUE);
			stOldCashflowUpdated.setObject(Integer.valueOf(stockNum));
			stOldCashflowUpdated.addBatch();
		}
		
		synchronized (uiLock) {
			int percentage = ++importedCount * 100 / totalImportCount;
			if (oldPercentage != percentage) {
				oldPercentage = percentage;
				RebuildUI.updateProgressBar(percentage);
			}
		}
	}
	
	public static void importOldCashflowToDb(MyDB db) throws Exception {
		
		Company[] companies = Company.getAllCompaniesWith("isOldCashflowUpdated = 0");
		
		if (companies == null || companies.length == 0) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		totalImportCount = companies.length;
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		importedCount = 0;
		
		stQuarterlyCashflow = new MyStatement(db.conn);
		stAnnualCashflow = new MyStatement(db.conn);
		stOldCashflowUpdated = new MyStatement(db.conn);

		stQuarterlyCashflow.setStatementInsertAndUpdate("quarterly", "YearQuarter", "StockNum", "營業現金流", "投資現金流",
		        "融資現金流", "自由現金流", "淨現金流", "現金流累計需更正");
		stAnnualCashflow.setStatementInsertAndUpdate("annual", "Year", "StockNum", "營業現金流", "投資現金流", "融資現金流", "自由現金流",
		        "淨現金流");
		
		stOldCashflowUpdated.setStatementUpdate("company", "StockNum = ?", "isOldCashflowUpdated");
		
		MyThreadPool threadPool = new MyThreadPool();

		for (Company company : companies) {
			
			if (!company.isValidQuarter(2012, 4)) {
				log.debug(company.code + " skipped: 已下市或未上市");
				setOldCashflowUpdated(company.stockNum);
				continue;
			}
			
			if (company.stockNum == 9103 || company.stockNum == 9105
				 || company.stockNum == 9106 || company.stockNum == 9110
				 || company.stockNum == 9136 || company.stockNum == 9151
				 || company.stockNum == 9157 || company.stockNum == 9188
				 || company.stockNum == 9801 || company.stockNum >= 900000) {
				
				log.debug(company.code + " skipped: 目前無資料");
				setOldCashflowUpdated(company.stockNum);
				continue;
			}
			
			log.debug("Import old cashflow: " + company.stockNum);

			threadPool.add(new QuarterlyImportOldCashflow(company));
		}
		
		threadPool.waitFinish();

		stQuarterlyCashflow.close();
		stAnnualCashflow.close();
		stOldCashflowUpdated.close();
	}
}
