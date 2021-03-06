package lstock;

import java.io.File;
import java.sql.Date;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import lstock.ui.RebuildUI;

public class AnnualImportDividend implements Runnable {
	private static final Logger log = LogManager.getLogger(AnnualImportDividend.class.getName());

	private static final String FOLDER_PATH = DataPath.ANNUAL_DIVIDEND;
	private static final int MIN_DOWNLOAD_GAP = 10000;
	private static final Downloader downloader = new Downloader(MIN_DOWNLOAD_GAP);
	
	private static MyStatement stm;
	private static final Object uiLock = new Object();
	private static int totalImportCount;
	private static int oldPercentage;
	private static int importedCount;

	private int year;
	private String fullFilePath;

	AnnualImportDividend(final int year) {
		this.year = year;

		fullFilePath = FOLDER_PATH + String.format("GoodInfo_%04d.html", year);
	}

	/**
	 * 刪除最後取得的資料 最後的檔案可能是尚未更新的資料
	 */
	private static boolean removeLatestFile(final int year) {
		final File dir = new File(FOLDER_PATH);
		if (!dir.isDirectory())
			return false;

		final File[] files = dir.listFiles();
		if (files.length == 0)
			return false;

		Comparator<File> comparator = new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				return ((File) f1).getName().compareTo(((File) f2).getName());
			}
		};

		Arrays.sort(files, comparator);
		final File lastfile = files[files.length - 1];
		log.info("Remove last file " + lastfile.getName());
		lastfile.delete();
	
		return true;
	}

	public void run() {

		final String uiMessage = String.format("股利: %d", year);
		try {
			RebuildUI.addProcess(uiMessage);
			
			if (!download())
				return;
			
			final File file = new File(fullFilePath);
			final Document doc = Jsoup.parse(file, "UTF-8");
			final Element table = doc.select("#divDetail > table").first();
			final Elements rows = table.select("tbody > tr");

			for (int i = 0; i < rows.size(); i++) {
				final Elements columns = rows.get(i).children();
				if (!"上市".equals(HtmlUtil.getText(columns.get(0))))
					continue;

				// TODO: FIX ME
				final String stockNum = HtmlUtil.getText(columns.get(1));
				try {
					Integer.parseInt(stockNum);
				} catch (NumberFormatException | NullPointerException e) {
					continue;
				}

				Date 除息日期, 除權日期;
				String text除息日期 = HtmlUtil.getText(columns.get(5));
				if (text除息日期 != null) {
					final String[] dateSplited = text除息日期.split("/");
					dateSplited[2] = dateSplited[2].split(" ")[0]; // 去除附加說明文字：例如
																	// (即將除權)、
																	// (即將除息)、(今日除息)
					除息日期 = Date.valueOf(String.format("%s-%s-%s", dateSplited[0], dateSplited[1], dateSplited[2]));
				} else {
					除息日期 = null;
				}

				String text除權日期 = HtmlUtil.getText(columns.get(7));
				if (text除權日期 != null) {
					final String[] dateSplited = text除權日期.split("/");
					dateSplited[2] = dateSplited[2].split(" ")[0]; // 去除附加說明文字：例如
																	// (即將除權)、
																	// (即將除息)、(今日除息)
					除權日期 = Date.valueOf(String.format("%s-%s-%s", dateSplited[0], dateSplited[1], dateSplited[2]));
				} else {
					除權日期 = null;
				}

				int idx = 1;
				synchronized (stm.AcquireLock()) {
					stm.setInt(idx++, year); // Year
					stm.setInt(idx++, stockNum); // StockNum
					stm.setDate(idx++, 除息日期);
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(6))); // 除息參考價
					stm.setDate(idx++, 除權日期);
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(8))); // 除權參考價
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(10))); // 現金盈餘
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(11))); // 現金公積
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(13))); // 股票盈餘
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(14))); // 股票公積
					stm.addBatch();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		} finally {
			RebuildUI.removeProcess(uiMessage);
			synchronized (uiLock) {
				int percentage = ++importedCount * 100 / totalImportCount;
				if (oldPercentage != percentage) {
					oldPercentage = percentage;
					RebuildUI.updateProgressBar(percentage);
				}
			}
		}
	}

	// 下載年度除權息資料
	private boolean download() {
		try {
			final File file = new File(fullFilePath);
			if (file.isFile()) {
				log.debug("Annual dividend file already exist: " + fullFilePath);
				return true;
			}

			final String url = String.format(
					"http://goodinfo.tw/StockInfo/StockDividendScheduleList.asp?MARKET_CAT=上市&INDUSTRY_CAT=全部&YEAR=%d",
					year);
			RebuildUI.addDownload(fullFilePath);
			if (!downloader.httpGet(url, fullFilePath)) {
				log.warn("Download %d dividend failed", year);
				return false;
			}
			log.info("Downloaded annual dividend to " + fullFilePath);

			

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return true;
	}

	public static void importToDB(MyDB db) {
		
		new File(FOLDER_PATH).mkdirs(); // create folder if it was not exist
		
		Calendar cal = Calendar.getInstance();
		int currentYear = cal.get(Calendar.YEAR);
		int currentMonth = cal.get(Calendar.MONTH) + 1;
		if (currentMonth < 4) {
			currentYear--;
		}

		final int dbLastUpdatedYear = db.getLastAnnualRevenue();
		removeLatestFile(dbLastUpdatedYear);
		
		// current year still need to be reloaded
		totalImportCount = currentYear - dbLastUpdatedYear;
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		importedCount = 0;

		stm = new MyStatement(db.conn);
		stm.setStatementInsertAndUpdate("annual", "Year", "StockNum", "除息日期", "除息參考價", "除權日期", "除權參考價", "現金盈餘", "現金公積", "股票盈餘", "股票公積");

		MyThreadPool threadPool = new MyThreadPool();


		for (int year = dbLastUpdatedYear; year < currentYear; year++) {
			threadPool.add(new AnnualImportDividend(year));
		}

		threadPool.waitFinish();

		stm.close();
	}

	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		importToDB(db);
		db.close();
		log.info("Done!!");
	}
}
