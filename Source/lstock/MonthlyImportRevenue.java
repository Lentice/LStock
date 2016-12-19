package lstock;

import java.io.File;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import lstock.ui.RebuildUI;

public class MonthlyImportRevenue implements Runnable {
	private static final Logger log = LogManager.getLogger(MonthlyImportRevenue.class.getName());

	private static final String FOLDER_PATH = DataPath.MONTHLY_REVENUE_PATH;
	private static final long MIN_DOWNLOAD_GAP = 1000;
	private static final Downloader downloader = new Downloader(MIN_DOWNLOAD_GAP);

	private static MyStatement companyST;
	private static MyStatement monthST;
	
	private static final Object uiLock = new Object();
	private static int totalMonths;
	private static int oldPercentage;
	private static int monthsImported;

	private int year;
	private int month;
	private String fullFilePath1 = null;
	private String fullFilePath2 = null;

	MonthlyImportRevenue(final int year, final int month) {
		this.year = year;
		this.month = month;

		if (year < 2013) {
			fullFilePath1 = DataPath.MONTHLY_REVENUE_PATH + String.format("%04d%02d_sii.html", year, month);
		} else {
			fullFilePath1 = DataPath.MONTHLY_REVENUE_PATH + String.format("%04d%02d_sii_0.html", year, month);
			fullFilePath2 = DataPath.MONTHLY_REVENUE_PATH + String.format("%04d%02d_sii_1.html", year, month);
		}
	}
	
	public static int monthsBetween(int startYear, int startMonth, int endYear, int endMonth)
	{
		return (endYear - startYear) * 12 + endMonth - startMonth + 1;
	}

	/**
	 * 刪除最後取得的資料 最後一天可能是取得尚未更新的資料
	 */
	private static boolean removeLatestFile(final int year, final int month) {
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
		File lastfile = files[files.length - 1];
		String filename = lastfile.getPath();
		if (filename.endsWith("_sii_1.html")) {
			log.info("Remove last file " + lastfile.getName());
			lastfile.delete();
			
			filename = filename.replace("_sii_1.html", "_sii_0.html");
			lastfile = new File(filename);
			log.info("Remove last file " + lastfile.getName());
			lastfile.delete();
		} else {
			log.info("Remove last file " + lastfile.getName());
			lastfile.delete();
		}
	
		return true;
	}

	private boolean downloadIfNotExist(final String url, final String fullFilePath) {
		File file = new File(fullFilePath);
		if (file.isFile()) {
			log.debug("Monthly revenue already exist: " + fullFilePath);
			return true;
		}

		RebuildUI.addDownload(fullFilePath);
		if (!downloader.httpGet(url, fullFilePath)) {
			log.warn(String.format("Download monthly revenue %04d_%02d failed", year, month));
			return false;
		}
		log.info("Downloaded revenue to " + fullFilePath);

		return true;
	}

	// 下載每日台灣加權股價指數
	// 台灣加權股價指數是記錄在每個月資料中，因此下載整個月的資料，再從中取得所需資料
	private boolean download() {
		final int twYear = year - 1911;

		if (year < 2004) {
			log.error("Date is earlier than 2004");
			return false;
		}

		String url;
		boolean result = true;
		if (year < 2013) {
			url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d.html", twYear, month);
			result = downloadIfNotExist(url, fullFilePath1);
		} else {
			// 市場別：國內
			url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_0.html", twYear, month);
			if (!downloadIfNotExist(url, fullFilePath1))
				result = false;

			// 市場別：國外
			url = String.format("http://mops.twse.com.tw/nas/t21/sii/t21sc03_%d_%d_1.html", twYear, month);
			if (!downloadIfNotExist(url, fullFilePath2))
				result = false;
		}

		return result;
	}

	private void parse(final String fullFilePath) {
		final File file = new File(fullFilePath);
		if (!file.isFile()) {
			log.warn("檔案不存在: " + fullFilePath);
			return;
		}

		final int yearMonth = year * 100 + month;
		try {
			final Document doc = Jsoup.parse(file, "MS950");
			final Elements eAllCategories = doc.getElementsContainingOwnText("產業別：");

			final Elements eTitles = doc.getElementsContainingOwnText("公司名稱");
			for (int iTable = 0; iTable < eAllCategories.size(); iTable++) {
				final Elements rows = eTitles.get(iTable).parent().parent().children();

				final int dropHead = 2; // 標題佔用2列
				final int dropTail = 1; // 最後一列合計
				if (rows.size() <= (dropHead + dropTail)) // 標題 2 + 合計 1 rows
					continue;

				String category = eAllCategories.get(iTable).text().replaceFirst("產業別：", "").trim();
				final int pos = category.indexOf('（'); // 去除產業別後面括號內容
				if (pos != -1) {
					category = category.substring(0, pos);
				}

				for (int iRow = dropHead; iRow < rows.size() - dropTail; iRow++) {
					final Elements columns = rows.get(iRow).children();
					final String stockNum = HtmlUtil.getText(columns.get(0));

					// TODO: FIX ME
					try {
						Integer.parseInt(stockNum);
					} catch (NumberFormatException | NullPointerException e) {
						continue;
					}

					int idx;
					;
					synchronized (companyST.AcquireLock()) {
						idx = 1;
						companyST.setChar(idx++, category);
						companyST.setInt(idx++, stockNum); // StockNum
						companyST.addBatch();
					}

					synchronized (monthST.AcquireLock()) {
						idx = 1;
						monthST.setInt(idx++, yearMonth); // YearMonth
						monthST.setInt(idx++, stockNum); // StockNum
						monthST.setBigInt(idx++, HtmlUtil.getText(columns.get(2))); // 當月營收
						monthST.setBigInt(idx++, HtmlUtil.getText(columns.get(3))); // 上月營收
						monthST.setBigInt(idx++, HtmlUtil.getText(columns.get(4))); // 去年當月營收
						monthST.setFloat(idx++, HtmlUtil.getText(columns.get(5))); // 上月比較增減
						monthST.setFloat(idx++, HtmlUtil.getText(columns.get(6))); // 去年同月增減
						monthST.setBigInt(idx++, HtmlUtil.getText(columns.get(7))); // 當月累計營收
						monthST.setBigInt(idx++, HtmlUtil.getText(columns.get(8))); // 去年累計營收
						monthST.setFloat(idx++, HtmlUtil.getText(columns.get(9))); // 前期比較增減
						if (columns.size() >= 11)
							monthST.setBlob(idx++, HtmlUtil.getText(columns.get(10))); // 備註
						else
							monthST.setBlob(idx++, null);

						monthST.addBatch();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public void run() {
		
		final String uiMessage = String.format("匯入營收: %04d-%02d", year, month);
		RebuildUI.addProcess(uiMessage);

		parse(fullFilePath1);

		if (fullFilePath2 != null) {
			parse(fullFilePath2);
		}
		
		RebuildUI.removeProcess(uiMessage);
		synchronized (uiLock) {
			int percentage = ++monthsImported * 100 / totalMonths;
			if (oldPercentage != percentage) {
				oldPercentage = percentage;
				RebuildUI.updateProgressBar(percentage);
			}
		}
	}

	public static void importToDB(MyDB db) {
		
		Calendar currentCal = Calendar.getInstance();
		int endYear = currentCal.get(Calendar.YEAR);
		int endMonth = (currentCal.get(Calendar.MONTH) + 1) - 1;
		if (currentCal.get(Calendar.DATE) <= 10)
			endMonth -= 2;
		else
			endMonth -= 1;

		int yearMonth = db.getLastMonthlyRevenue();
		int year = yearMonth / 100;
		int month = yearMonth % 100;
		
		totalMonths = monthsBetween(year, month, endYear, endMonth);
		if (totalMonths <= 1) {
			RebuildUI.updateProgressBar(100);
			return;
		}
		
		RebuildUI.updateProgressBar(0);
		oldPercentage = 0;
		monthsImported = 0;

		new File(FOLDER_PATH).mkdirs(); // create folder if it was not exist

		String update = "UPDATE company SET 產業別 = ? WHERE StockNum = ?";
		companyST = new MyStatement(db.conn, update);

		monthST = new MyStatement(db.conn);
		monthST.setStatementInsertIgnore("monthly", "YearMonth", "StockNum", "當月營收", "上月營收", "去年當月營收", "上月比較增減",
				"去年同月增減", "當月累計營收", "去年累計營收", "前期比較增減", "備註");

		removeLatestFile(year, month);

		MyThreadPool threadPool = new MyThreadPool();

		while (year < endYear || (year == endYear && month < endMonth)) {
			MonthlyImportRevenue imp = new MonthlyImportRevenue(year, month);
			if (imp.download()) {
				threadPool.add(imp);
			}

			if (++month > 12) {
				year++;
				month = 1;
			}
		}

		threadPool.waitFinish();

		companyST.close();
		monthST.close();
	}

	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		importToDB(db);
		db.close();
		log.info("Done!!");
	}
}
