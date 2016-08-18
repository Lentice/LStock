package lstockv2;

import java.io.File;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class ImportMonthlyTaiExPrice implements Runnable {
	private static final Logger log = LogManager.getLogger(ImportMonthlyTaiExPrice.class.getName());

	private static final String FOLDER_PATH = DataPath.MONTHLY_TAIEX_PATH;
	private static final int MAX_THREAD = 20;
	private static final long MIN_DOWNLOAD_GAP = 1000;

	private static long lastDownloadTime = 0;
	private static MyStatement stm;

	private int year;
	private int month;
	private String fullFilePath;

	ImportMonthlyTaiExPrice(final int year, final int month) {
		this.year = year;
		this.month = month;

		fullFilePath = FOLDER_PATH + String.format("%04d%02d.htm", year, month);
	}

	/**
	 * 刪除最後取得的資料 最後的檔案可能是尚未更新的資料
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
		final File lastfile = files[files.length - 1];
		log.info("Remove last file " + lastfile.getName());
		lastfile.delete();

		return true;
	}

	// 下載每日台灣加權股價指數
	// 台灣加權股價指數是記錄在每個月資料中，因此下載整個月的資料，再從中取得所需資料
	private boolean download() {

		final File file = new File(fullFilePath);
		if (file.isFile()) {
			log.debug("Daily TaiEx already exist: " + fullFilePath);
			return true;
		}

		final String url = "http://www.twse.com.tw/ch/trading/indices/MI_5MINS_HIST/MI_5MINS_HIST.php";
		if (year < 2004 || (year == 2004 && month < 2)) {
			log.error("Date is earlier than 2004/02");
			return false;
		}

		// example: "year=105&mmon=03"
		final String postData = String.format("myear=%03d&mmon=%02d", year - 1911, month);

		final long downloadGap = System.currentTimeMillis() - lastDownloadTime;
		if (downloadGap < MIN_DOWNLOAD_GAP) {
			try {
				Thread.sleep(MIN_DOWNLOAD_GAP - downloadGap);
			} catch (InterruptedException e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		if (!Downloader.httpPost(url, postData, fullFilePath)) {
			log.warn(String.format("Download daily TaiEx %04d_%02d failed", year, month));
			return false;
		}
		log.info("Downloaded daily TaiEx to " + fullFilePath);

		lastDownloadTime = System.currentTimeMillis();

		return true;
	}

	public void run() {

		try {
			final File file = new File(fullFilePath);
			final Document doc = Jsoup.parse(file, "MS950");
			final Element table = doc.select("#contentblock > td > table.board_trad").first();
			final Elements rows = table.select("tbody > tr");

			for (int i = 2; i < rows.size(); i++) {
				final Elements columns = rows.get(i).children();
				final String[] date = HtmlUtil.getText(columns.get(0)).split("/");
				final String dataFormated = String.format("%04d-%02d-%s", year, month, date[2]);

				int idx = 1;
				synchronized (stm.AcquireLock()) {
					stm.setDate(idx++, Date.valueOf(dataFormated)); // Date
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(1))); // 開盤指數
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(2))); // 最高指數
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(3))); // 最低指數
					stm.setFloat(idx++, HtmlUtil.getText(columns.get(4))); // 收盤指數
					stm.addBatch();
				}
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			System.exit(-1);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void supplementDB(MyDB db) {

		File importDir = new File(FOLDER_PATH);
		if (!importDir.isDirectory()) {
			log.error("路徑不存在: " + FOLDER_PATH);
			System.exit(-1);
		}

		stm = new MyStatement(db.conn);
		stm.setStatementInsertAndUpdate("daily_summary", "Date", "開盤指數", "最高指數", "最低指數", "收盤指數");

		Calendar currentCal = Calendar.getInstance();
		final int currentYear = currentCal.get(Calendar.YEAR);
		final int currentMonth = currentCal.get(Calendar.MONTH) + 1;

		final int yearMonth = db.getLastDailyTaiExDate();
		int year = yearMonth / 100;
		int month = yearMonth % 100;

		removeLatestFile(year, month);

		ExecutorService service = Executors.newFixedThreadPool(MAX_THREAD);
		List<Future<?>> futures = new ArrayList<>();

		while (year < currentYear || (year == currentYear && month <= currentMonth)) {

			log.debug("Import dialy TaiEx " + year + "_" + month);

			ImportMonthlyTaiExPrice imp = new ImportMonthlyTaiExPrice(year, month);
			if (imp.download()) {
				Future<?> f = service.submit(imp);
				futures.add(f);
			}

			if (++month > 12) {
				year++;
				month = 1;
			}
		}

		// wait for all tasks to complete before continuing
		try {
			for (Future<?> f : futures) {
				f.get();
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		service.shutdownNow();

		stm.close();
	}

	public static void main(String[] args) throws Exception {
		MyDB db = new MyDB();
		supplementDB(db);
		db.close();
		log.info("Done!!");
	}
}
