import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class EtfComponentBase {
	static final String folderPath = DataPath.ETF_PATH;
	String filename;
	String url;
	boolean needUpdate;
	File file;
	String[][] data;
	String field; // SQL filed name

	EtfComponentBase(String filename, String url, String field) throws Exception {
		this.filename = folderPath + filename;
		this.url = url;
		this.field = field;

		file = new File(this.filename);

		if (!file.exists()) {
			download();
			file = new File(this.filename);
			return;
		}

		needUpdate = needUpdate();
		if (needUpdate) {
			download();
			file = new File(this.filename);
			return;
		}
	}

	boolean needUpdate() throws Exception {
		Calendar lastModified = Calendar.getInstance();
		lastModified.setTimeInMillis(file.lastModified());
		lastModified.add(Calendar.DAY_OF_MONTH, 1);

		Calendar cal = Calendar.getInstance();
		if (cal.compareTo(lastModified) > 0) {
			download();
			file = new File(filename);
			return true;
		}

		return false;
	}

	int download() throws Exception {
		Downloader.createFolder(folderPath);

		Log.info("Download to " + filename);
		int ret = Downloader.httpDownload(url, filename);
		if (ret != 0) {
			Log.info("Fail");
			return ret;
		}

		return 0;
	}

	boolean parse() throws IOException {
		if (!file.exists()) {
			Log.warn("檔案不存在: " + file.getPath());
			return false;
		}

		Document doc = Jsoup.parse(file, "MS950");
		Element eTitle = doc.getElementsMatchingOwnText("代 號").first();
		Elements eTRs = eTitle.parent().parent().children();

		final int offset = 3;
		data = new String[eTRs.size() - offset][2];
		for (int i = 0; i < data.length; i++) {
			Elements eTDs = eTRs.get(i + offset).children();

			data[i][0] = HtmlParser.getText(eTDs.get(0));
			data[i][1] = HtmlParser.getPercent(eTDs.get(5));
		}

		return true;
	}
	
	void reset(MyDB db, String field) throws SQLException {
		Statement stm = db.conn.createStatement();
		stm.executeUpdate("UPDATE company set " + field + " = 0");
		stm.close();
	}
	
	public void importToDB(MyDB db) throws Exception {
		if (!parse())
			return;
		
		reset(db, field);
		
		final String query = String.format("UPDATE company set %s=? where StockNum=?", field);
		MyStatement stm = new MyStatement(db.conn, query);
		for (int j = 0; j < data.length; j++) {
			
			int idx = 1;
			stm.setDecimal(idx++, data[j][1]); // StockNum
			stm.setInt(idx++, data[j][0]); // StockNum
			stm.addBatch();
		}
		stm.close();
	}
}

class Tw50 extends EtfComponentBase {
	static final String filename = "TW50.htm";
	static final String url = "http://www.twse.com.tw/ch/trading/indices/twco/tai50i_print.php?language=ch&datafile=twco_c.htm";
	static final String field = "台灣50成份比重";
	
	Tw50() throws Exception {
		super(filename, url, field);
	}
}

class Tw100 extends EtfComponentBase {
	static final String filename = "TW100.htm";
	static final String url = "http://www.twse.com.tw/ch/trading/indices/tmcc/tai100i_print.php?language=ch&datafile=tmcc_c.htm";
	static final String field = "台灣中型100成分比重";
	
	Tw100() throws Exception {
		super(filename, url, field);
	}
}

class TwDiv extends EtfComponentBase {
	static final String filename = "TwDiv.htm";
	static final String url = "http://www.twse.com.tw/ch/trading/indices/twdp/taidividi_print.php?language=ch&datafile=twdp_c.htm";
	static final String field = "高股息成分比重";
	
	TwDiv() throws Exception {
		super(filename, url, field);
	}
}

class TwEightIndustry extends EtfComponentBase {
	static final String filename = "TwEightIndustry.htm";
	static final String url = "http://www.twse.com.tw/ch/trading/indices/twei/taiei_print.php?language=ch&datafile=twei_c.htm";
	static final String field = "台灣發達成分比重";
	
	TwEightIndustry() throws Exception {
		super(filename, url, field);
	}
}

public class ImportETF {
	
	static void importAllToDB(MyDB db) throws Exception {
		Tw50 tw50 = new Tw50();
		tw50.importToDB(db);
		
		Tw100 tw100 = new Tw100();
		tw100.importToDB(db);
		
		TwDiv twdiv = new TwDiv();
		twdiv.importToDB(db);
		
		TwEightIndustry twEI = new TwEightIndustry();
		twEI.importToDB(db);
	}
	
	public static void main(String[] args) {
		try {
			MyDB db = new MyDB();
			
			importAllToDB(db);
			
			db.close();
			Log.info("Done");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
