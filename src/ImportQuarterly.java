import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

class FinancialParser {
	static final int MAX_ROWS = 39;
	static final int DATA_ROWS = 31;

	int[] quarters;
	int numQuarter;
	private int idxQuarter;

	String[][] data;

	FinancialParser(File file) throws Exception {
		Elements eAllColumes;

		Document doc = Jsoup.parse(file, "MS950");
		Elements eAllRows = doc.select(
		        "#SysJustIFRAMEDIV > table > tbody > tr:nth-child(2) > td:nth-child(2) > table:nth-child(2) > tbody > tr > td > table > tbody > tr");
		int numRows = eAllRows.size();

		if (numRows != MAX_ROWS)
			throw new Exception("Row number=" + numRows + " are not correct");

		Element eQuarter = eAllRows.get(1);
		eAllColumes = eQuarter.select(":root > td:nth-child(n+2)");
		numQuarter = eAllColumes.size();

		data = new String[numQuarter][DATA_ROWS];
		for (int i = 0; i < numQuarter; i++) {
			String temp;
			int iRow = 0;
			// Parse quarter number
			String title = eAllColumes.get(i).text().replaceAll("Q", "");
			temp = String.valueOf((int) ((Float.parseFloat(title) + 1911) * 10));
			Log.trace_(String.format("%s ", temp));
			data[i][iRow++] = temp;

			for (int j = 3; j < 38; j++) {
				if (j == 14 || j == 22 || j == 27 || j == 34 || j == 35)
					continue;

				String select = String.format(":root > td:nth-child(%d)", i + 2);
				Element eRow = eAllRows.get(j).select(select).first();
				temp = eRow.text().replaceAll(",", "");
				if (temp.length() == 3 && temp.compareTo("N/A") == 0)
					temp = null;
				Log.trace_(String.format("%s ", temp));
				data[i][iRow++] = temp;
			}
		}
		Log.trace_("\n");
		idxQuarter = 0;
	}

	public String[] getNextData() {
		if (idxQuarter >= numQuarter) {
			return null;
		}

		return data[idxQuarter++];
	}
}

public class ImportQuarterly {

	public static void supplementFinancial() throws Exception {
		MyDB db = new MyDB();
		Statement compST = db.conn.createStatement();
		ResultSet compResult = compST.executeQuery("SELECT Code FROM company");

		String insert = "INSERT INTO quarterly "
		        + "(Quarter, StockNum, 營業毛利率, 營業利益率, 稅前淨利率, 稅後淨利率, 每股淨值, 每股營業額, 每股營業利益, 每股稅前淨利, 股東權益報酬率, 資產報酬率, 每股稅後淨利, 營收成長率, 營業利益成長率, 稅前淨利成長率, 稅後淨利成長率, 總資產成長率, 淨值成長率, 固定資產成長率, 流動比率, 速動比率, 負債比率, 利息保障倍數, 應收帳款週轉率, 存貨週轉率, 固定資產週轉率, 總資產週轉率, 員工平均營業額, 淨值週轉率, 負債對淨值比率, 長期資金適合率) VALUES "
		        + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		MyStatement finaST = new MyStatement(db.conn, insert);

		while (compResult.next()) {
			FinancialParser financialParser;
			String code = compResult.getString(("Code"));

			File file = new File(Environment.QuarterFinancial + code + ".html");
			if (!file.exists()) {
				Log.warn("Ignore file " + file.getName());
				continue;
			}

			try {
				financialParser = new FinancialParser(file);
			} catch (Exception ex) {
				Log.warn("Ignore incorrect file " + file.getName());
				continue;
			}

			String[] data;

			Log.info_("Import stock " + code);
			while ((data = financialParser.getNextData()) != null) {
				Log.info_(" Q" + data[0] + ":");

				finaST.setInt(1, data[0]); // Quarter
				finaST.setInt(2, code); // StockNum

				int iData = 1;
				for (int j = 3; j <= 32; j++) {
					finaST.setFloat(j, data[iData++]);
				}
				
				finaST.addBatch();
				Log.info_("Done");
			}
			Log.info_("\n");
		}
		compST.close();
		finaST.close();
		db.close();
	}

	public static void main(String[] args) {

		try {
			supplementFinancial();

			Log.info("Done");
			// db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
