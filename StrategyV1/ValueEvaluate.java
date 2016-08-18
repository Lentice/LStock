import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ValueEvaluate {
	static MyDB db;
	static final int LOW = 0;
	static final int MEDIUM = 1;
	static final int HIGH = 2;
	
	static Writer writer;

	Company company;
	float[] 本益比估計;
	float[] 股利估計;
	int 本益比分數;
	int 股利分數;

	ValueEvaluate(Company company) {
		this.company = company;
	}

	boolean check每年配息(int year) throws Exception {
		int yearCount = company.yData.length;
		if (yearCount < year)
			return false;

		for (int i = 0; i < year; i++) {
			float v1 = company.yData[yearCount - 1 - i].現金股利;
			float v2 = company.yData[yearCount - 1 - i].盈餘配股;
			if (v1 + v2 < 0)
				return false;
		}

		return true;
	}

	static float[] 平均股利法(Company company) {
		final float YEAR = 5;

		float sum = 0;
		int yearCount = company.yData.length;
		for (int i = 0; i < 5; i++) {
			sum += company.yData[yearCount - 1 - i].現金股利;
			sum += company.yData[yearCount - 1 - i].盈餘配股;
		}
		float avg = sum / YEAR;
		float[] price = { avg * 15, avg * 20, avg * 30 };

		return price;
	}

	void 本益比估價法(Company company) throws Exception {

	}

	float get股利分數() throws Exception {
		final int year = 6;
		股利估計 = getEvaFromAnnual("(IFNULL(現金股利, 0) + IFNULL(盈餘配股, 0))", year, false);
		if (!check每年配息(year)) {
			股利分數 = -100;
			return -100;
		}

		float gapLow = (股利估計[MEDIUM] * 15) / 100;
		float gapHigh = (股利估計[MEDIUM] * 80) / 100;

		float 預估均價 = 股利估計[MEDIUM] * 30;
		if (company.現價 > 預估均價)
			股利分數 = (int) ((預估均價 - company.現價) / gapHigh);
		else
			股利分數 = (int) ((預估均價 - company.現價) / gapLow);

		股利分數 = Math.min(Math.max(股利分數, -100), 100);
		return 股利分數;
	}

	int get本益比分數() throws Exception {
		本益比估計 = getEvaFromDaily("本益比", 5, true);
		本益比估計[LOW] = Math.max(本益比估計[LOW], 5);
		本益比估計[HIGH] = Math.min(本益比估計[HIGH], 32);

		float gapLow = (本益比估計[MEDIUM] - 本益比估計[LOW]) / 100;
		float gapHigh = (本益比估計[HIGH] - 本益比估計[MEDIUM]) / 100;

		if (company.當前本益比 > 本益比估計[MEDIUM])
			本益比分數 = (int) ((本益比估計[MEDIUM] - company.當前本益比) / gapHigh);
		else
			本益比分數 = (int) ((本益比估計[MEDIUM] - company.當前本益比) / gapLow);

		本益比分數 = Math.min(Math.max(本益比分數, -100), 100);

		return 本益比分數;
	}

	float[] getEvaFromAnnual(String colName, int year, boolean noneZero) throws Exception {
		String dateLimit = "(Year + " + year + ") > (SELECT Max(Year) from annual WHERE StockNum = " + company.stockNum
				+ ")";

		return getLowMidHigh(colName, "annual", dateLimit, noneZero);
	}

	float[] getEvaFromDaily(String colName, int year, boolean noneZero) throws Exception {
		String dateLimit = "Date > DATE_ADD(CURDATE(), INTERVAL " + (0 - year) + " YEAR)";

		return getLowMidHigh(colName, "daily", dateLimit, noneZero);
	}

	float[] getLowMidHigh(String colName, String table, String dateLimit, boolean noneZero) throws Exception {
		ResultSet rs;
		Statement stmt = db.conn.createStatement();
		float[] val = { 0, 0, 0 };

		final String noneZeroLimit = noneZero ? "" : " AND " + colName + " > 0";

		String select;
		final String from = " FROM " + table;
		final String where = " WHERE StockNum = " + company.stockNum + " AND " + dateLimit + noneZeroLimit;

		select = "SELECT COUNT(" + colName + ") AS val ";
		rs = stmt.executeQuery(select + from + where);
		if (!rs.first())
			return val;
		int count = rs.getInt("val");

		// 最大值
		select = "SELECT MAX(" + colName + ") AS val ";
		rs = stmt.executeQuery(select + from + where);
		rs.first();
		val[2] = rs.getFloat("val");

		// 最小值
		select = "SELECT MIN(" + colName + ") AS val ";
		rs = stmt.executeQuery(select + from + where);
		rs.first();
		val[0] = rs.getFloat("val");

		// 中位數
		select = "SELECT " + colName + " AS val ";
		final String orderBy = " ORDER BY val LIMIT " + count / 2 + ",1";
		// Log.dbg_(select + from + where + orderBy + "\n");
		rs = stmt.executeQuery(select + from + where + orderBy);
		rs.first();
		val[1] = rs.getFloat("val");

		stmt.close();

		return val;
	}

	float get近四季ROE() {
		int qtCount = company.qData.length;
		if (qtCount < 4) // 因使用近四季
			return 0;

		Float 近四季ROE = company.qData[qtCount - 1].近四季ROE;
		if (近四季ROE == null)
			return 0;
		
		return 近四季ROE.floatValue() * 100;
	}
	
	static void writeInfo(String str) throws Exception {
		
		Log.info_(str);
		writer.write(str.replaceAll("\t", ","));
	}

	static void evaluate(MyDB db) throws Exception {
		List<Company> goodCompList = GoodCompanyV2.GetGoodCompanies(db);
		List<ValueEvaluate> evaCompList = new ArrayList<>();
		
		writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("ValueEvaluate.csv"), "MS950"));

		writeInfo("\n合格公司:\n");
		writeInfo("代號\t名稱\t本益比分數\t當前本益比\t本益比(便宜)\t本益比(合理)\t本益比(昂貴)\t股利分數\t還原股利\t近四季ROE\t權重分數\n");

		for (Company company : goodCompList) {
			ValueEvaluate evaComp = new ValueEvaluate(company);

			// Log.info_(company.stockNum + "\n");
			evaComp.get本益比分數();
			evaComp.get股利分數();
			writeInfo(company.stockNum + "\t" + company.name + "\t");
			writeInfo(evaComp.本益比分數 + "\t" + company.getLatestPER() + "\t");
			writeInfo(evaComp.本益比估計[0] + "\t" + evaComp.本益比估計[1] + "\t" + evaComp.本益比估計[2] + "\t");
			writeInfo(String.format("%d\t%d%%", evaComp.股利分數, (int) (evaComp.股利估計[1] * 100 / company.現價)) + "\t");
			writeInfo(String.format("%.2f%%\t", evaComp.get近四季ROE()));
			writeInfo(String.format("%.2f\t", evaComp.本益比分數 + evaComp.股利分數 * 0.5 + evaComp.get近四季ROE()));
			writeInfo("\n");

			evaCompList.add(evaComp);
		}

		writer.close();
	}

	public static void main(String[] args) {
		try {
			db = new MyDB();
			evaluate(db);
			db.close();
			Log.info_("Done!!");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
