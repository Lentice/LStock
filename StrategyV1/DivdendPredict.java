import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

class DidendInfo {
	public static Comparator<DidendInfo> comparator = new Comparator<DidendInfo>() {
		@Override
		public int compare(DidendInfo o1, DidendInfo o2) {
			return (int) (((DidendInfo) o2).今年殖利率 * 10000 - ((DidendInfo) o1).今年殖利率 * 10000);
		}
	};

	QuarterlyData 去年Q1;
	QuarterlyData 去年Q2;
	QuarterlyData 去年Q3;
	QuarterlyData 去年Q4;
	AnnualData 去年;

	QuarterlyData 今年Q1;
	QuarterlyData 今年Q2;
	QuarterlyData 今年Q3;
	float 今年Q4EPS;

	float 今年EPS;

	float 去年配股配息;
	float 今年配股配息;

	float 去年殖利率;
	float 今年殖利率;

	float 現價;
	float 去年除權息參考價;

	Company company;
	int year;

	DidendInfo(Company company, int year) {
		this.company = company;
		this.year = year;
	}

	Boolean getInfo() throws Exception {
		company.fetchAllBasicData();
		if (company.mData == null || company.qData == null)
			return false;

		Log.trace(company.code + "  " + company.name);

		去年Q1 = company.getQData(year - 1, 1);
		去年Q2 = company.getQData(year - 1, 2);
		去年Q3 = company.getQData(year - 1, 3);
		去年Q4 = company.getQData(year - 1, 4);
		if (去年Q1 == null || 去年Q2 == null || 去年Q3 == null || 去年Q4 == null)
			return false;

		今年Q1 = company.getQData(year, 1);
		今年Q2 = company.getQData(year, 2);
		今年Q3 = company.getQData(year, 3);
		if (今年Q1 == null || 今年Q2 == null || 今年Q3 == null)
			return false;

		MonthlyData 去年09月 = company.getMData(year - 1, 9);
		MonthlyData 去年10月 = company.getMData(year - 1, 10);
		MonthlyData 去年11月 = company.getMData(year - 1, 11);
		MonthlyData 去年12月 = company.getMData(year - 1, 12);
		MonthlyData 今年09月 = company.getMData(year, 9);
		MonthlyData 今年10月 = company.getMData(year, 10);
		MonthlyData 今年11月 = company.getMData(year, 11);
		MonthlyData 今年12月 = company.getMData(year, 12);
		if (今年09月 == null || 今年10月 == null || 去年09月 == null || 去年10月 == null || 去年11月 == null || 去年12月 == null)
			return false;

		if (今年09月.當月營收 == null || 今年10月.當月營收 == null || 去年09月.當月營收 == null || 去年10月.當月營收 == null || 去年11月.當月營收 == null
		        || 去年12月.當月營收 == null)
			return false;

		去年 = company.getYData(year - 1);
		if (去年 == null || 去年.EPS == 0 || 去年.現金股利 == 0)
			return false;

		// DailyData dData =
		// company.getDData(Date.valueOf(String.format("%d-%d-%d", 2014, 11,
		// 19)));
		// if (dData == null)
		// return false;
		// company.latestPrice = dData.收盤價;

		long 今年11月營收;
		if (今年11月 != null)
			今年11月營收 = 今年11月.當月營收;
		else
			今年11月營收 = Math.min(今年09月.當月營收,
			        Math.min(今年10月.當月營收, Math.min(去年09月.當月營收, Math.min(去年10月.當月營收, Math.min(去年11月.當月營收, 去年12月.當月營收)))));

		long 今年12月營收;
		if (今年12月 != null)
			今年12月營收 = 今年12月.當月營收;
		else
			今年12月營收 = Math.min(今年09月.當月營收,
			        Math.min(今年10月.當月營收, Math.min(去年10月.當月營收, Math.min(去年11月.當月營收, 去年12月.當月營收))));

		今年Q4EPS = (今年10月.當月營收 + 今年11月營收 + 今年12月營收) * Math.min(今年Q3.EPS / 今年Q3.營收, 去年Q4.EPS / 去年Q4.營收);

		今年EPS = 今年Q4EPS + 今年Q3.EPS + 今年Q2.EPS + 今年Q1.EPS;

		去年配股配息 = (去年.現金股利 + 去年.盈餘配股 + 去年.資本公積);
		今年配股配息 = 今年EPS * (去年配股配息 / 去年.EPS) * ((float) 去年.股本 / 今年Q3.股本);
		今年殖利率 = 今年配股配息 / company.現價;
		Log.info(String.format("今年殖利率  %.2f %%  今年EPS %.2f 去年EPS %.2f", 今年殖利率 * 100, 今年EPS, 去年.EPS));

		if (去年.除權除息參考價 > 0)
			去年殖利率 = 去年配股配息 / 去年.除權除息參考價;
		else
			去年殖利率 = -1;

		return true;
	}

	boolean fieldsLessThan(double limit, Float... values) {
		for (Float value : values) {
			if (value < limit)
				return true;
		}
		return false;
	}

	boolean fieldsGreaterThan(double limit, Float... values) {
		for (Float value : values) {
			if (value > limit)
				return true;
		}

		return false;
	}

	boolean analyze() {

		boolean ret = true;

		DivdendPredict.cond[0]++;
		if (fieldsLessThan(0, 今年Q4EPS, 今年Q3.EPS, 今年Q2.EPS, 今年Q1.EPS)) {
			Log.trace(String.format("1. %.2f %.2f %.2f %.2f", 今年Q4EPS, 今年Q3.EPS, 今年Q2.EPS, 今年Q1.EPS));
			DivdendPredict.cond[1]++;
			ret = false;
		}

		if (今年殖利率 < 0.04) {
			DivdendPredict.cond[2]++;
			Log.trace(String.format("2. 今年殖利率 %.2f < 4%%", 今年殖利率));
			ret = false;
		}

		if (今年EPS / 去年.EPS < 0.90) {
			DivdendPredict.cond[3]++;
			Log.trace(String.format("3. 今年EPS %.2f / 去年EPS %.2f < 9%%", 今年EPS, 去年.EPS));
			ret = false;
		}

		if (今年Q4EPS / 去年Q4.EPS < 0.85) {
			DivdendPredict.cond[4]++;
			Log.trace(String.format("4. 今年Q4EPS %.2f / 去年Q4EPS %.2f < 85%%", 今年Q4EPS, 去年Q4.EPS));
			ret = false;
		}

		if (今年Q4EPS / 今年Q3.EPS < 0.80) {
			DivdendPredict.cond[5]++;
			Log.trace(String.format("5. 今年Q4EPS %.2f / 今年Q3.EPS %.2f < 80%%", 今年Q4EPS, 今年Q3.EPS));
			ret = false;
		}

		if (fieldsLessThan(-0.3, 今年Q1.單季營業利益年增率, 今年Q1.單季稅後淨利年增率, 今年Q1.單季總資產年增率, 今年Q1.單季淨值年增率, 今年Q1.單季固定資產年增率)) {
			DivdendPredict.cond[6]++;
			Log.trace(String.format("6. %.2f %.2f %.2f %.2f %.2f", 今年Q1.單季營業利益年增率, 今年Q1.單季稅後淨利年增率, 今年Q1.單季總資產年增率,
			        今年Q1.單季淨值年增率, 今年Q1.單季固定資產年增率));
			ret = false;
		}

		if (fieldsLessThan(-0.25, 今年Q2.單季營業利益年增率, 今年Q2.單季稅後淨利年增率, 今年Q2.單季總資產年增率, 今年Q2.單季淨值年增率, 今年Q2.單季固定資產年增率)) {
			DivdendPredict.cond[7]++;
			Log.trace(String.format("7. %.2f %.2f %.2f %.2f %.2f", 今年Q2.單季營業利益年增率, 今年Q2.單季稅後淨利年增率, 今年Q2.單季總資產年增率,
			        今年Q2.單季淨值年增率, 今年Q2.單季固定資產年增率));
			ret = false;
		}

		if (fieldsLessThan(-0.2, 今年Q3.單季營業利益年增率, 今年Q3.單季稅後淨利年增率, 今年Q3.單季總資產年增率, 今年Q3.單季淨值年增率, 今年Q3.單季固定資產年增率)) {
			DivdendPredict.cond[8] += 1;
			Log.trace(String.format("8. %.2f %.2f %.2f %.2f %.2f", 今年Q3.單季營業利益年增率, 今年Q3.單季稅後淨利年增率, 今年Q3.單季總資產年增率,
			        今年Q3.單季淨值年增率, 今年Q3.單季固定資產年增率));
			ret = false;
		}

		if (company.stockNum == 2324)
			ret = true;

		return ret;
	}

	public String toString() {
		String cp = String.format(" , %s, %s, %s\n", company.code, company.name, company.category);
		String caption = " , , , Q1 EPS, Q2 EPS, Q3 EPS, Q4 EPS, 年度EPS, 股利+股息, 殖利率(%), 參考價\n";
		String thisY = String.format(" , , %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f%%, %.2f\n", year, 今年Q1.EPS,
		        今年Q2.EPS, 今年Q3.EPS, 今年Q4EPS, 今年EPS, 今年配股配息, 今年殖利率 * 100, company.現價);

		String lastY = String.format(" , , %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f%%, %.2f\n", year - 1, 去年Q1.EPS,
		        去年Q2.EPS, 去年Q3.EPS, 去年Q4.EPS, 去年.EPS, 去年配股配息, 去年殖利率 * 100, 去年.除權除息參考價);

		return cp + caption + thisY + lastY;
	}

	public String otherField() {
		String caption = String.format(" , , , 營收成長率, 營業利益成長率, 稅後淨利成長率, 總資產成長率, 淨值成長率, 固定資產成長率, EPS成長率, ROE\n");
		String q3 = String.format(" , , Q3, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n", 今年Q3.單季營收年增率,
		        今年Q3.單季營業利益年增率, 今年Q3.單季稅後淨利年增率, 今年Q3.單季總資產年增率, 今年Q3.單季淨值年增率, 今年Q3.單季淨值年增率, 今年Q3.單季固定資產年增率,
		        今年Q3.單季EPS年增率, 今年Q3.ROE);
		String q2 = String.format(" , , Q2, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n", 今年Q2.單季營收年增率,
		        今年Q2.單季營業利益年增率, 今年Q2.單季稅後淨利年增率, 今年Q2.單季總資產年增率, 今年Q2.單季淨值年增率, 今年Q2.單季淨值年增率, 今年Q2.單季固定資產年增率,
		        今年Q2.單季EPS年增率, 今年Q2.ROE);
		String q1 = String.format(" , , Q1, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n", 今年Q1.單季營收年增率,
		        今年Q1.單季營業利益年增率, 今年Q1.單季稅後淨利年增率, 今年Q1.單季總資產年增率, 今年Q1.單季淨值年增率, 今年Q1.單季淨值年增率, 今年Q1.單季固定資產年增率,
		        今年Q1.單季EPS年增率, 今年Q1.ROE);

		return caption + q3 + q2 + q1;
	}
}

public class DivdendPredict {

	static List<DidendInfo> divInfoList = new ArrayList<>();
	public static int cond[] = new int[20];

	public DivdendPredict() {
		// TODO Auto-generated constructor stub
	}

	static void writeToCsv() throws Exception {
		Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("DivdendPredict.csv"), "MS950"));

		for (DidendInfo divInfo : divInfoList) {
			writer.write(divInfo.toString());
			writer.write(divInfo.otherField() + "\n");
			Log.info_(divInfo.toString() + "\n");
		}
		writer.close();
		Log.info_("\n");
	}

	static void analyze(MyDB db, int year) throws Exception {
		Company[] companies = Company.getAllValidCompanies(db);

		Arrays.fill(cond, 0);

		for (Company company : companies) {

			DidendInfo info = new DidendInfo(company, year);
			if (!info.getInfo())
				continue;

			if (!info.analyze())
				continue;

			divInfoList.add(info);
		}

		divInfoList.sort(DidendInfo.comparator);

		writeToCsv();

		for (int i = 1; i < 20; i++) {
			Log.info(String.format("%d \t%.0f %%", i, (float) cond[i] / cond[0] * 100));
		}
	}

	public static void main(String[] args) {
		try {
			MyDB db = new MyDB();
			analyze(db, 2015);
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
