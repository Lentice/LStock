import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.regression.SimpleRegression;

public class GoodCompanyV2 {
	static MyDB db;

	public Company company;
	public int stockNum;
	public String name;

	public GoodCompanyV2(Company company) throws SQLException {
		this.company = company;
		this.stockNum = company.stockNum;
		this.name = company.name;

	}

	boolean checkROE() throws Exception {
		final float 最低ROE成長率 = (float) 0.00;	// 0%
		final float 最低近四季ROE = (float) 0.05;	// 5%
		final int maxQt = 8; // 8季

		int qtCount = company.qData.length;
		if (qtCount < maxQt + 3) // 因使用近四季，故需額外3季
			return false;

		int idx = 0;
		SimpleRegression sr = new SimpleRegression();
		for (int i = maxQt - 1; i >= 0 ; i--) {
			Float 近四季ROE = company.qData[qtCount - 1 - i].近四季ROE;
			if (近四季ROE == null || 近四季ROE < 最低近四季ROE)
				return false;

			sr.addData(++idx, 近四季ROE);
		}

		if (sr.getSlope() < 最低ROE成長率)
			return false;

		return true;
	}

	boolean check營業利益率() throws Exception {
		final float 最低營業利益率成長率 = (float) -0.10;	// -10%
		final float 最低近四季營業利益率 = (float) 0.05;	//  5%
		final int maxQt = 8; // 8季

		int qtCount = company.qData.length;
		if (qtCount < maxQt + 3) // 因使用近四季，故需額外3季
			return false;

		int idx = 0;
		SimpleRegression sr = new SimpleRegression();
		for (int i = maxQt - 1; i >= 0; i--) {
			Float 近四季營業利益率 = company.qData[qtCount - 1 - i].近四季營業利益率;
			if (近四季營業利益率 == null || 近四季營業利益率 < 最低近四季營業利益率)
				return false;

			sr.addData(++idx, 近四季營業利益率);
		}

		if (sr.getSlope() < 最低營業利益率成長率)
			return false;

		return true;
	}

	boolean check自由現金流() throws Exception {
		final long 最低累計自由現金流 = 0;
		final int maxQt = 11; // TODO: 目前只有11季年自由現金流資料，目標是五年

		int qtCount = company.qData.length;
		if (qtCount < maxQt)
			return false;

		long 累計自由現金流 = 0;
		for (int i = 0; i < maxQt; i++) {
			累計自由現金流 += company.qData[qtCount - 1 - i].自由現金流;
		}

		if (累計自由現金流 < 最低累計自由現金流)
			return false;

		return true;
	}

	boolean check每股自由現金流() throws Exception {
		final float 最高現金股利自由現金流比 = (float) 2.0;

		// TODO: 目前只有兩年自由現金流資料，目標是五年
		final int maxYear = 2;

		int yearCount = company.yData.length;
		float 累計自由現金流 = 0;
		float 累計現金股利 = 0;

		if (yearCount < maxYear)
			return false;

		for (int i = 0; i < maxYear; i++) {
			累計現金股利 += company.yData[yearCount - 1 - i].現金股利;
		}

		for (int i = 0; i < maxYear; i++) {
			累計自由現金流 += company.yData[yearCount - 1 - i].自由現金流;
		}

		if (累計現金股利 / 累計自由現金流 > 最高現金股利自由現金流比)
			return false;

		return true;
	}

	boolean check營業現金流對淨利比() throws Exception {
		final float 最低營業現金流對淨利比 = (float) 0.5;	// 50%, 即 盈餘品質
		// TODO: 目標 12季
		final int maxQt = 8;

		int qtCount = company.qData.length;
		if (qtCount < maxQt + 3) // 因使用近四季，故需額外3季
			return false;

		for (int i = 0; i < maxQt; i++) {
			long 近四季營業現金流 = company.qData[qtCount - 1 - i].近四季營業現金流;
			long 近四季稅後淨利 = company.qData[qtCount - 1 - i].近四季稅後淨利;
			if ((float) 近四季營業現金流 / 近四季稅後淨利 < 最低營業現金流對淨利比)
				return false;
		}

		return true;
	}

	boolean check利息保障倍數() throws Exception {
		final int 最低利息保障倍數 = 6;
		final int maxQt = 8;		// 8季

		if (company.isFinancial())
			return true;

		int qtCount = company.qData.length;
		if (qtCount < maxQt + 3)	// 因使用近四季，故需額外3季
			return false;

		for (int i = 0; i < maxQt; i++) {
			Float 近四季利息保障倍數 = company.qData[qtCount - 1 - i].近四季利息保障倍數;
			if (近四季利息保障倍數 == null)
				continue;

			if (近四季利息保障倍數 < 最低利息保障倍數)
				return false;
		}

		return true;
	}

	boolean check負債比() throws Exception {
		final float 最高負債比 = (float) 0.65;	// 65%

		if (company.isFinancial())
			return true;

		int qtCount = company.qData.length;
		if (qtCount < 1)
			return false;

		if (company.qData[qtCount - 1].負債比 == null)
			return true;

		if (company.qData[qtCount - 1].負債比 > 最高負債比)
			return false;

		return true;
	}

	boolean check董監持股比例() throws Exception {

		// TODO:
		return true;
	}

	boolean check董監持股質押比例() throws Exception {

		// TODO:
		return true;
	}

	boolean check速動比() throws Exception {
		final float 最低速動比 = (float) 0.9;
		final int maxYear = 3;
		
		int yearCount = company.yData.length;
		if (yearCount < maxYear)
			return false;

		for (int i = 0; i < maxYear; i++) {
			if (company.yData[yearCount - 1 - i].速動比 < 最低速動比)
				return false;
		}

		return true;
	}

	boolean check每年配息() throws Exception {
		final int maxYear = 8;

		int yearCount = company.yData.length;
		if (yearCount < maxYear)
			return false;

		// 近三年配息
		for (int i = 0; i < 3; i++) {
			float v1 = company.yData[yearCount - 1 - i].現金股利;
			float v2 = company.yData[yearCount - 1 - i].盈餘配股;
			if (v1 + v2 <= 0)
				return false;
		}
		
		// 近八年配息6次以上
		int count = 0;
		for (int i = 0; i < 8; i++) {
			float v1 = company.yData[yearCount - 1 - i].現金股利;
			float v2 = company.yData[yearCount - 1 - i].盈餘配股;
			if (v1 + v2 > 0)
				count++;
		}
		if (count < 6)
			return false;

		return true;
	}

	boolean check本益比() {
		if (company.當前本益比 <= 0 || company.當前本益比 > 32)
			return false;

		return true;
	}

	boolean isGoodCompany() throws Exception {

		if (company.stockNum == 2886)
			Log.info("");

		if (!checkROE())
			return false;

		if (!check營業利益率()) {
			Log.dbg_(company.stockNum + " " + company.name + "  營業利益: 不合格\n");
			return false;
		}

		if (!check自由現金流()) {
			Log.dbg_(company.stockNum + " " + company.name + " 自由現金流: 不合格\n");
			return false;
		}

		if (!check每股自由現金流()) {
			Log.dbg_(company.stockNum + " " + company.name + " 每股自由現金流: 不合格\n");
			return false;
		}

		if (!check營業現金流對淨利比()) {
			Log.dbg_(company.stockNum + " " + company.name + "  營業現金流對淨利比: 不合格\n");
			return false;
		}

		if (!check利息保障倍數()) {
			Log.dbg_(company.stockNum + " " + company.name + "  利息保障倍數: 不合格\n");
			return false;
		}

		if (!check負債比()) {
			Log.dbg_(company.stockNum + " " + company.name + " 負債比: 不合格\n");
			return false;
		}

		// if (!check董監持股比例(company)) {
		// Log.dbg_(company.stockNum + " " + company.name + " 董監持股比例: 不合格\n");
		// return false;
		// }
		//
		// if (!check董監持股質押比例(company)) {
		// Log.dbg_(company.stockNum + " " + company.name + " 董監持股質押比例: 不合格\n");
		// return false;
		// }
		//
		// if (!check速動比(company)) {
		// Log.dbg_(company.stockNum + " " + company.name + " 速動比: 不合格\n");
		// return false;
		// }
		//
		if (!check每年配息()) {
			Log.dbg_(company.stockNum + " " + company.name + "  每年配息: 不合格\n");
			return false;
		}

		if (!check本益比()) {
			Log.dbg_(company.stockNum + " " + company.name + " 本益比: 不合格\n");
			return false;
		}

		// Log.dbg_("\n");
		// Log.info_(company.stockNum + "\t" + company.name + "\t" + score +
		// "\n");

		return true;
	}

	static List<Company> GetGoodCompanies(MyDB myDB) throws Exception {
		db = myDB;
		Company[] companies = Company.getAllValidCompanies(db);

		List<Company> goodCompList = new ArrayList<>();

		for (Company company : companies) {
			company.fetchAllBasicData();
			if (company.mData == null || company.qData == null)
				continue;

			GoodCompanyV2 goodCmp = new GoodCompanyV2(company);
			if (goodCmp.isGoodCompany())
				goodCompList.add(goodCmp.company);
		}

		return goodCompList;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
//			MyDB db = new MyDB();
//			List<Company> goodCompList = GoodCompanyV2.GetGoodCompanies(db);
			// List<Company> goodCompList = GoodCompanyV2.GetGoodCompanies(db);
			// Log.info_("\n合格公司:\n");
			// Log.info_("代號\t名稱\t本益比分數\t當前本益比\t正常本益比\t\n");
//			for (Company goodComp : goodCompList) {
//				 int score = goodComp.get本益比分數();
//				 float 當前本益比 = goodComp.company.當前本益比;
//				 Log.info_(goodComp.stockNum + "\t" + goodComp.name + "\t" +
//				 score
//				 + "\t" + 當前本益比 + "\t"
//				 + goodComp.本益比估計[1] + "\n");
//			}

//			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
