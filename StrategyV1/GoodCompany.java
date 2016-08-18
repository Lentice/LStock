import java.util.ArrayList;
import java.util.List;

public class GoodCompany {
	MyDB db;
	static float[] roe = new float[20];
	static float[] roeNear4Q = new float[20];
	static float[] 營業利益率 = new float[20];

	public GoodCompany() {
		// TODO Auto-generated constructor stub
	}

	static boolean checkROE(Company company) throws Exception {

		int qtCount = company.qData.length;

		// 起碼五年，共20季
		if (qtCount < 20)
			return false;

		for (int i = 0; i < 20; i++) {
			if (company.qData[qtCount - 1 - i].ROE == null)
				return false;
			roe[i] = company.qData[qtCount - 1 - i].ROE;
		}

		for (int i = 0; i < 4; i++) {
			roeNear4Q[i] = roe[i] + roe[i + 1] + roe[i + 2] + roe[i + 3];
			
			//Log.dbg_(String.format("%.2f ", roeNear4Q[i] * 100));
			if (roeNear4Q[i] < 0.08)
				return false;
		}
		
		for (int i = 4; i < 8; i++) {
			roeNear4Q[i] = roe[i] + roe[i + 1] + roe[i + 2] + roe[i + 3];
			
			//Log.dbg_(String.format("%.2f ", roeNear4Q[i] * 100));
			if (roeNear4Q[i] < 0.075)
				return false;
		}
		
		for (int i = 8; i < 12; i++) {
			roeNear4Q[i] = roe[i] + roe[i + 1] + roe[i + 2] + roe[i + 3];
			
			//Log.dbg_(String.format("%.2f ", roeNear4Q[i]*100));
			if (roeNear4Q[i] < 0.060)
				return false;
		}
		
		for (int i = 12; i < 20 - 3; i++) {
			roeNear4Q[i] = roe[i] + roe[i + 1] + roe[i + 2] + roe[i + 3];
			
			//Log.dbg_(String.format("%.2f ", roeNear4Q[i]*100));
			if (roeNear4Q[i] < 0.050)
				return false;
		}
		
		return true;
	}

	static boolean check營業利益率(Company company) throws Exception {

		int yearCount = company.yData.length;

		if (yearCount < 5)
			return false;

		for (int i = 0; i < 5; i++) {
			營業利益率[i] = company.yData[yearCount - 1 - i].營業利益率;
			if (營業利益率[i] < 0)
				return false;
		}

		return true;
	}
	
	static boolean check自由現金流(Company company) throws Exception {

		int yearCount = company.yData.length;
		float sum = 0;

		if (yearCount < 5)
			return false;

		// TODO: 目前只有兩年自由現金流資料，目標是五年
		for (int i = 0; i < 2; i++) {
			sum += company.yData[yearCount - 1 - i].自由現金流;
		}
		
		if (sum < 0)
			return false;

		return true;
	}
	
	static boolean check每股自由現金流(Company company) throws Exception {

		int yearCount = company.yData.length;
		float 累計自由現金流 = 0;
		float 累計現金股利 = 0;

		if (yearCount < 5)
			return false;

		// TODO: 目前只有兩年自由現金流資料，目標是五年
		for (int i = 0; i < 2; i++) {
			累計現金股利 += company.yData[yearCount - 1 - i].現金股利;
		}

		for (int i = 0; i < 2; i++) {
			累計自由現金流 += company.yData[yearCount - 1 - i].自由現金流;
		}
		
		if (累計現金股利 / 累計自由現金流 > 2)
			return false;

		return true;
	}
	
	static boolean check營業現金流對淨利比(Company company) throws Exception {

		//  
		int qtCount = company.qData.length;

		// 共12季
		if (qtCount < 12)
			return false;

		// TODO: 目標 12季
		final int maxQt = 11;
		
		long 累計營業現金流 = 0;
		for (int i = 0; i < maxQt; i++) {
			累計營業現金流 += company.qData[qtCount - 1 - i].營業現金流;
		}
		
		long 累計稅後淨利 = 0;
		for (int i = 0; i < maxQt; i++) {
			累計稅後淨利 += company.qData[qtCount - 1 - i].稅後淨利;
		}
		
		if ((float)累計營業現金流 / 累計稅後淨利 < 0.5)
			return false;
		
		return true;
	}
	
	static boolean check利息保障倍數(Company company) throws Exception {
		if (company.isFinancial())
			return true;
		
		int qtCount = company.qData.length;
		if (qtCount < 8)
			return false;

		for (int i = 0; i < 8; i++) {
			if (company.qData[qtCount - 1 - i].近四季利息保障倍數 == null)
				continue;

			if (company.qData[qtCount - 1 - i].近四季利息保障倍數 < 6)
				return false;
		}

		return true;
	}
	
	static boolean check負債比(Company company) throws Exception {
		if (company.isFinancial())
			return true;
		
		int qtCount = company.qData.length;
		if (qtCount < 1)
			return false;

		if (company.qData[qtCount - 1].負債比 == null)
			return true;
		
		if (company.qData[qtCount - 1].負債比 > 0.75)
			return false;

		return true;
	}
	
	static boolean check董監持股比例(Company company) throws Exception {

		// TODO:
		return true;
	}
	
	static boolean check董監持股質押比例(Company company) throws Exception {

		// TODO:
		return true;
	}
	
	static boolean check速動比(Company company) throws Exception {

		int yearCount = company.yData.length;
		if (yearCount < 3)
			return false;

		for (int i = 0; i < 3; i++) {
			if (company.yData[yearCount - 1 - i].速動比 < 0.9)
				return false;
		}

		return true;
	}
	
	static boolean check每年配息(Company company) throws Exception {

		int yearCount = company.yData.length;
		if (yearCount < 3)
			return false;

		for (int i = 0; i < 3; i++) {
			float v1 = company.yData[yearCount - 1 - i].現金股利;
			float v2 = company.yData[yearCount - 1 - i].盈餘配股;
			if (v1 + v2 < 0)
				return false;
		}

		return true;
	}

	static boolean isGoodCompany(Company company) throws Exception {

		
		if (!checkROE(company))
			return false;

		if (!check營業利益率(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  營業利益: 不合格\n");
			return false;
		}
		
		if (!check自由現金流(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  自由現金流: 不合格\n");
			return false;
		}
		
		if (!check每股自由現金流(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  每股自由現金流: 不合格\n");
			return false;
		}
		
		if (!check營業現金流對淨利比(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  營業現金流對淨利比: 不合格\n");
			return false;
		}
		
		if (!check利息保障倍數(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  利息保障倍數: 不合格\n");
			return false;
		}
		
		if (!check負債比(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  負債比: 不合格\n");
			return false;
		}
		
		if (!check董監持股比例(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  董監持股比例: 不合格\n");
			return false;
		}
		
		if (!check董監持股質押比例(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  董監持股質押比例: 不合格\n");
			return false;
		}
		
		if (!check速動比(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  速動比: 不合格\n");
			return false;
		}
		
		if (!check每年配息(company)) {
			Log.dbg_(company.stockNum + " " + company.name + "  每年配息: 不合格\n");
			return false;
		}
		
		//Log.dbg_("\n");
		//Log.info_(company.stockNum + " " + company.name + "\n");
		
		
		//Log.dbg(String.format("ROE: %3.2f %3.2f %3.2f %3.2f %3.2f", roe[0], roe[1], roe[2], roe[3],
		//		roe[4]));

		return true;
	}

	static void GetGoodCompanies(MyDB db) throws Exception {
		Company[] companies = Company.getAllValidCompanies(db);

		List<Company> goodCompList = new ArrayList<>();

		for (Company company : companies) {

			company.fetchAllBasicData();
			if (company.mData == null || company.qData == null)
				continue;

			if (isGoodCompany(company))
				goodCompList.add(company);
		}
		
		Log.info_("\n合格公司:\n");
		for (Company company : goodCompList) {
			Log.info_(company.stockNum + " " + company.name + "\n");
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			MyDB db = new MyDB();
			GetGoodCompanies(db);
			db.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
