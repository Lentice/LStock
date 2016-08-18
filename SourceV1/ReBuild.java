import java.util.Calendar;

public class ReBuild {

	public ReBuild() {
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) {
		MyDB db = null;

		try {
			Calendar cal = Calendar.getInstance();
			int currentMonth = cal.get(Calendar.MONTH) + 1;
			
			db = new MyDB();
			DailyTradeStocks.removeLatestFile();
			DailyTradeStocks.supplementDB(db);

			DailyTaiEx.removeLatestFile();
			DailyTaiEx.supplementDB(db);
			
			MonthlyRevenue.removeLatestFile();
			MonthlyRevenue.supplementDB(db);
			
			ImportETF.importAllToDB(db);

			int yearQuarter = db.getLastQuarterlyRevenue();
			int year = yearQuarter / 100;
			int quarter = yearQuarter % 100;
			ImportQuarterly.supplementBasicData(db, year, quarter);
			QuarterlyFixAndSupplement.calculate(db);
			
			year = db.getLastAnnualRevenue();
			ImportAnnual.supplementBasicData(db, year);

			if (currentMonth >= 4 && currentMonth <= 10) {
				Dividend.removeLatestFile();
			}
			Dividend.supplementDB(db, year + 1);
			
			AnnualSupplement.calculate(db);

			db.close();
			Log.info("Done!!");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
