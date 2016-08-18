import org.apache.commons.math3.stat.regression.SimpleRegression;

public class TestLeastSquare {

	static double calculateRate(double nper, double pmt, double pv, double fv, double type, double guess) {
		// FROM MS
		// http://office.microsoft.com/en-us/excel-help/rate-HP005209232.aspx
		int FINANCIAL_MAX_ITERATIONS = 20;// Bet accuracy with 128
		double FINANCIAL_PRECISION = 0.0000001;// 1.0e-8

		double y, y0, y1, x0, x1 = 0, f = 0, i = 0;
		double rate = guess;
		if (Math.abs(rate) < FINANCIAL_PRECISION) {
			y = pv * (1 + nper * rate) + pmt * (1 + rate * type) * nper + fv;
		} else {
			f = Math.exp(nper * Math.log(1 + rate));
			y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
		}
		y0 = pv + pmt * nper + fv;
		y1 = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;

		// find root by Newton secant method
		i = x0 = 0.0;
		x1 = rate;
		while ((Math.abs(y0 - y1) > FINANCIAL_PRECISION) && (i < FINANCIAL_MAX_ITERATIONS)) {
			rate = (y1 * x0 - y0 * x1) / (y1 - y0);
			x0 = x1;
			x1 = rate;

			if (Math.abs(rate) < FINANCIAL_PRECISION) {
				y = pv * (1 + nper * rate) + pmt * (1 + rate * type) * nper + fv;
			} else {
				f = Math.exp(nper * Math.log(1 + rate));
				y = pv * f + pmt * (1 / rate + type) * (f - 1) + fv;
			}

			y0 = y1;
			y1 = y;
			++i;
		}
		return rate;
	}

	public static double rate(double nper, double pmt, double pv) {
		double error = 0.0000001;
		double high = 1.00;
		double low = 0.00;

		double rate = (2.0 * (nper * pmt - pv)) / (pv * nper);

		while (true) {
			// check for error margin
			double calc = Math.pow(1 + rate, nper);
			calc = (rate * calc) / (calc - 1.0);
			calc -= pmt / pv;

			if (calc > error) {
				// guess too high, lower the guess
				high = rate;
				rate = (high + low) / 2;
			} else if (calc < -error) {
				// guess too low, higher the guess
				low = rate;
				rate = (high + low) / 2;
			} else {
				// acceptable guess
				break;
			}
		}

		System.out.println("Rate : " + rate);
		return rate;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SimpleRegression sr = new SimpleRegression();
		sr.addData(1, 6);
		sr.addData(2, 5);
		sr.addData(3, 7);
		sr.addData(4, 10);

		// y = intercept + slope * x
		Log.info("Slop: " + sr.getSlope());
		//Log.info("Intercept: " + sr.getIntercept());

		Log.info("" + calculateRate(10,0, -1.18, 3.7, 0, 0.1) * 100 + " %");
	}

}
