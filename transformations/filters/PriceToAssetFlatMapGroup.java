package bigdata.transformations.filters;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapGroupsFunction;

import bigdata.objects.Asset;
import bigdata.objects.AssetFeatures;
import bigdata.objects.StockPrice;
import bigdata.technicalindicators.Returns;
import bigdata.technicalindicators.Volitility;
import bigdata.util.TimeUtil;

/**
 * This creates an Asset, with the constructor values, a ticker and an
 * assetFeature field, for a group of <Ticker, StockPrice> records. It returns
 * an Asset if the computed volatility is below a specified threshold.
 *
 * @author Boluwatife
 */
public class PriceToAssetFlatMapGroup implements FlatMapGroupsFunction<String, StockPrice, Asset> {

	private static final long serialVersionUID = -1148334251256760959L;

	int returnsDays;
	double volatilityCeiling;

	/**
	 * Default constructor, to set the return period and volatility threshold
	 *
	 * @param returnsDays       the number of days to calculate returns over (e.g., 5)
	 * @param volatilityCeiling the maximum allowed volatility value
	 */
	public PriceToAssetFlatMapGroup(int returnsDays, double volatilityCeiling) {
		this.returnsDays = returnsDays;
		this.volatilityCeiling = volatilityCeiling;
	}

	/**
	 * For each group of StockPrice records with the same ticker: 
	 * - Sort the records by date 
	 * - Get the closing prices 
	 * - Compute the volatility using the provided Volatility.calculate() method. 
	 * - If volatility is below the threshold, 
	 *   compute the returns using the provided Returns.calculate() method,
	 *   creates an AssetFeature with the returns and volatility and build an
	 *   Asset with the ticker and Asset features
	 */
	@Override

	public Iterator<Asset> call(String key, Iterator<StockPrice> values) throws Exception {
		// Collect all StockPrice records for the key into a list
		List<StockPrice> priceList = new ArrayList<>();
		while (values.hasNext()) {
			StockPrice price = values.next();
			priceList.add(price);
		}

		// Sort the priceList by date using TimeUtil
		Collections.sort(priceList, new Comparator<StockPrice>() {
			@Override
			public int compare(StockPrice p1, StockPrice p2) {
				Instant d1 = TimeUtil.fromDate(p1.getYear(), p1.getMonth(), p1.getDay());
				Instant d2 = TimeUtil.fromDate(p2.getYear(), p2.getMonth(), p2.getDay());
				return d1.compareTo(d2);
			}
		});

		// Get the closing prices
		List<Double> closePrices = new ArrayList<>();
		for (StockPrice price : priceList) {
			closePrices.add(price.getClosePrice());
		}

		// Calculate volatility using the Volatility.calculate() method
		double volatilityValue = Volitility.calculate(closePrices);

		// Initialise the result list
		List<Asset> result = new ArrayList<>();

		// If volatility is acceptable(below volatilityCeiling), compute returns and
		// create an Asset
		if (volatilityValue < volatilityCeiling) {
			// Calculate returns using the Returns.calculate() method
			double returnsValue = Returns.calculate(returnsDays, closePrices);

			// Create a new AssetFeatures with the returns and volatility computed features
			AssetFeatures assetFeatures = new AssetFeatures();
			assetFeatures.setAssetReturn(returnsValue);
			assetFeatures.setAssetVolitility(volatilityValue);

			// Create a new Asset with the key (ticker) and computed asset features.
			result.add(new Asset(key, assetFeatures));

		}
		return result.iterator();

	}
}
