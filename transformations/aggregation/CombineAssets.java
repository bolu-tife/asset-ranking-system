package bigdata.transformations.aggregation;

import org.apache.spark.api.java.function.Function2;

import bigdata.objects.AssetsAggregate;

/**
 * This is a Spark Combination (Comb) function, that merges two AssetsAggregate
 * objects by combining their asset maps. 
 * 
 * @author Boluwatife
 */
public class CombineAssets implements Function2<AssetsAggregate, AssetsAggregate, AssetsAggregate> {

	private static final long serialVersionUID = -3578488018462757792L;

	/**
	 * adds all tickers in aggregate 2 to aggregate 1 and return the new larger
	 * aggregate 1
	 */
	@Override
	public AssetsAggregate call(AssetsAggregate aggregate1, AssetsAggregate aggregate2) throws Exception {
		// Iterate over each ticker in aggregate 2 and adds them to aggregate 1
		for (String ticker : aggregate2.getAssetMap().keySet()) {
			// If ticker is not present in aggregate1, add it
			if (!aggregate1.getAssetMap().containsKey(ticker)) {
				aggregate1.getAssetMap().put(ticker, aggregate2.getAssetMap().get(ticker));
			}
		}
		return aggregate1;

	}

}
