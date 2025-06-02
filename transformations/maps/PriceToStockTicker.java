package bigdata.transformations.maps;

import org.apache.spark.api.java.function.MapFunction;

import bigdata.objects.StockPrice;

/**
 * Return the Stock Ticker for a stock price (StockPrice object)
 *
 * @author Boluwatife
 */
public class PriceToStockTicker implements MapFunction<StockPrice, String> {

	private static final long serialVersionUID = 6143536254514641987L;

	/**
	 * Returns the stock ticker from the given StockPrice object.
	 */
	@Override
	public String call(StockPrice value) throws Exception {
		return value.getStockTicker();
	}

}
