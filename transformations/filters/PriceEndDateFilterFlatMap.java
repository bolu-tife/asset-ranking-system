package bigdata.transformations.filters;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import bigdata.objects.StockPrice;
import bigdata.util.TimeUtil;

/**
 * This filters StockPrice records based on the datasetEndDate It returns the
 * record if its date is within one year(365 days) before or on the datasetEndDate.
 *
 * @author Boluwatife
 */
public class PriceEndDateFilterFlatMap implements FlatMapFunction<StockPrice, StockPrice> {

	private static final long serialVersionUID = 9222015963108925505L;

	String datasetEndDate;

	/**
	 * Default constructor, specifies the date not to be filtered out in
	 * format(e.g., "2020-04-01", "YYYY-MM-DD")
	 *
	 * @param datasetEndDate
	 */
	public PriceEndDateFilterFlatMap(String datasetEndDate) {
		this.datasetEndDate = datasetEndDate;
	}

	@Override
	public Iterator<StockPrice> call(StockPrice price) throws Exception {

		// Convert the price's date components to an Instant
		Instant stockDate = TimeUtil.fromDate(price.getYear(), price.getMonth(), price.getDay());

		// Convert the datasetEndDate to an Instant
		Instant endDate = TimeUtil.fromDate(datasetEndDate);

		// Calculate the start date as one year(365 days) before the end date
		Instant startDate = endDate.minus(Duration.ofDays(365));

		// Check if the stock date is between the start and end dates (inclusive).
		if (((stockDate.isAfter(startDate) || stockDate.equals(startDate)))
				&& (stockDate.isBefore(endDate) || stockDate.equals(endDate))) {
			List<StockPrice> priceList = new ArrayList<StockPrice>(1);
			priceList.add(price);
			return priceList.iterator(); // Return the iterator containing the filtered price
		} else {
			// if one of the check fails we want to return nothing
			List<StockPrice> priceList = new ArrayList<StockPrice>(0); // create an empty array of size 0
			return priceList.iterator(); // return the iterator for the empty list

		}

	}

}
