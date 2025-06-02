package bigdata.transformations.filters;

import org.apache.spark.api.java.function.Function;

import bigdata.objects.AssetMetadata;
import scala.Tuple2;

/**
 * This filters out AssetMetadata PriceEarningRatio <= 0 and >= threshold.
 * It returns AssetMetadata with 0 < PriceEarningRatio < threshold. 
 * Also, null AssetMetadata are filtered out
 * 
 * @author Boluwatife
 */
public class AssetMetadataPriceEarningRatioFilter implements Function<Tuple2<String, AssetMetadata>, Boolean> {

	private static final long serialVersionUID = 7445156591246829133L;

	double peThreshold;

	/**
	 * @param peThreshold the maximum allowed Price-to-Earnings ratio
	 * 
	 */
	public AssetMetadataPriceEarningRatioFilter(double peThreshold) {
		this.peThreshold = peThreshold;
	}

	@Override
	public Boolean call(Tuple2<String, AssetMetadata> tuple) throws Exception {
		AssetMetadata assetMetadata = tuple._2();

		return assetMetadata != null && assetMetadata.getPriceEarningRatio() < peThreshold
				&& assetMetadata.getPriceEarningRatio() > 0;
	}

}