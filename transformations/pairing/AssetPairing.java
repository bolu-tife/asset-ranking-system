package bigdata.transformations.pairing;

import org.apache.spark.api.java.function.PairFunction;

import bigdata.objects.Asset;
import scala.Tuple2;

/**
 * Converts an Asset to a tuple (key-value pair) where the key is the
 * asset ticker and the value is the corresponding Asset
 *
 * @author Boluwatife
 */
public class AssetPairing implements PairFunction<Asset, String, Asset> {

	private static final long serialVersionUID = -1631287585791997889L;

	/**
	 * Retrieve the ticker and the asset from the Asset
	 */
	@Override
	public Tuple2<String, Asset> call(Asset t) throws Exception {
		String ticker = t.getTicker();
		Asset asset = t;

		return new Tuple2<String, Asset>(ticker, asset);

	}

}
