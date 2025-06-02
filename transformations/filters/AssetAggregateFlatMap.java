package bigdata.transformations.filters;

import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import bigdata.objects.Asset;
import bigdata.objects.AssetsAggregate;
import scala.Tuple2;

/**
 * AssetAggregateFlatMap is a Spark FlatMapFunction that takes a Tuple2 consisting of a key (ticker)
 * and an AssetsAggregate, and returns an iterator over the individual Asset objects contained 
 * within the AssetsAggregate
 * 
 * @author Boluwatife
 */
public class AssetAggregateFlatMap implements FlatMapFunction<Tuple2<String, AssetsAggregate>, Asset> {

	private static final long serialVersionUID = 603458921765097670L;

	/**
	 * The call method extracts the asset map from the AssetsAggregate (the second element of the tuple)
     * and returns an iterator over its values (the individual Asset objects).
     *
	 */
	@Override
	public Iterator<Asset> call(Tuple2<String, AssetsAggregate> tuple) throws Exception {
		return tuple._2().getAssetMap().values().iterator();
	}

}
