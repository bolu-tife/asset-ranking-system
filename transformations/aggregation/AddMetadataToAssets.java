package bigdata.transformations.aggregation;

import java.util.Map;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

import bigdata.objects.Asset;
import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetsAggregate;

/**
 * This is a sequence (Seq) function that updates an Asset and adds to the 
 * AssetsAggregate using a broadcast variable of asset metadata.
 *
 * @author Boluwatife
 */
public class AddMetadataToAssets implements Function2<AssetsAggregate, Asset, AssetsAggregate> {

	private static final long serialVersionUID = 6367230651887644388L;

	Broadcast<Map<String, AssetMetadata>> broadcastAssetMetadatas;

	/**
	 * @param broadcastAssetMetadatas the broadcast variable containing asset metadata keyed by a
	 *                        ticker
	 * 
	 */
	public AddMetadataToAssets(Broadcast<Map<String, AssetMetadata>> broadcastAssetMetadatas) {
		this.broadcastAssetMetadatas = broadcastAssetMetadatas;
	}

	/**
	 * For each Asset record, if the ticker is valid, update the
	 * AssetFeatures field in the Asset with the P/E
	 * ratio, and add it to the aggregate
	 */
	@Override
	public AssetsAggregate call(AssetsAggregate aggregate, Asset asset) throws Exception {
		// Get the ticker symbol from the assetMetadata
		String ticker = asset.getTicker();

		// Get the Map of Asset from the broadcast
		Map<String, AssetMetadata> assetMetadatas = broadcastAssetMetadatas.value();

		// Check if the symbol is not null
		if (ticker != null) {
			// Get the asset for the given ticker
			AssetMetadata assetMetadata = assetMetadatas.get(ticker);

			// if the asset is valid, update it with the metadata fields and add the asset
			// to the aggregate
			if (assetMetadata != null) {
				// Get additional metadata fields
				String name = assetMetadata.getName();
				String industry = assetMetadata.getIndustry();
				String sector = assetMetadata.getSector();
				double priceEarningRatio = assetMetadata.getPriceEarningRatio();

				// Update Asset object with the asset metadata fields
				asset.setName(name);
				asset.setIndustry(industry);
				asset.setSector(sector);
				asset.getFeatures().setPeRatio(priceEarningRatio);

				// add asset to aggregate
				aggregate.getAssetMap().put(ticker, asset);
			}

		}
		return aggregate;

	}
}
