package bigdata.objects;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This is dictionary/hash map that provides a mapping between an asset ticker
 * and the asset.
 *
 * Its serializable, so can be used in Spark Functions
 *
 * @author Boluwatife
 */
public class AssetsAggregate implements Serializable {

	private static final long serialVersionUID = -6491221583591599703L;

	// Map of ticker to Asset
	Map<String, Asset> assetMap;

	// Default constructor that creates an empty HashMap
	public AssetsAggregate() {
		assetMap = new HashMap<String, Asset>();
	}

	// Constructor that initializes the aggregate with an existing map.
	public AssetsAggregate(Map<String, Asset> assetMap) {
		super();
		this.assetMap = assetMap;
	}

	public Map<String, Asset> getAssetMap() {
		return assetMap;
	}

	public void setAssetMap(Map<String, Asset> assetMap) {
		this.assetMap = assetMap;
	}

	public static long getSerialversionuid() {
		return serialVersionUID;
	}

}
