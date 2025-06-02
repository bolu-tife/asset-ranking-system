package bigdata.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import bigdata.objects.Asset;
import bigdata.objects.AssetMetadata;
import bigdata.objects.AssetRanking;
import bigdata.objects.AssetsAggregate;
import bigdata.objects.StockPrice;
import bigdata.transformations.aggregation.AddMetadataToAssets;
import bigdata.transformations.aggregation.CombineAssets;
import bigdata.transformations.filters.AssetAggregateFlatMap;
import bigdata.transformations.filters.AssetMetadataPriceEarningRatioFilter;
import bigdata.transformations.filters.NullPriceFilter;
import bigdata.transformations.filters.PriceEndDateFilterFlatMap;
import bigdata.transformations.filters.PriceToAssetFlatMapGroup;
import bigdata.transformations.maps.PriceReaderMap;
import bigdata.transformations.maps.PriceToStockTicker;
import bigdata.transformations.pairing.AssetMetadataPairing;
import bigdata.transformations.pairing.AssetPairing;

public class AssessedExercise {

	public static void main(String[] args) throws InterruptedException {

		// --------------------------------------------------------
		// Static Configuration
		// --------------------------------------------------------
		String datasetEndDate = "2020-04-01";
		double volatilityCeiling = 4;
		double peRatioThreshold = 25;

		long startTime = System.currentTimeMillis();

		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef == null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can
			// get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that
			// Spark finds it
			sparkMasterDef = "local[4]"; // default is local mode with two executors
		}

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the asset pricing data
		String pricesFile = System.getenv("BIGDATA_PRICES");
		if (pricesFile == null)
			pricesFile = "resources/all_prices-noHead.csv"; // default is a sample with 3 queries

		// Get the asset metadata
		String assetsFile = System.getenv("BIGDATA_ASSETS");
		if (assetsFile == null)
			assetsFile = "resources/stock_data.json"; // default is a sample with 3 queries

		// ----------------------------------------
		// Pre-provided code for loading the data
		// ----------------------------------------

		// Create Datasets based on the input files

		// Load in the assets, this is a relatively small file
		Dataset<Row> assetRows = spark.read().option("multiLine", true).json(assetsFile);
		// assetRows.printSchema();
		System.err.println(assetRows.first().toString());
		JavaPairRDD<String, AssetMetadata> assetMetadata = assetRows.toJavaRDD().mapToPair(new AssetMetadataPairing());

		// Load in the prices, this is a large file (not so much in data size, but in
		// number of records)
		Dataset<Row> priceRows = spark.read().csv(pricesFile); // read CSV file
		Dataset<Row> priceRowsNoNull = priceRows.filter(new NullPriceFilter()); // filter out rows with null prices
		Dataset<StockPrice> prices = priceRowsNoNull.map(new PriceReaderMap(), Encoders.bean(StockPrice.class)); // Convert
		// to
		// Stock
		// Price
		// Objects

		AssetRanking finalRanking = rankInvestments(spark, assetMetadata, prices, datasetEndDate, volatilityCeiling,
				peRatioThreshold);

		System.out.println(finalRanking.toString());

		System.out.println("Holding Spark UI open for 1 minute: http://localhost:4040");

		Thread.sleep(60000);

		// Close the spark session
		spark.close();

		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out != null)
			resultsDIR = out;

		long endTime = System.currentTimeMillis();

		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(resultsDIR).getAbsolutePath() + "/SPARK.DONE")));

			Instant sinstant = Instant.ofEpochSecond(startTime / 1000);
			Date sdate = Date.from(sinstant);

			Instant einstant = Instant.ofEpochSecond(endTime / 1000);
			Date edate = Date.from(einstant);

			writer.write("StartTime:" + sdate.toGMTString() + '\n');
			writer.write("EndTime:" + edate.toGMTString() + '\n');
			writer.write("Seconds: " + ((endTime - startTime) / 1000) + '\n');
			writer.write('\n');
			writer.write(finalRanking.toString());
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static AssetRanking rankInvestments(SparkSession spark, JavaPairRDD<String, AssetMetadata> assetMetadata,
			Dataset<StockPrice> prices, String datasetEndDate, double volatilityCeiling, double peRatioThreshold) {

		// ----------------------------------------
		// Student's solution starts here
		// ----------------------------------------

		// Filter stock prices to only include records from one year before the
		// datasetEndDate (inclusive)
		PriceEndDateFilterFlatMap priceEndDateFilter = new PriceEndDateFilterFlatMap(datasetEndDate);
		Dataset<StockPrice> datasetEndDatePrices = prices.flatMap(priceEndDateFilter, Encoders.bean(StockPrice.class));

		// Group the filtered stock prices by their stock ticker/symbol
		PriceToStockTicker keyFunction = new PriceToStockTicker();
		KeyValueGroupedDataset<String, StockPrice> pricesByStockTicker = datasetEndDatePrices.groupByKey(keyFunction,
				Encoders.STRING());

		// Create asset with price and asset features(returns and volatility) for each
		// ticker
		int returnDays = 5;
		PriceToAssetFlatMapGroup priceToAsset = new PriceToAssetFlatMapGroup(returnDays, volatilityCeiling);
		Dataset<Asset> assetDS = pricesByStockTicker.flatMapGroups(priceToAsset, Encoders.bean(Asset.class));

		// Filter asset metadata to remove rows with P/E ratios <= 0 or >=
		// peRatioThreshold
		AssetMetadataPriceEarningRatioFilter assetMetadataPriceEarningRatioFunction = new AssetMetadataPriceEarningRatioFilter(
				peRatioThreshold);
		JavaPairRDD<String, AssetMetadata> filteredAssetMetadata = assetMetadata
				.filter(assetMetadataPriceEarningRatioFunction);

		// Collect the Asset metadata dataset as a Map of(ticker, AssetMetadata)
		Map<String, AssetMetadata> assetMetadataMap = filteredAssetMetadata.collectAsMap();

		// Broadcast the assetMetadata to all executors for efficient lookup during
		// aggregation
		Broadcast<Map<String, AssetMetadata>> broadcastMetadatas = JavaSparkContext
				.fromSparkContext(spark.sparkContext()).broadcast(assetMetadataMap);

		// Collect the Asset dataset as a Map of(ticker, Asset)
		AssetPairing pairFunction = new AssetPairing();
		JavaPairRDD<String, Asset> assetMap = assetDS.toJavaRDD().mapToPair(pairFunction);

		// Initialize an empty AssetsAggregate that holds a mapping of an asset ticker
		// and an asset
		AssetsAggregate assetsData = new AssetsAggregate();

		// Aggregate asset metadata into the AssetsAggregate using the broadcast asset
		// metadata map
		AddMetadataToAssets addMetadataToAssets = new AddMetadataToAssets(broadcastMetadatas);

		// Combines two AssetsAggregate objects together from different partitions
		CombineAssets combineAssets = new CombineAssets();

		// Aggregate asset per ticker, resulting in one AssetsAggregate per
		// asset symbol
		JavaPairRDD<String, AssetsAggregate> AssetsByTicker = assetMap.aggregateByKey(assetsData, addMetadataToAssets,
				combineAssets);

		// Extract individual Asset objects from the aggregated assets (flatten the
		// aggregate)
		AssetAggregateFlatMap assetAggregateFlatMap = new AssetAggregateFlatMap();
		JavaRDD<Asset> assetRDD = AssetsByTicker.flatMap(assetAggregateFlatMap);

		// Collect assets from the RDD into a mutable ArrayList
		List<Asset> allAssets = new ArrayList<>(assetRDD.collect());

		// Sort the list of assets in ascending order using Asset.compareTo()
		Collections.sort(allAssets);

		// Reverse the sorted list so that assets with the highest returns come first
		Collections.reverse(allAssets);

		// Extract the top 5 assets (or fewer if there aren't 5) from the sorted list
		List<Asset> topAssets = allAssets.subList(0, Math.min(5, allAssets.size()));

		// Create an AssetRanking object from the top 5 assets by converting the list to
		// an array
		AssetRanking finalRanking = new AssetRanking(topAssets.toArray(new Asset[topAssets.size()]));

		return finalRanking;
	}

}
