package com.company.fileprocessingengine.business;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;

/**
 * 
 * @author Yash Khatri 
 * 
 * 		   This class contains methods for reading the file from the
 *         source path and saving the output in destinaton path.
 *
 */
@Component
public class FileProcessorImpl implements FileProcessor{

	@Value("${source.path}")
	private String sourcePath;

	@Value("${destination.path}")
	private String destinationPath;

	// Counter to count number of files processed.
	private static Integer counter = 0;

	static final Logger LOGGER = LoggerFactory.getLogger(FileProcessorImpl.class);

	/**
	 * This method is used for reading the files. The sourece location is specified
	 * in the application.properties file
	 * @throws Exception 
	 */
	public void readFiles() throws Exception {

		final Path baseDir = Paths.get(sourcePath);

		List<Path> filesList = Lists.newArrayList();

		final BiPredicate<Path, BasicFileAttributes> predicate = (path, attrs) -> attrs.isRegularFile()
				&& path.toString().endsWith(".txt");

		try (final Stream<Path> stream = Files.find(baseDir, 1, predicate);) {
			filesList = stream.collect(Collectors.toList());
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Creating 10000 threads. Maximum 10000 files can be processed in parallel.
		ExecutorService executor = Executors.newFixedThreadPool(10000);
		for (Path path : filesList) {
			executor.execute(new Task(path));

		}
	}

	/**
	 * Task for multi-threading.
	 */
	public class Task implements Runnable {

		public Task(Path path) throws Exception {
			LOGGER.info("Processing file : {}", path.toString());
			processFiles(path);
		}

		public void run() {
			LOGGER.debug(Thread.currentThread().getName() + " is Started");
		}
	}
	
	/**
	 * This method is used for reading the file and extracting the data.
	 * 
	 * @param path The path of the input file.
	 * @throws Exception The generic exception.
	 */
	private void processFiles(Path path) throws Exception {

		StringBuilder stringBuilder = new StringBuilder();

		AtomicInteger noOfClients = new AtomicInteger(0);
		AtomicInteger noOfSellers = new AtomicInteger(0);
		Map<String, Double> salesMap = new HashMap<String, Double>();
		Map<String, Integer> soldItemsMap = new HashMap<String, Integer>();

		try (Stream<String> stream = Files.lines(path)) {

			stream.peek((line) -> {
				String[] dataArray = line.split(",");
				// If 0th index value is 001 it is a clients data.
				if (Integer.valueOf(dataArray[0]).equals(Integer.valueOf(001)))
					noOfClients.getAndIncrement();
				// If 0th index value is 002 it is a sellers data.
				if (Integer.valueOf(dataArray[0]).equals(Integer.valueOf(002)))
					noOfSellers.getAndIncrement();
				// If 0th index value is 003 it is a sales data.
				if (Integer.valueOf(dataArray[0]).equals(Integer.valueOf(003))) {
					processSalesData(salesMap, soldItemsMap, dataArray);
				}

			}).forEach(System.out::println);
			
			writeOutput(path, stringBuilder, noOfClients, noOfSellers, salesMap, soldItemsMap);

		} catch (Exception e) {
			LOGGER.error(e.getLocalizedMessage());
			e.printStackTrace();
			throw new Exception("Application Stopped due to exception in file processing");
		}


	}

	/**
	 * 
	 * @param salesMap     Contains sales_id as key and total_selling_price as
	 *                     value.
	 * @param soldItemsMap Contains seller_name as key and total_items_sold as value
	 * @param dataArray
	 * 
	 *                     Example Data: Sales data - Format: (003, sales_id,
	 *                     [item_id-amount-price], seller_name)
	 *                     003,01,[1-10-100;2-30-2.50;3-40-3.10],Jeferson Items
	 *                     Arrays: [1-10-100;2-30-2.50;3-40-3.10]
	 * 
	 */

	private void processSalesData(Map<String, Double> salesMap, Map<String, Integer> soldItemsMap,
			String[] dataArray) {

		// Collecting items data in items array.
		String[] itemsArray = dataArray[2].split(";");
		Double totalSaleOfASeller = 0D;
		Integer totalItemSold = 0;

		for (String itemDetail : itemsArray) {
			// Splitting and collecting item_id, amount, price
			String[] itemStatsArray = itemDetail.split("-");
			totalItemSold = totalItemSold + Integer.valueOf(itemStatsArray[1]);
			totalSaleOfASeller = totalSaleOfASeller
					+ Integer.valueOf(itemStatsArray[1]) * Double.valueOf(itemStatsArray[2].replace("]", ""));
		}

		salesMap.put(dataArray[1], totalSaleOfASeller);
		soldItemsMap.put(dataArray[3], totalItemSold);

		LOGGER.debug("Total Sales of a Seller", totalSaleOfASeller);
	}

	/**
	 * This methid is used for writing the results to the file.
	 * 
	 * @param path The path of the input file.
	 * @param stringBuilder String builder containing the outout to be written in file.
	 * @param noOfClients Number of clients data in file.
	 * @param noOfSellers Number of Sellers data in file.
	 * @param salesMap Map of sales_id as key and total selling price as value.
	 * @param soldItemsMap Map of Sellers name as key and num of items sold as value.
	 * @throws Exception The generic expection.
	 */
	private void writeOutput(Path path, StringBuilder stringBuilder, AtomicInteger noOfClients,
			AtomicInteger noOfSellers, Map<String, Double> salesMap, Map<String, Integer> soldItemsMap) throws Exception {

		try {

			LOGGER.debug("Num of Clients data are {} in file {}: ", noOfClients);
			LOGGER.debug("Num of Sales data are {} in file {}: ", noOfSellers);

			Optional<Entry<String, Double>> biggestSaleEntry = salesMap.entrySet().stream()
					.max(Comparator.comparing(Map.Entry::getValue));

			LOGGER.debug("Max Sales ID is {} in file {}: ", biggestSaleEntry.get().getKey(), path.toString());

			Optional<Entry<String, Integer>> sellersNameEntry = soldItemsMap.entrySet().stream()
					.min(Comparator.comparing(Map.Entry::getValue));

			LOGGER.debug("Seller with minimum sales", sellersNameEntry.get().getKey());

			stringBuilder.append("-> Number of Clients found in the file: " + noOfClients + "\n");
			stringBuilder.append("-> Number of Sellers found in the file: " + noOfSellers + "\n");
			stringBuilder.append("-> Sales id of the biggest sale: " + biggestSaleEntry.get().getKey() + "\n");
			stringBuilder.append("-> Name of the Seller that sold less items: " + sellersNameEntry.get().getKey());

			String outputFileName = path.getFileName().toString().replace(".txt", ".done.txt");

			Files.write(Paths.get(destinationPath + "/" + outputFileName), stringBuilder.toString().getBytes());
			
			counter++;
			
			path.toFile().delete();

			LOGGER.info("File {} processed and deleted", path.toString());
			LOGGER.info("Total files processed: {} ", counter);	
		
		} catch (Exception e) {
			LOGGER.error("Exception while processing file {}", path.toString());
			e.printStackTrace();
			throw new Exception("Application Stopped due to exception in file processing");
		}
	}
}
