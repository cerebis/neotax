package mzd.taxonomy.neo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.IteratorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.convert.Wrappers.SeqWrapper;

public class NeoDao {

	private GraphDatabaseService gds;
	// all manner of options have been tried here. Neo4j keeps making logs anyhow.
	private static String LOGICAL_LOG_SIZE = "false";
	private static String NCBI_DELIMITER = "\t\\|\t?";
	private static Label NODE_LABEL = DynamicLabel.label("Node");
	private static File DEFAULT_DATABASE = new File("./neotaxdb");
	private static Logger logger = LoggerFactory.getLogger(NeoConsole.class);
	private File dbPath;
	
	public NeoDao() {
		this(DEFAULT_DATABASE);
		dbPath = DEFAULT_DATABASE;
	}
	
	public NeoDao(File dbPath) {
		if (dbPath == null) {
			dbPath = DEFAULT_DATABASE;
		}
		this.dbPath = dbPath;

		gds = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder(dbPath.toString())
				.setConfig(GraphDatabaseSettings.keep_logical_logs, LOGICAL_LOG_SIZE)
				.newGraphDatabase();
		logger.debug("Opened gdb connection to [" + dbPath + "]");
		
		Runtime.getRuntime().addShutdownHook(new Thread(){
			@Override
			public void run() {
				logger.debug("Invoking shutdown hook for gdb");
				gds.shutdown();
			}
		});
	}

	private static String parseString(String field) {
		field = field.trim();
		return field.length() == 0 ? null : field;
	}
	
	private static Integer parseInt(String field) {
		field = parseString(field);
		return field == null ? null : Integer.parseInt(field);
	}

	public void storeNodes(String path) throws IOException {
		
		// Create all NCBI node nodes... (an unfortunate name clash)
		System.out.println("Creating nodes...");
		logger.debug("Creating nodes...");
		
		BufferedReader reader = null;
		try (Transaction tx = beginTransaction()){
			reader = new BufferedReader(new FileReader(new File(path)));
			
			while (true) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				line = line.trim();
				if (line.length() == 0) {
					continue;
				}
				String[] fields = line.split(NCBI_DELIMITER);
				Node n = createNode(NODE_LABEL);
				n.setProperty("taxid", parseInt(fields[0]));
				n.setProperty("parentid", parseInt(fields[1]));
				n.setProperty("rank", parseString(fields[2]));
			}
			
			tx.success();
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	
		// Add constraints
		try (Transaction tx = beginTransaction()) {
			ExecutionEngine engine = getEngineInstance();
			engine.execute("create constraint on (n:Node) assert n.taxid is unique;");
			engine.execute("create index on :Node(parentid);");
			engine.execute("create index on :Node(rank);");
			tx.success();
		}
		
		// All parent->child relations created via this database query.
		System.out.println("Creating relations...");
		logger.debug("Creating relations...");
		
		try (Transaction tx = beginTransaction()) {
			getEngineInstance().execute(
					"match (parent:Node), (child:Node) " +
						"using index parent:Node(taxid) " +
						"where parent.taxid = child.parentid " + 
						"create (child)-[:CHILD]->(parent);");
			tx.success();
		}
 	}
	
	public void addNames(String path) throws IOException {
		System.out.println("Decorating nodes with scientific names...");
		logger.debug("Decorating nodes with scientific names...");
		
		BufferedReader reader = null;
		try (Transaction tx = beginTransaction()){
			reader = new BufferedReader(new FileReader(new File(path)));
			
			ExecutionEngine engine = getEngineInstance();
			
			while (true) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				line = line.trim();
				if (line.length() == 0) {
					continue;
				}
				
				String[] fields = line.split(NCBI_DELIMITER);
				
				// Only record scientific name
				if (!fields[3].equals("scientific name")) {
					continue;
				}
				
				// Append scientific name
				Map<String, Object> props = new HashMap<String, Object>();
				props.put("taxid", parseInt(fields[0]));
				props.put("sci_name", parseString(fields[1]));
				engine.execute(
						"match (n:Node {taxid: {taxid}}) " +
							"set n += {sci_name: {sci_name}} " +
							"return n;", props);
			}

			tx.success();
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
		
		try (Transaction tx = beginTransaction()) {
			// create an index on the names for queries.
			getEngineInstance().execute("create index on :Node(sci_name);");
			tx.success();
		}
	}
	
	/**
	 * Test for the existence of a taxon by taxon id.
	 * 
	 * @param taxId taxon to find
	 * @return true if taxon exists
	 */
	public boolean taxonExists(Integer taxId) {
		try (Transaction tx = beginTransaction()) {
			Map<String, Object> props = new HashMap<String, Object>();
			props.put("taxid", taxId);
			ExecutionResult result = getEngineInstance().execute(
					"optional match (n:Node {taxid: {taxid}}) " +
						"return distinct {taxid} as taxid, count(n) = 1 as node_exists;", props);
			
			ResourceIterator<Map<String, Object>> it = result.iterator();
			Map<String, Object> resultMap = it.next();
			if (it.hasNext()) {
				throw new RuntimeException("findTaxon did not return a unique query result");
			}
			
			return resultMap.get("taxid").equals(taxId) && 
					resultMap.get("node_exists").equals(true);
		}
	}

	public void taxonsExist(List<Integer> taxonList) {
		for (Integer taxId : taxonList) {
			System.out.println(String.format("%d %s",taxId,taxonExists(taxId) ? "valid" : "invalid"));
		}
	}
	
	/**
	 * Test whether a path exists between two ids within the NCBI taxonomy.
	 * 
	 * @param firstId beginning (lowest) taxon id in path
	 * @param topId final (highest/root) taxon id in path
	 * @return true if a path exists
	 */
	public boolean lineageExists(Integer firstId, Integer topId) {
		try (Transaction tx = beginTransaction()) {
			Map<String, Object> props = new HashMap<String, Object>();
			props.put("firstid", firstId);
			props.put("topid", topId);
			ExecutionResult result = getEngineInstance().execute(
					"optional match (first:Node {taxid: {firstid}}), (top:Node {taxid:{topid}}), " +
						"p = shortestPath((first)-[*..20]-(top)) " +
						"return distinct {firstid} as firstid, {topid} as topid, count(p)=1 as path_exists;", props);
			
			Map<String, Object> resultMap = IteratorUtil.single(result);
			return resultMap.get("firstid").equals(firstId) &&
					resultMap.get("topid").equals(topId) &&
					resultMap.get("path_exists").equals(true);
		}
	}

	public boolean traversePath(List<Integer> lineage, boolean reverseOrder) {
		boolean validLineage = true;
		try (Transaction tx = beginTransaction()) {
			ExecutionEngine engine = getEngineInstance();
			
			// We will walk backwards child -> parent.
			if (reverseOrder) {
				Collections.reverse(lineage);
			}
			ListIterator<Integer> it = lineage.listIterator();
			Integer queryId = it.next(); // starting here.
			logger.debug("Beginning lineage test with " + queryId);
			do {
				// Fetch the node via taxid property
				Map<String, Object> props = new HashMap<String, Object>();
				props.put("taxid", queryId);
				ExecutionResult result = engine.execute(
						"match (n:Node {taxid: {taxid}}) return distinct n;",props);

				// Treat as a single-valued result.
				Iterator<Node> nodes = result.columnAs("n");
				try {
					Node next = IteratorUtil.single(nodes);
					Integer parentId = (Integer)next.getProperty("parentid");
					Integer nextInLineage = it.next();
					logger.debug("Checking equality of tax ids: " + parentId + " == " + nextInLineage);
					// Test each node in path for equivalence
					if (!parentId.equals(nextInLineage)) {
						logger.debug(nextInLineage + " was not found to be the parent of " + queryId);
						validLineage = false;
						break;
					}
					queryId = parentId;
				}
				catch (NoSuchElementException ex) {
					throw new RuntimeException(String.format("TaxonId %d was not found", queryId));
				}
			} while (it.hasNext());
		}
		return validLineage;
	}
	
	/**
	 * Determine the taxonomic lineage between two points in the NCBI
	 * taxonomy. This will be the shortest path, although not necessary
	 * since the taxonomy is a directed tree.
	 * 
	 * As the tree is directed, {@code firstId} must be lower within the
	 * taxonomic tree than {@code topId}.
	 * 
	 * @param firstId beginnning (lowest) taxon id in path
	 * @param topId final (highest/root) taxon id in path
	 * @return a list of taxonomic ids, beggining with the top/root id.
	 */
	public List<Integer> getLineage(Integer firstId, Integer topId) {
		List<Integer> lineage = null;
		try (Transaction tx = beginTransaction()) {
			Map<String, Object> props = new HashMap<String, Object>();
			props.put("firstid", firstId);
			props.put("topid", topId);
			ExecutionResult result = getEngineInstance().execute(
					"match (first:Node {taxid: {firstid}}), (top:Node {taxid:{topid}}), " +
						"p = shortestPath((first)-[*..20]-(top)) " +
						"return extract(n in nodes(p)| n.taxid) as lineage;", props);
			
			// Extract the tax ids and add to list.
			// Reverse the order so that the root is first.
			ResourceIterator<SeqWrapper<Integer>> it = result.columnAs("lineage");
			while (it.hasNext()) {
				SeqWrapper<Integer> sw = it.next();
				lineage = Arrays.asList(sw.toArray(new Integer[1]));
				Collections.reverse(lineage);
			}
		}
		return lineage;
	}
	
	public GraphDatabaseService getGds() {
		return this.gds;
	}

	public ExecutionEngine getEngineInstance() {
		return new ExecutionEngine(getGds());
	}
	
	public Node createNode(Label... labels) {
		return getGds().createNode(labels);
	}

	public Transaction beginTransaction() {
		return getGds().beginTx();
	}

	/**
	 * Shutdown method for clean exit. This should be
	 * registered as a shutdown hook.
	 */
	public void shutdown() {
		removeLogFiles();
		getGds().shutdown();
	}
	
	/**
	 * Neo4j leaves behind little logical logs, even when asked not to do so. This is
	 * probably something to do with shutdown or an error on my part with closing 
	 * iterators. (I tried!)
	 * 
	 * This method simply scans the database folder for the relevant files and deletes
	 * them. It is included in the shutdown method which is registered as a hook. Deletion
	 * is done before shutdown, so that Neo4j can be kept happy with one logical file --
	 * just in case we need recovery.
	 */
	public void removeLogFiles() {
		Iterator<File> logFileIt = FileUtils.iterateFiles(
				this.dbPath, new RegexFileFilter(".*\\.log\\.v[0-9]+$"), TrueFileFilter.INSTANCE);
		while (logFileIt.hasNext()) {
			logFileIt.next().delete();
		}
	}

	public static void main(String[] args) throws IOException {
		if (args.length != 3) {
			System.out.println("Usage: <database path> <nodes.dmp> <names.dmp>");
			System.exit(1);
		}
		NeoDao dao = new NeoDao(new File(args[0]));
		dao.storeNodes(args[1]);
		dao.addNames(args[2]);
		dao.shutdown();
	}
	
	
}
