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

/**
 * Data access object for NCBI taxonomy stored in Neo4j database
 */
public class NeoDao {

	// Access mode
	public static enum Mode {
		INIT_NEW,
		OPEN_EXISTING
	}
	
	private GraphDatabaseService gds;
	// all manner of options have been tried here. Neo4j keeps making logs anyhow.
	private static String LOGICAL_LOG_SIZE = "false";
	private static String NCBI_DELIMITER = "\t\\|\t?";
	private static Label NODE_LABEL = DynamicLabel.label("Node");
	private static File DEFAULT_DATABASE = new File("./taxdb");
	private static int PERIODIC_COMMIT = 5000;
	private static Logger logger = LoggerFactory.getLogger(NeoConsole.class);
	private File dbPath;
	
	public NeoDao() throws IOException {
		this(DEFAULT_DATABASE, Mode.OPEN_EXISTING);
		dbPath = DEFAULT_DATABASE;
	}
	
	public NeoDao(Mode mode) throws IOException {
		this(DEFAULT_DATABASE, mode);
		dbPath = DEFAULT_DATABASE;
	}
	
	public NeoDao(File dbPath) throws IOException {
		this(dbPath, Mode.OPEN_EXISTING);
	}
	
	public NeoDao(File dbPath, Mode mode) throws IOException {
		if (dbPath == null) {
			dbPath = DEFAULT_DATABASE;
		}
		this.dbPath = dbPath;

		// Mode determines whether database should already exist.
		switch (mode) {
		case OPEN_EXISTING:
			if (!dbPath.exists()) {
				throw new IOException(String.format("Database at path [%s] does not exist", dbPath));
			}
			if (dbPath.isFile()) {
				throw new IOException(String.format("Specified path [%s] is not a folder", dbPath));
			}
			break;
		case INIT_NEW:
			if (dbPath.exists()) {
				throw new IOException(String.format("Database at path [%s] already exists. " +
						"Move away or delete before initialisation.", dbPath));
			}
			break;
		}
		
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

	private void storeNodes(File path) throws IOException {
		
		// Create all NCBI node nodes... (an unfortunate name clash)
		System.out.println("Creating nodes...");
		logger.debug("Creating nodes...");

		BufferedReader reader = null;
		Transaction tx = null;
		try {
			reader = new BufferedReader(new FileReader(path));
			tx = beginTransaction();
			int n_nodes = 0;
			ConsoleProgress progress = new ConsoleProgress(10000);
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
				
				// Do import over multiple transactions for the
				// sake of memory footprint.
				if (++n_nodes % PERIODIC_COMMIT == 0) {
					tx.success();
					tx.close();
					tx = beginTransaction();
				}
				
				progress.print();
			}
			
			tx.success();
		}
		finally {
			if (reader != null) {
				reader.close();
			}
			if (tx != null) {
				tx.close();
			}
		}
	
		// Add constraints
		try (Transaction tx_ = beginTransaction()) {
			ExecutionEngine engine = getEngineInstance();
			engine.execute("create constraint on (n:Node) assert n.taxid is unique;");
			engine.execute("create index on :Node(parentid);");
			engine.execute("create index on :Node(rank);");
			tx_.success();
		}
		
		// All parent->child relations created via this database query.
		System.out.println("Creating relations...");
		logger.debug("Creating relations...");
		
		long maxNode = countAllNodes();
		ConsoleProgress progress = new ConsoleProgress(maxNode,50);
		for (long n=0; n<=maxNode; n+=PERIODIC_COMMIT) {
			try (Transaction tx_ = beginTransaction()) {
				Map<String, Object> props = new HashMap<String, Object>();
				props.put("skip_n", n);
				props.put("limit_n", PERIODIC_COMMIT);
				getEngineInstance().execute(
						"match (parent:Node), (child:Node) " +
							"using index parent:Node(taxid) " +
							"where parent.taxid = child.parentid " + 
							"with child, parent " +
							"skip {skip_n} limit {limit_n} " +
							"create (child)-[:CHILD]->(parent);", props);
				tx_.success();
			}
			progress.updateTo(n);
		}
		progress.finish();
 	}
	
	private void addNames(File path) throws IOException {
		System.out.println("Decorating nodes with scientific names...");
		logger.debug("Decorating nodes with scientific names...");
		
		BufferedReader reader = null;
		Transaction tx = null;
		try {
			reader = new BufferedReader(new FileReader(path));
			tx = beginTransaction();
			
			int n_nodes = 0;
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
				
				// Do import over multiple transactions for the
				// sake of memory footprint.
				if (++n_nodes % PERIODIC_COMMIT == 0) {
					tx.success();
					tx.close();
					tx = beginTransaction();
				}
			}

			tx.success();
		}
		finally {
			if (reader != null) {
				reader.close();
			}
			if (tx != null) {
				tx.close();
			}
		}
		
		try (Transaction tx_ = beginTransaction()) {
			// create an index on the names for queries.
			getEngineInstance().execute("create index on :Node(sci_name);");
			tx_.success();
		}
	}
	
	protected Long getLargestNodeId() {
		ExecutionResult result = getEngineInstance().execute(
				"match (Node) return max(id(Node)) as max_id");
		
		return (Long)IteratorUtil.single(result).get("max_id");
	}
	
	protected Long countAllNodes() {
		ExecutionResult result = getEngineInstance().execute(
				"match (Node) return count(*) as node_count");
		
		return (Long)IteratorUtil.single(result).get("node_count");
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

	private static boolean fileExists(File fname) {
		return fname.exists() && !fname.isDirectory();
	}
	
	/**
	 * Initialise a new database and once complete issue shutdown. To use the newly
	 * created database, users should create a new instance of {@code NeoDao}.
	 *  
	 * @param dbPath
	 * @param nodesDump
	 * @param namesDump
	 * @throws IOException
	 */
	public void initialiseDatabase(File dbPath, File nodesDump, File namesDump) throws IOException {

		// Check NCBI files for existence.
		for (File f : new File[]{nodesDump, namesDump}) {
			if (!fileExists(nodesDump)) {
				throw new IOException(String.format(
						"Specified NCBI input file [%s] does not exist or is not a file",f.getPath()));
			}
		}
		
		storeNodes(nodesDump);
		addNames(namesDump);
		shutdown();
	}

}
