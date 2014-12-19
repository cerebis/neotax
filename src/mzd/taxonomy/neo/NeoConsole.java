package mzd.taxonomy.neo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class NeoConsole {
	private NeoDao dao;
	
	public NeoConsole() {
		this.dao = new NeoDao();
	}
	
	public NeoConsole(File dbPath) {
		this.dao = new NeoDao(dbPath);
	}
	
	public NeoDao getDao() {
		return this.dao;
	}
	
	/**
	 * Validate that a list of taxons exist. Results are printed
	 * to stdout.
	 * 
	 * @param taxons the taxons to validate
	 */
	public void validateTaxons(List<Integer> taxons) {
		getDao().taxonsExist(taxons);
	}
	
	/**
	 * Validate that a taxon exists for the given ids, one per line
	 * in a file. Results are printed to stdout
	 * 
	 * @param inputTaxa the file of taxon ids
	 * @throws IOException if there was an error reading file
	 */
	public void validateTaxons(File inputTaxa) throws IOException {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(inputTaxa));
			while (true) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				line = line.trim();
				if (line.length() == 0) {
					continue;
				}
				String[] fields = line.split(" ");
				if (fields.length > 1) {
					throw new RuntimeException("Only one per taxon id per line when validating individuals");
				}
				List<Integer> taxIds = stringsToInt(fields);
				getDao().taxonsExist(taxIds);
			}
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
	
	/**
	 * Validate a single lineage, checkig each taxon in the series.
	 * Results are printed to stdout.
	 * 
	 * @param lineage a list representing a single lineage
	 * @param reverseOrder if the order should be reversed prior to checking
	 */
	public void validateLineage(List<Integer> lineage, boolean reverseOrder) {
		boolean valid = getDao().traversePath(lineage, reverseOrder);
		System.out.println(lineage.toString() + (valid ? " valid" : " invalid"));
	}
	
	/**
	 * Validate a lineage, checking each taxon in the series.
	 * Results are printed to stdout.
	 * 
	 * @param inputLineages input file of lineages, one per line
	 * @param reverseOrder reverse lineage order prior to checkign
	 * @throws IOException when error reading from file
	 */
	public void validateLineage(File inputLineages, boolean reverseOrder) throws IOException {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(inputLineages));
			while (true) {
				String line = reader.readLine();
				if (line == null) {
					break;
				}
				line = line.trim();
				if (line.length() == 0) {
					continue;
				}
				String[] fields = line.split(" ");
				
				List<Integer> lineage = stringsToInt(fields);
				boolean valid = getDao().traversePath(lineage, reverseOrder);
				System.out.println(lineage.toString() + (valid ? " valid" : " invalid"));
			}
		}
		finally {
			if (reader != null) {
				reader.close();
			}
		}
	}
	
	public void shutdown() {
		getDao().shutdown();
	}
	
	/**
	 * Convert {@code String[]} to {@code List<Integer>}.
	 * 
	 * @param values list of integers in string form
	 * @return list of integers
	 * @throws RuntimeException when string cannot be converted to an
	 * integer
	 */
	public static List<Integer> stringsToInt(String[] values) {
		try {
			List<Integer> intVals = new ArrayList<Integer>();
			for (String s : values) {
				intVals.add(Integer.parseInt(s));
			}
			return intVals;
		}
		catch (NumberFormatException ex) {
			throw new RuntimeException("Taxon Ids can only be integers.",ex);
		}
	}

	/**
	 * Convert the trailing argument into a File object. Considers only
	 * the first argument.
	 * 
	 * @param cmd commandline to parse
	 * @return File object
	 * @throws ParseException
	 */
	public static File getInputFile(CommandLine cmd) throws ParseException {
		String[] args = cmd.getArgs();
		if (args.length != 1) {
			throw new ParseException("Only one file source can be specified");
		}
		File inputFile = new File(args[0]);
		if (!inputFile.exists() || !inputFile.isFile()) {
			throw new ParseException("Input does not exist or is not a file");
		}
		return inputFile;
	}
	
	@SuppressWarnings("static-access")
	public static void main(String[] args) throws IOException, ParseException {
		char DATABASE_OPT = 'd';
		char LINEAGE_OPT = 'l';
		char TAXON_OPT = 't';
		char REVERSE_OPT = 'r';
		char FILE_OPT = 'f';

		Options options = new Options();
		
		OptionGroup cli_group = new OptionGroup();
		
		cli_group.addOption(OptionBuilder
				.withDescription("Validate NCBI taxon by taxonomic id (integer)")
				.withArgName("TAXID")
				.hasArg(false)
				.create(TAXON_OPT));
		
		cli_group.addOption(OptionBuilder
				.withDescription("Validate comma taxonomic lineage (comma separated integers)")
				.withArgName("LINEAGE")
				.hasArg(false)
				.create(LINEAGE_OPT));
		
		options.addOptionGroup(cli_group);
		
		options.addOption(OptionBuilder
				.withDescription("Database path")
				.withArgName("PATH")
				.hasArg()
				.create(DATABASE_OPT));
		
		options.addOption(OptionBuilder
				.withDescription("Reverse lineage order")
				.hasArg(false)
				.create(REVERSE_OPT));
		
		options.addOption(OptionBuilder
				.withDescription("File input")
				.hasArg(false)
				.create(FILE_OPT));
		
		CommandLineParser parser =  new GnuParser();
		NeoConsole nc = null;
		
		try {
			// Parse command line
			CommandLine cmd = parser.parse(options, args, true);
			
			if (cmd.getArgList().size() == 0) {
				throw new ParseException("no arguments supplied");
			}
			
			// Set the path to the database, using a canonical path
			// since Neo4j won't accept paths without at least
			// a relative directory specification.
			File dbPath = null;
			if (cmd.hasOption(DATABASE_OPT)) {
				dbPath = new File(cmd.getOptionValue(DATABASE_OPT))
							.getCanonicalFile();
				
				if (dbPath.isFile()) {
					throw new IOException(String.format("Specified path %s is not a folder", dbPath));
				}
				if (!dbPath.exists()) {
					throw new IOException(String.format("Database at path %s does not exist", dbPath));
				}
			}
			
			boolean reverseOrder = false;
			if (cmd.hasOption(REVERSE_OPT)) {
				reverseOrder = true;
			}
			
			// Validate taxons
			if (cmd.hasOption(TAXON_OPT)) {
				nc = new NeoConsole(dbPath);
				if (cmd.hasOption(FILE_OPT)) {
					nc.validateTaxons(getInputFile(cmd));
				}
				else {
					nc.validateTaxons(stringsToInt(cmd.getArgs()));
				}
			}
			
			// Validate lineages
			else if (cmd.hasOption(LINEAGE_OPT)) {
				nc = new NeoConsole(dbPath);
				if (cmd.hasOption(FILE_OPT)) {
					nc.validateLineage(getInputFile(cmd), reverseOrder);
				}
				else {
					nc.validateLineage(stringsToInt(cmd.getArgs()), reverseOrder);
				}
			}
			
		}
		catch (ParseException ex) {
			System.out.println("Commandline error: " + ex.getMessage());
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("neotax", options);
		}		
		finally {
			if (nc != null) {
				nc.shutdown();
			}
		}
	}
}
