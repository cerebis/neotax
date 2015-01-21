##NCBI Taxonomy validator using Neo4j database.

Validate taxon ids and lineages against the published Taxonomy database.

Includes methods for populating a new Neo4J database from nodes.dmp and names.dmp.

For the sake of spaces conservation, the backing database does not attempt to be a complete representation of all the information in the NCBI Taxonomy dump. There is currently only enough information stored to achieve the goal of validation.

The project can be built using Maven. (Eg. ```mvn package```)

###Installation

1. Get NeoTax
  a. Download a standalone jar from Releases
  b. Or build from source using maven (the resulting jar will require supporting libraries on the classpath)
2. Download or initialise a taxonomy database
  a. Download and extract a premade database from Releases
  b. Or initialise a new database
    1. Download taxdump.tar.gz from [NCBI FTP](ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/)
    2. Extract the archive to some path.
    2. java -jar neotax-x.y.jar -i {extracted path}/nodes.dmp {extracted path}/names.dmp
