# Charity Commission Data Conversion

 This repository contains Python scripts to convert UK Charity Commission data dumps to CSV and RDF.
 It does not contain copies of the source or converted data itself. To convert the data for yourself
 please read the Usage section below.
 
## Usage
 
 The script is plain Python 3 so you just need Python 3 (tested on 3.3 and later) and some source data to convert.
 
 Download the data extract you want to convert from http://data.charitycommission.gov.uk/. Each data extract is complete,
 so really you only need to grab the most recently published extract. Each extract consists of three zip files, the
 Charity register extract; the SIR data and the Table build scripts. The table build scripts are not used so you can 
 ignore that.
 
 Unzip the downloaded extracts into a directory - the script allows you to specify the location of this directory,
 so it doesn't matter where you choose. Make sure that all the files are extracted into the same directory.
 DO NOT alter the names of these files as they are hard-coded into the script.
 
 The script provides two conversion output options. The CSV output is a straight conversion of the tabular data 
 in each file of the source data extract. The RDF output maps the tabular data to RDF which is written out in
 Turtle syntax. For more details about the structure of the output RDF see the Ontology section below.
 
 To convert to CSV:
 
    python cc_convert.py csv [path to source directory] [path to output directory]
    
 To convert to RDF:
 
    python cc_convert.py rdf [path to source directory] [path to output directory]
 
 NOTE: The converter is written to skip conversion where an output file already exists. To force reconversion,
 specify an empty (or non-existent) directory as the output path.
 
## Ontology

An ontology description is provided in the ontology.ttl file. Although this is an OWL file the ontology 
pretty much restricts itself to simple RDF Schema domain/range statements. 

## About the data

Please remember that the data from the Charity Commission is provided under the 
[Open Government License v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) and as such
you must ensure that you attribute the data as described at that link.

The source data contains names, email addresses and postal addresses for charity contacts and trustees. As a matter of 
respect for the privacy of these individuals we have chosen not to include this information in our RDF output; the 
parts of the script that perform this filtering are clearly marked.

## License

This script is licensed to you under the terms of the Gnu General Public License v3. In particular, it is provided
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

Please see the text file named COPYING in this repository for the full text of the license. IF for any reason you 
do not have that file, the full text of the license is available at http://www.gnu.org/licenses/

## Spotted An Error ?

Please let us know by filing an issue on GitHub at https://github.com/NetworkedPlanet/charity-commission-data/issues