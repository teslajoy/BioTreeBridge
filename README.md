# BioTreeBridge

BioTreeBridge is a data transformation tool that bridges the Human Tumor Atlas Network (HTAN) schema with FHIR healthcare standards. It seamlessly converts data between these two biomedical formats, while the integrated visualization tool renders HTAN's BioThings complex hierarchical structures as navigable tree views to assist in the transformation process.

## Features
- Bidirectional transformation between HTAN bioschemas and FHIR resources
- Support for mapping tumor atlas data elements to fast healthcare interoperability standards
- Interactive D3.js-based tree visualization of HTAN schema hierarchy
- Customizable visualization with focused views on specific branches
- Python-based data processing ETL pipeline for schema transformation

## Installation

```bash
# clone the repository
git clone https://github.com/yourusername/biotreebridge.git
cd biotreebridge

# create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate  # on Windows: venv\Scripts\activate

# install the package in development mode
pip install -e .
```

## Usage

### Downloading the Schema Locally
```bash
# download using curl
curl -s https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld > schema.json

# download using wget
wget -O schema.json https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld
```

### Generate Hierarchy JSON

Generate a hierarchical representation of the HTAN schema for visualization:

```bash
# generate a tree starting from the File node
biotreebridge schema tree --parent Assay

# include attributes in the output
biotreebridge schema tree --parent Assay --include-attributes

# limit the depth of the tree
biotreebridge schema tree --parent Assay --max-depth 2

# save to a custom file
biotreebridge schema tree --parent Assay --output assay_hierarchy.json
```

### Search for Schema Elements

```bash
# search for nodes containing "Assay"
biotreebridge schema search --term Assay

# search using the HTAN schema URL
biotreebridge schema search --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld --term File

# search and save results to a file
biotreebridge schema search --term Imaging --output imaging_nodes.json
```

### List All Root Nodes

```bash
# list all root nodes
biotreebridge schema roots

# list roots directly from the HTAN schema URL
biotreebridge schema roots --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld
```

## Visualization

After generating the hierarchy JSON, you can view the visualizations locally via ```python -m http.server 8000``` and your localhost:8000


## Purpose
BioTreeBridge facilitates interoperability between HTAN's cancer research and clinical healthcare systems, enabling researchers and clinicians to leverage human tumor atlas data within FHIR standard healthcare workflows while maintaining semantic integrity across systems.

## License

[MIT License](LICENSE)