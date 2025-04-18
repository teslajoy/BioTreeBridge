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

### Generate Hierarchy JSON

Generate a hierarchical representation of the HTAN schema for visualization:

```bash
# generate the full HTAN schema hierarchy
biotreebridge schema tree --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld

# generate just the Assay branch graph 
biotreebridge schema tree --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld --parent "Assay"
```

### Search for Schema Elements

```bash
# search for nodes with "Assay" in the name
biotreebridge schema search --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld --term "Assay"

biotreebridge schema search --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld --term "Assay" --output assay_nodes.json
```

### List All Root Nodes

```bash
biotreebridge schema roots --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld
```

## Visualization

After generating the hierarchy JSON, you can view the visualizations @ https://teslajoy.github.io/biotreebridge/


## Purpose
BioTreeBridge facilitates interoperability between HTAN's cancer research and clinical healthcare systems, enabling researchers and clinicians to leverage human tumor atlas data within FHIR standard healthcare workflows while maintaining semantic integrity across systems.

## License

[MIT License](LICENSE)