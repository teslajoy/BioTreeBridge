⚠️ Note: This is the public skeleton version of the full project. 

BioTreeBridge transforms data between Human Tumor Atlas Network (HTAN) schema and FHIR healthcare standards. It converts biomedical data between these formats and visualizes HTAN's complex hierarchical structures as navigable tree views as a utility for registry mappings and transformers.

## Features
This system provides a complete bidirectional mapping between HTAN schema/data and FHIR resources. Key features include:

1. Schema Registry: Maps HTAN entities and fields to FHIR resources with bidirectional field name support
2. Transformers: Convert between formats with entity-specific logic (ex. Patient transformer)
3. Schema Enhancement: Exports HTAN schema with FHIR mapping annotations
4. Bidirectional Conversion: Supports both HTAN → FHIR and FHIR → HTAN
5. CLI Tools: Command-line utilities for schema exploration and data conversion

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

### Getting the HTAN Schema
Having a local instance helps to keep the schema version static 

```bash
# download using curl
curl -s https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld > schema.json

# download using wget
wget -O schema.json https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld
```

### Working with Schema 
#### 1. Generate Hierarchy JSON

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

#### 2. Search for Schema Elements

```bash
# search for nodes containing "Assay"
biotreebridge schema search --term Assay

# search using the HTAN schema URL
biotreebridge schema search --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld --term File

# search and save results to a file
biotreebridge schema search --term Imaging --output imaging_nodes.json
```

#### 3.List All Root Nodes

```bash # todo: fix root fetching
# list all root nodes
biotreebridge schema roots

# list roots directly from the HTAN schema URL
biotreebridge schema roots --source https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld
```

#### 4. Schema Visualization

After generating the hierarchy JSON, you can view the visualizations locally via ```python -m http.server 8000``` and your localhost:8000

### FHIR to HTAN mapping 
```bash
# create mapping template
biotreebridge fhir create-mapping-template

# apply mapping to schema
biotreebridge fhir apply-mapping

# add a single mapping
biotreebridge fhir add-mapping --node "Assay" --property "resourceType" --value "ServiceRequest"
```

#### Programmatic Use: 
```python 
from biotreebridge.bridge.registry import SchemaRegistry

# 1. load HTAN schema and create registry
registry = SchemaRegistry("schema.json")

# 2. create mapping template
registry.create_mapping_template("mappings.json")

# 3. manually edit the mapping template to add FHIR mappings

# 4. apply mapping template
registry.apply_mapping_template("mappings.json")

# 5. add FHIR properties directly
registry.add_fhir_property("Patient", "resourceType", "Patient")
registry.add_field_mapping("HTANParticipantID", {
    "path": "Patient.identifier.value",
    "system": "https://data.humantumoratlas.org/participant"
})

# 6. save updated HTAN schema with FHIR mappings 
registry.save_schema("schema_fhir.json")
```

### Data Transformation 

1. Parse and load schema 
2. Register mappings
3. Pass updated bidirectional schema mapping to Transformers 
4. Transform data

### Architecture

BioTreeBridge has three main components:

1. Schema Parser: Processes HTAN BioThings schema into navigable structures
2. Schema Registry: Maps between HTAN and FHIR data models
3. Transformers: Handle conversion with entity-specific logic

The system supports field name variations (camelCase vs. space-separated) and can export enhanced schemas with FHIR mapping annotations for reuse.


## Purpose
BioTreeBridge connects HTAN cancer research data with clinical healthcare systems using FHIR standards, enabling seamless data exchange while maintaining semantic integrity.

## License

[MIT License](LICENSE)
