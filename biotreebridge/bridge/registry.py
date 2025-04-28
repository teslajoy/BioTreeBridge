import json
from typing import Dict, List, Optional, Any, Tuple, Set


class SchemaRegistry:
    """Registry for managing FHIR properties in BioThings schema entities"""

    # for FHIR resource mapping for each item in HTAN schematic graph
    # if it maps to FHIR resoureType main entity will have completed possible mappings for:
    # fhir:resourceType
    # fhir:reference: [{fhir:resourceType: "Observation", other attributes of this reference}] # in cases where context of entity is required like Observation of (focus Patient)
    # fhir:validation: [ this could be variable across servers and fhir versions] # may leave this to g3t validator
    # ==========================
    # if it maps to a FHIR field will have completed possible mappings for:
    # fhir:resourceType # this would be the fhir entity this field belongs to
    # fhir:fieldMapping = [fhir:filed, fhir:label, fhir:use, fhir:url, other metadata that goes with this field]
    # may have type, required, cardinality allowed rules
    # these depend on the class and entity properties - human intervention is on mappings.json

    def __init__(self, schema_path=None, verbose=False):
        self.schema = None
        self.verbose = verbose
        self.allowed_properties = [
            "resourceType", "reference", "validation",
            "path", "system", "use", "type", "cardinality",
            "required", "profile", "version", "coding", "code",
            "description", "url", "relationship", "fieldMapping",
            "schema_subClassOf"
        ]
        if schema_path:
            self.load_schema(schema_path)

    def load_schema(self, schema_path: str) -> Dict:
        """Load a BioThings schema from file"""
        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                self.schema = json.load(f)
            if self.verbose:
                print(f"schema loaded from {schema_path}")
            return self.schema
        except Exception as e:
            raise ValueError(f"error loading schema: {str(e)}")

    def save_schema(self, output_path: str) -> None:
        """Save the BioThings schema to file"""
        if not self.schema:
            raise ValueError("no schema loaded")
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(self.schema, f, indent=2)
            if self.verbose:
                print(f"schema saved to {output_path}")
        except Exception as e:
            raise ValueError(f"error saving schema: {str(e)}")

    def ensure_fhir_context(self) -> None:
        """Ensure FHIR context is in the schema"""
        if not self.schema:
            raise ValueError("no schema loaded")
        if "@context" not in self.schema:
            self.schema["@context"] = {}
        if "fhir" not in self.schema["@context"]:
            self.schema["@context"]["fhir"] = "https://hl7.org/fhir/R5/"

    def find_node_by_id(self, node_id: str) -> Optional[Dict]:
        """Find a node in the schema by its @id"""
        if not self.schema or "@graph" not in self.schema:
            return None
        if node_id.startswith("bts:"):
            search_ids = [node_id, node_id[4:]]
        else:
            search_ids = [node_id, f"bts:{node_id}"]
        for node in self.schema["@graph"]:
            if "@id" in node and node["@id"] in search_ids:
                return node
        return None

    def check_valid_fhir_property(self, property_name: str) -> bool:
        """Check if property name is in allowed list"""
        if property_name.startswith("fhir:"):
            property_name = property_name[5:]
        valid = property_name in self.allowed_properties
        if not valid and self.verbose:
            print(f"warning: '{property_name}' is not a recognized fhir property")
        return valid

    def add_fhir_property(self, node_id: str, property_name: str, property_value: Any, update: bool = True) -> None:
        """Add or update a FHIR property to a node"""
        if not self.schema:
            raise ValueError("no schema loaded")
        if not self.check_valid_fhir_property(property_name):
            return
        self.ensure_fhir_context()
        node = self.find_node_by_id(node_id)
        if not node:
            if self.verbose:
                print(f"warning: node '{node_id}' not found in schema")
            return
        if not property_name.startswith("fhir:"):
            property_name = f"fhir:{property_name}"
        if property_name in node:
            if update:
                node[property_name] = property_value
                if self.verbose:
                    print(f"updated {property_name} for node '{node_id}'")
            elif self.verbose:
                print(f"property {property_name} already exists for node '{node_id}' and update=false")
        else:
            node[property_name] = property_value
            if self.verbose:
                print(f"added {property_name} to node '{node_id}'")

    def add_field_mapping(self, node_id: str, field_properties: Dict, update: bool = True) -> None:
        """Add a field mapping to a FHIR property"""
        if not self.schema:
            raise ValueError("no schema loaded")

        node = self.find_node_by_id(node_id)
        if not node:
            if self.verbose:
                print(f"warning: node '{node_id}' not found in schema")
            return

        if "fhir:resourceType" not in node:
            if self.verbose:
                print(f"warning: node '{node_id}' requires a resourceType before adding field mappings")
            return

        property_name = "fhir:fieldMapping"

        field_entry = {}
        for key, value in field_properties.items():
            if not key.startswith("fhir:"):
                key = f"fhir:{key}"
            field_entry[key] = value

        if property_name in node:
            if isinstance(node[property_name], list):
                found = False
                for i, mapping in enumerate(node[property_name]):
                    if mapping.get("fhir:path") == field_entry.get("fhir:path"):
                        if update:
                            node[property_name][i] = field_entry
                            found = True
                            if self.verbose:
                                print(f"updated field mapping for path '{field_entry.get('fhir:path')}' in node '{node_id}'")
                        else:
                            found = True
                            if self.verbose:
                                print(f"field mapping for path '{field_entry.get('fhir:path')}' already exists and update=false")

                if not found:
                    node[property_name].append(field_entry)
                    if self.verbose:
                        print(f"added new field mapping for path '{field_entry.get('fhir:path')}' to node '{node_id}'")
            else:
                node[property_name] = [field_entry]
                if self.verbose:
                    print(f"converted fieldMapping to list and added mapping for node '{node_id}'")
        else:
            node[property_name] = [field_entry]
            if self.verbose:
                print(f"added new fieldMapping property to node '{node_id}'")

    def remove_field_mapping(self, node_id: str, field_path: str = None) -> None:
        """Remove a field mapping by path or all field mappings"""
        if not self.schema:
            raise ValueError("no schema loaded")

        node = self.find_node_by_id(node_id)
        if not node:
            if self.verbose:
                print(f"warning: node '{node_id}' not found in schema")
            return

        property_name = "fhir:fieldMapping"

        if property_name not in node:
            if self.verbose:
                print(f"no field mappings found in node '{node_id}'")
            return

        if field_path:
            if not isinstance(node[property_name], list):
                if self.verbose:
                    print(f"fieldMapping for node '{node_id}' is not a list")
                return

            original_length = len(node[property_name])
            node[property_name] = [
                mapping for mapping in node[property_name]
                if mapping.get("fhir:path") != field_path
            ]

            if len(node[property_name]) < original_length:
                if self.verbose:
                    print(f"removed field mapping for path '{field_path}' from node '{node_id}'")
            else:
                if self.verbose:
                    print(f"field mapping for path '{field_path}' not found in node '{node_id}'")

            if len(node[property_name]) == 0:
                del node[property_name]
                if self.verbose:
                    print(f"removed empty fieldMapping property from node '{node_id}'")
        else:
            del node[property_name]
            if self.verbose:
                print(f"removed all field mappings from node '{node_id}'")

    def get_field_mappings(self, node_id: str) -> List[Dict]:
        """Get all field mappings for a node"""
        if not self.schema:
            raise ValueError("no schema loaded")

        node = self.find_node_by_id(node_id)
        if not node:
            if self.verbose:
                print(f"warning: node '{node_id}' not found in schema")
            return []

        property_name = "fhir:fieldMapping"

        if property_name not in node:
            return []

        if isinstance(node[property_name], list):
            return node[property_name]
        else:
            if self.verbose:
                print(f"warning: fieldMapping for node '{node_id}' is not a list")
            return []

    def remove_fhir_property(self, node_id: str, property_name: str = None) -> None:
        """Remove a FHIR property from a node"""
        if not self.schema:
            raise ValueError("no schema loaded")
        node = self.find_node_by_id(node_id)
        if not node:
            if self.verbose:
                print(f"warning: node '{node_id}' not found in schema")
            return
        if property_name:
            if not property_name.startswith("fhir:"):
                property_name = f"fhir:{property_name}"
            if property_name in node:
                del node[property_name]
                if self.verbose:
                    print(f"removed {property_name} from node '{node_id}'")
            elif self.verbose:
                print(f"property {property_name} not found in node '{node_id}'")
        else:
            removed = 0
            for key in list(node.keys()):
                if key.startswith("fhir:"):
                    del node[key]
                    removed += 1
            if self.verbose and removed > 0:
                print(f"removed {removed} fhir properties from node '{node_id}'")
            elif self.verbose:
                print(f"no fhir properties found in node '{node_id}'")

    def get_fhir_property(self, node_id: str, property_name: str) -> Optional[Any]:
        """Get a FHIR property from a node"""
        if not self.schema:
            raise ValueError("no schema loaded")
        node = self.find_node_by_id(node_id)
        if not node:
            if self.verbose:
                print(f"warning: node '{node_id}' not found in schema")
            return None
        if not property_name.startswith("fhir:"):
            property_name = f"fhir:{property_name}"
        return node.get(property_name)

    def list_fhir_properties(self, node_id: str) -> Dict:
        """List all FHIR properties of a node"""
        if not self.schema:
            raise ValueError("no schema loaded")
        node = self.find_node_by_id(node_id)
        if not node:
            if self.verbose:
                print(f"warning: node '{node_id}' not found in schema")
            return {}
        return {k: v for k, v in node.items() if k.startswith("fhir:")}

    def get_subclass_relationship(self, node_id: str) -> List[str]:
        """Get direct subClassOf relationships for a node"""
        node = self.find_node_by_id(node_id)
        if not node or "rdfs:subClassOf" not in node:
            return []
        parent_classes = []
        subclass_info = node["rdfs:subClassOf"]
        if isinstance(subclass_info, list):
            for parent in subclass_info:
                if isinstance(parent, dict) and "@id" in parent:
                    parent_classes.append(parent["@id"])
        elif isinstance(subclass_info, dict) and "@id" in subclass_info:
            parent_classes.append(subclass_info["@id"])
        elif isinstance(subclass_info, str):
            parent_classes.append(subclass_info)
        return parent_classes

    def list_all_fhir_mappings(self) -> Dict:
        """List all FHIR mappings in the schema grouped by fhir property"""
        if not self.schema or "@graph" not in self.schema:
            return {}
        mappings = {}
        for node in self.schema["@graph"]:
            if "@id" not in node:
                continue
            node_id = node["@id"]
            for key, value in node.items():
                if key.startswith("fhir:"):
                    if key not in mappings:
                        mappings[key] = {}
                    mappings[key][node_id] = value
        return mappings

    def bulk_add_mappings(self, mappings: List[Dict]) -> None:
        """Add multiple FHIR mappings in bulk"""
        if not self.schema:
            raise ValueError("no schema loaded")
        for mapping in mappings:
            node_id = mapping.get("node_id")
            property_name = mapping.get("property")
            value = mapping.get("value")
            update = mapping.get("update", True)
            if not node_id or not property_name or value is None:
                if self.verbose:
                    print(f"warning: skipping invalid mapping: {mapping}")
                continue
            self.add_fhir_property(node_id, property_name, value, update)

    def bulk_add_field_mappings(self, field_mappings: List[Dict]) -> None:
        """Add multiple field mappings in bulk"""
        if not self.schema:
            raise ValueError("no schema loaded")
        for mapping in field_mappings:
            node_id = mapping.get("node_id")
            field_properties = mapping.get("field_properties")
            update = mapping.get("update", True)
            if not node_id or not field_properties:
                if self.verbose:
                    print(f"warning: skipping invalid field mapping: {mapping}")
                continue
            self.add_field_mapping(node_id, field_properties, update)

    def bulk_remove_mappings(self, removals: List[Dict]) -> None:
        """Remove multiple FHIR mappings in bulk"""
        if not self.schema:
            raise ValueError("no schema loaded")
        for removal in removals:
            node_id = removal.get("node_id")
            property_name = removal.get("property")
            if not node_id:
                if self.verbose:
                    print(f"warning: skipping invalid removal: {removal}")
                continue
            self.remove_fhir_property(node_id, property_name)

    def bulk_remove_field_mappings(self, removals: List[Dict]) -> None:
        """Remove multiple field mappings in bulk"""
        if not self.schema:
            raise ValueError("no schema loaded")
        for removal in removals:
            node_id = removal.get("node_id")
            field_path = removal.get("field_path")
            if not node_id:
                if self.verbose:
                    print(f"warning: skipping invalid field mapping removal: {removal}")
                continue
            self.remove_field_mapping(node_id, field_path)

    def create_mapping_template(self, output_path: str) -> Tuple[List[Dict], Set[str]]:
        """Create a mapping template excluding schema:rangeIncludes values"""
        if not self.schema:
            raise ValueError("no schema loaded")

        range_includes_values = set()
        if "@graph" in self.schema:
            for node in self.schema["@graph"]:
                if "schema:rangeIncludes" in node:
                    range_values = node["schema:rangeIncludes"]
                    if isinstance(range_values, list):
                        for range_val in range_values:
                            if isinstance(range_val, dict) and "@id" in range_val:
                                range_includes_values.add(range_val["@id"])
                    elif isinstance(range_values, dict) and "@id" in range_values:
                        range_includes_values.add(range_values["@id"])

        property_range_values = {}
        if "@graph" in self.schema:
            for node in self.schema["@graph"]:
                if "@id" not in node:
                    continue

                node_id = node["@id"]
                if "schema:rangeIncludes" in node:
                    range_values = []
                    range_data = node["schema:rangeIncludes"]

                    if isinstance(range_data, list):
                        for val in range_data:
                            if isinstance(val, dict) and "@id" in val:
                                range_values.append(val["@id"])
                    elif isinstance(range_data, dict) and "@id" in range_data:
                        range_values.append(range_data["@id"])

                    if range_values:
                        property_range_values[node_id] = range_values

        subclass_relations = {}
        for node in self.schema.get("@graph", []):
            if "@id" not in node:
                continue

            node_id = node["@id"]

            if "rdfs:subClassOf" in node:
                subclass_info = node["rdfs:subClassOf"]

                if isinstance(subclass_info, list):
                    parent_classes = []
                    for parent in subclass_info:
                        if isinstance(parent, dict) and "@id" in parent:
                            parent_classes.append(parent["@id"])
                        elif isinstance(parent, str):
                            parent_classes.append(parent)
                    if parent_classes:
                        subclass_relations[node_id] = parent_classes[0]
                elif isinstance(subclass_info, dict) and "@id" in subclass_info:
                    subclass_relations[node_id] = subclass_info["@id"]
                elif isinstance(subclass_info, str):
                    subclass_relations[node_id] = subclass_info

        mappings = []
        if "@graph" in self.schema:
            for node in self.schema["@graph"]:
                if "@id" not in node:
                    continue

                node_id = node["@id"]
                if node_id in range_includes_values:
                    continue

                htan_subclass = subclass_relations.get(node_id, "")
                range_values = property_range_values.get(node_id, [])

                mapping_entry = {
                    "node": node_id,
                    "fhir:resourceType": "",
                    "fhir:reference": [{"fhir:resourceType": "", "focus": ""}],
                    "fhir:validation": [{"fhir:type": "", "fhir:required": "", "fhir:cardinality": ""}],
                    "fhir:fieldMapping": [{"fhir:filed": ""}],
                    "rdfs:subClassOf": htan_subclass,
                    "fhir:schema_subClassOf": "",
                    "range_values": range_values
                }

                mappings.append(mapping_entry)

        def format_json(obj, indent=0):
            if isinstance(obj, dict):
                items = []
                for k, v in obj.items():
                    if k in ["fhir:reference", "fhir:validation", "fhir:fieldMapping"] and isinstance(v, list):
                        array_str = json.dumps(v)
                        items.append(f'{" " * (indent + 2)}"{k}": {array_str}')
                    else:
                        value_str = format_json(v, indent + 2)
                        items.append(f'{" " * (indent + 2)}"{k}": {value_str}')
                return "{\n" + ",\n".join(items) + "\n" + (" " * indent) + "}"
            elif isinstance(obj, list):
                if len(obj) == 0:
                    return "[]"
                if all(isinstance(x, (str, int, float, bool)) or x is None for x in obj):
                    return json.dumps(obj)
                else:
                    items = [format_json(item, indent + 2) for item in obj]
                    return "[\n" + ",\n".join(f"{' ' * (indent + 2)}{item}" for item in items) + "\n" + (" " * indent) + "]"
            else:
                return json.dumps(obj)

        with open(output_path, 'w') as f:
            json_str = "[\n"
            for i, mapping in enumerate(mappings):
                formatted = format_json(mapping, 2)
                if i < len(mappings) - 1:
                    json_str += f"  {formatted},\n"
                else:
                    json_str += f"  {formatted}\n"
            json_str += "]"
            f.write(json_str)

        if self.verbose:
            print(f"Created mapping template with {len(mappings)} nodes at {output_path}")
            print(f"Excluded {len(range_includes_values)} schema:rangeIncludes values")

        return mappings, range_includes_values

    def apply_mapping_template(self, mapping_path: str) -> None:
        """Apply mappings from a mapping template to the schema"""
        if not self.schema:
            raise ValueError("no schema loaded")

        try:
            with open(mapping_path, 'r') as f:
                mappings = json.load(f)

            if self.verbose:
                print(f"Loaded {len(mappings)} mappings from {mapping_path}")
        except Exception as e:
            raise ValueError(f"Error loading mapping template: {str(e)}")

        for mapping in mappings:
            node_id = mapping.get("node")
            if not node_id:
                if self.verbose:
                    print("Warning: Skipping mapping without node ID")
                continue

            resource_type = mapping.get("fhir:resourceType")
            if resource_type:
                self.add_fhir_property(node_id, "resourceType", resource_type)

            reference = mapping.get("fhir:reference")
            if reference and isinstance(reference, list) and len(reference) > 0:
                if any(isinstance(ref, dict) and ref.get("fhir:resourceType") for ref in reference):
                    self.add_fhir_property(node_id, "reference", reference)

            validation = mapping.get("fhir:validation")
            if validation and isinstance(validation, list) and len(validation) > 0:
                if any(isinstance(val, dict) for val in validation):
                    self.add_fhir_property(node_id, "validation", validation)

            field_mappings = mapping.get("fhir:fieldMapping")
            if field_mappings and isinstance(field_mappings, list) and len(field_mappings) > 0:
                for field_mapping in field_mappings:
                    if isinstance(field_mapping, dict) and any(field_mapping.values()):
                        self.add_field_mapping(node_id, field_mapping)

            fhir_subclass = mapping.get("fhir:schema_subClassOf")
            if fhir_subclass:
                self.add_fhir_property(node_id, "schema_subClassOf", fhir_subclass)

        if self.verbose:
            print("Mapping template applied to schema")