

class SchemaRegistry:
    """Registry for mapping between HTAN schema entities and FHIR resources"""

    def __init__(self, parser=None):
        self.parser = parser
        self.class_maps = {}  # HTAN class -> FHIR resource type
        self.field_maps = {}  # HTAN class -> {field -> FHIR path}
        self.field_metadata = {}  # HTAN class -> {field -> metadata}
        self.systems = {
            "htan": "https://data.humantumoratlas.org",
            "loinc": "http://loinc.org",
            "snomed": "http://snomed.info/sct"
        }
        self._init_defaults()

    def _init_defaults(self):
        """default mappings"""
        defaults = {
            "Assay": "ServiceRequest",
            "Biospecimen": "Specimen",
            "Patient": "Patient",
            "Diagnosis": "Condition",
            "File": "DocumentReference"
        }
        for htan_class, fhir_type in defaults.items():
            self.register_class(htan_class, fhir_type)

    def register_class(self, htan_class, fhir_type):
        """register a class mapping"""
        clean_class = self.parser._strip_prefix(htan_class) if self.parser else htan_class
        self.class_maps[clean_class] = fhir_type

    def get_fhir_type(self, htan_class):
        """get FHIR resource type for HTAN class"""
        clean_class = self.parser._strip_prefix(htan_class) if self.parser else htan_class

        if clean_class in self.class_maps:
            return self.class_maps[clean_class]

        if self.parser:
            parents = self.parser.get_parents(clean_class, recursive=True)
            for parent in parents:
                if parent in self.class_maps:
                    return self.class_maps[parent]

        return None

    def _normalize_field_name(self, field):
        """Normalize field names by removing all spaces"""
        return field.replace(" ", "").lower()

    def register_field(self, htan_class, field, fhir_path, metadata=None):
        """Register a field mapping with optional metadata"""
        clean_class = self.parser._strip_prefix(htan_class) if self.parser else htan_class

        if clean_class not in self.field_maps:
            self.field_maps[clean_class] = {}
            self.field_metadata[clean_class] = {}

        self.field_maps[clean_class][field] = fhir_path

        if metadata:
            self.field_metadata[clean_class][field] = metadata

        normalized = self._normalize_field_name(field)
        if clean_class not in self.field_maps:
            self.field_maps[clean_class] = {}
        self.field_maps[clean_class][f"__norm__{normalized}"] = field

    def get_field_path(self, htan_class, field):
        """get FHIR field path for HTAN field"""
        clean_class = self.parser._strip_prefix(htan_class) if self.parser else htan_class

        if clean_class not in self.field_maps:
            return None

        if field in self.field_maps[clean_class]:
            return self.field_maps[clean_class][field]

        normalized = self._normalize_field_name(field)
        norm_key = f"__norm__{normalized}"

        if norm_key in self.field_maps[clean_class]:
            original_field = self.field_maps[clean_class][norm_key]
            return self.field_maps[clean_class][original_field]

        return None

    def get_field_metadata(self, htan_class, field):
        """get metadata for a field mapping"""
        clean_class = self.parser._strip_prefix(htan_class) if self.parser else htan_class

        if clean_class not in self.field_metadata:
            return None

        if field in self.field_metadata[clean_class]:
            return self.field_metadata[clean_class][field]

        normalized = self._normalize_field_name(field)
        norm_key = f"__norm__{normalized}"

        if clean_class in self.field_maps and norm_key in self.field_maps[clean_class]:
            original_field = self.field_maps[clean_class][norm_key]
            if original_field in self.field_metadata[clean_class]:
                return self.field_metadata[clean_class][original_field]

        return None

    def export_schema_with_fhir_paths(self, output_path=None):
        """export enhanced schema with FHIR mappings"""
        import json

        enhanced_schema = {
            "@context": {},
            "@graph": []
        }

        if self.parser and hasattr(self.parser, 'schema'):
            enhanced_schema["@context"] = self.parser.schema.get("@context", {})

        for class_name, fhir_type in self.class_maps.items():
            node = {
                "@id": class_name,
                "@type": "rdfs:Class",
                "fhir:resourceType": fhir_type
            }

            if class_name in self.field_maps:
                field_mappings = []
                processed_norm_fields = set()

                for field, value in self.field_maps[class_name].items():
                    if field.startswith("__norm__"):
                        continue

                    normalized = self._normalize_field_name(field)
                    if normalized in processed_norm_fields:
                        continue

                    processed_norm_fields.add(normalized)

                    field_map = {
                        "field": field,
                        "fhir:path": value
                    }

                    if class_name in self.field_metadata and field in self.field_metadata[class_name]:
                        for key, value in self.field_metadata[class_name][field].items():
                            field_map[f"fhir:{key}"] = value

                    field_mappings.append(field_map)

                if field_mappings:
                    node["fhir:fieldMappings"] = field_mappings

            enhanced_schema["@graph"].append(node)

        if output_path:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(enhanced_schema, f, indent=2)

        return enhanced_schema

    def load_mapping_schema(self, schema_path):
        """load mappings from a schema with FHIR annotations"""
        import json

        try:
            with open(schema_path, 'r', encoding='utf-8') as f:
                schema = json.load(f)

            if "@graph" not in schema or not isinstance(schema["@graph"], list):
                print(f"Warning: Schema at {schema_path} has no valid @graph")
                return False

            loaded_count = 0

            for node in schema["@graph"]:
                class_name = node.get("@id")
                if not class_name:
                    continue

                if "fhir:resourceType" in node:
                    self.register_class(class_name, node["fhir:resourceType"])
                    loaded_count += 1

                if "fhir:fieldMappings" in node and isinstance(node["fhir:fieldMappings"], list):
                    for mapping in node["fhir:fieldMappings"]:
                        field = mapping.get("field")
                        path = mapping.get("fhir:path")

                        if field and path:
                            metadata = {}
                            for key, value in mapping.items():
                                if key.startswith("fhir:") and key != "fhir:path":
                                    metadata[key[5:]] = value

                            self.register_field(class_name, field, path, metadata if metadata else None)
                            loaded_count += 1

            print(f"Successfully loaded {loaded_count} mappings from schema")
            return True

        except Exception as e:
            print(f"Error loading mapping schema: {e}")
            return False

