import os
import pytest
import tempfile
import pandas as pd
from biotreebridge.schema_parser.parser import BioThingsSchemaParser
from biotreebridge.bridge.registry import SchemaRegistry

import importlib.resources
from pathlib import Path

hierarchy_path = str(Path(importlib.resources.files('biotreebridge').parent / 'hierarchy.json'))

class TestSchemaRegistry:
    """test suite for SchemaRegistry class"""

    @pytest.fixture
    def schema_parser(self):
        """fixture to create a schema parser"""
        try:
            parser = BioThingsSchemaParser(hierarchy_path)
            return parser
        except Exception as e:
            pytest.skip(f"Could not initialize parser: {e}")

    @pytest.fixture
    def registry(self, schema_parser):
        """fixture to create a registry with default mappings"""
        registry = SchemaRegistry(schema_parser)
        return registry

    @pytest.fixture
    def populated_registry(self, registry):
        """fixture to create a registry with sample mappings"""
        registry.register_field("Patient", "HTANParticipantID", "identifier[0].value", {
            "system": "https://data.humantumoratlas.org/participant",
            "use": "official"
        })
        registry.register_field("Patient", "Gender", "gender")
        registry.register_field("Patient", "Race", "extension[0].valueString", {
            "url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"
        })
        registry.register_field("Patient", "VitalStatus", "deceasedBoolean")

        registry.register_field("Biospecimen", "HTANBiospecimenID", "identifier[0].value", {
            "system": "https://data.humantumoratlas.org/biospecimen"
        })
        registry.register_field("Biospecimen", "BiospecimenType", "type.coding[0].code")

        return registry

    def test_register_class(self, registry):
        """test registering a class mapping"""
        registry.register_class("Medication", "Medication")
        assert registry.get_fhir_type("Medication") == "Medication"

    def test_register_field(self, registry):
        """test registering a field mapping"""
        registry.register_field("Patient", "HTANParticipantID", "identifier[0].value")
        path = registry.get_field_path("Patient", "HTANParticipantID")
        assert path == "identifier[0].value"

    def test_register_field_with_metadata(self, registry):
        """test registering a field with metadata"""
        metadata = {"system": "http://example.org", "use": "official"}
        registry.register_field("Patient", "HTANParticipantID", "identifier[0].value", metadata)

        path = registry.get_field_path("Patient", "HTANParticipantID")
        metadata_result = registry.get_field_metadata("Patient", "HTANParticipantID")

        assert path == "identifier[0].value"
        assert metadata_result == metadata

    def test_field_name_mapping_camel_to_spaced(self, populated_registry):
        """test mapping between camelCase and space-separated field names"""
        assert "HTANParticipantID" in populated_registry.field_maps["Patient"]
        assert populated_registry.field_maps["Patient"]["HTANParticipantID"] == "identifier[0].value"

        path1 = populated_registry.get_field_path("Patient", "HTANParticipantID")
        path2 = populated_registry.get_field_path("Patient", "HTAN Participant ID")

        assert path1 == "identifier[0].value"
        assert path2 == "identifier[0].value"

        metadata1 = populated_registry.get_field_metadata("Patient", "HTANParticipantID")
        metadata2 = populated_registry.get_field_metadata("Patient", "HTAN Participant ID")

        expected_metadata = {"system": "https://data.humantumoratlas.org/participant", "use": "official"}
        assert metadata1 == expected_metadata
        assert metadata2 == expected_metadata

    def test_get_fhir_type(self, registry):
        """test getting FHIR resource type"""
        assert registry.get_fhir_type("Patient") == "Patient"
        assert registry.get_fhir_type("NonExistentClass") is None


    def test_export_and_load_schema(self, populated_registry):
        """test exporting and loading a schema with mappings"""
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
            temp_schema_path = tmp.name

        try:
            populated_registry.export_schema_with_fhir_paths(temp_schema_path)
            new_registry = SchemaRegistry(populated_registry.parser)
            result = new_registry.load_mapping_schema(temp_schema_path)
            assert result is True

            path = new_registry.get_field_path("Patient", "HTAN Participant ID")
            metadata = new_registry.get_field_metadata("Patient", "HTAN Participant ID")
            assert path == "identifier[0].value"
            assert metadata == {"system": "https://data.humantumoratlas.org/participant", "use": "official"}

            path_camel = new_registry.get_field_path("Patient", "HTANParticipantID")
            assert path_camel == "identifier[0].value"

        finally:
            if os.path.exists(temp_schema_path):
                os.unlink(temp_schema_path)

    def test_data_processing(self, populated_registry):
        """test processing HTAN data with field mappings"""
        patient_data = {
            "HTAN Participant ID": ["HTAN_123", "HTAN_456"],
            "Gender": ["male", "female"],
            "Race": ["White", "Asian"],
            "Vital Status": ["Alive", "Dead"]
        }
        patient_df = pd.DataFrame(patient_data)

        results = []
        for _, row in patient_df.iterrows():
            fhir_resource = {"resourceType": populated_registry.get_fhir_type("Patient")}

            for field, value in row.items():
                path = populated_registry.get_field_path("Patient", field)
                metadata = populated_registry.get_field_metadata("Patient", field)

                if path:
                    if "identifier" in path:
                        if "identifier" not in fhir_resource:
                            fhir_resource["identifier"] = []

                        identifier = {"value": value}
                        if metadata:
                            for meta_key, meta_value in metadata.items():
                                identifier[meta_key] = meta_value
                        fhir_resource["identifier"].append(identifier)

                    elif "extension" in path:
                        if "extension" not in fhir_resource:
                            fhir_resource["extension"] = []

                        extension = {"valueString": value}
                        if metadata and "url" in metadata:
                            extension["url"] = metadata["url"]

                        fhir_resource["extension"].append(extension)
                    else:
                        fhir_resource[path] = value

            results.append(fhir_resource)

        assert len(results) == 2

        assert results[0]["resourceType"] == "Patient"
        assert results[0]["identifier"][0]["value"] == "HTAN_123"
        assert results[0]["identifier"][0]["system"] == "https://data.humantumoratlas.org/participant"
        assert results[0]["identifier"][0]["use"] == "official"
        assert results[0]["gender"] == "male"
        assert results[0]["extension"][0]["valueString"] == "White"
        assert results[0]["extension"][0]["url"] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"

        assert results[1]["resourceType"] == "Patient"
        assert results[1]["identifier"][0]["value"] == "HTAN_456"
        assert results[1]["identifier"][0]["system"] == "https://data.humantumoratlas.org/participant"
        assert results[1]["identifier"][0]["use"] == "official"
        assert results[1]["gender"] == "female"
        assert results[1]["extension"][0]["valueString"] == "Asian"
        assert results[1]["extension"][0]["url"] == "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race"