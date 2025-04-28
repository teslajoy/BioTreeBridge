import os
import pytest
import tempfile
import json
from biotreebridge.bridge.registry import SchemaRegistry

class TestSchemaRegistry:
    @pytest.fixture
    def sample_schema(self):
        schema = {
            "@context": {
                "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
                "schema": "http://schema.org/",
                "bts": "http://biothinkg.org/schema/"
            },
            "@graph": [
                {
                    "@id": "Patient",
                    "rdfs:label": "Patient",
                    "rdfs:comment": "A person receiving healthcare services"
                },
                {
                    "@id": "HTANParticipantID",
                    "rdfs:label": "HTAN Participant ID",
                    "rdfs:comment": "The identifier for a participant in HTAN"
                },
                {
                    "@id": "Gender",
                    "rdfs:label": "Gender",
                    "rdfs:comment": "The gender of the patient"
                },
                {
                    "@id": "Biospecimen",
                    "rdfs:label": "Biospecimen",
                    "rdfs:comment": "A biological sample"
                }
            ]
        }
        temp_fd, temp_path = tempfile.mkstemp(suffix='.json')
        os.close(temp_fd)
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(schema, f, indent=2)
        return temp_path

    @pytest.fixture
    def registry(self, sample_schema):
        return SchemaRegistry(schema_path=sample_schema, verbose=True)

    @pytest.fixture
    def populated_registry(self, registry):
        registry.add_fhir_property("Patient", "resourceType", "Patient")
        registry.add_fhir_property("HTANParticipantID", "resourceType", "Patient")
        registry.add_field_mapping("HTANParticipantID", {
            "path": "Patient.identifier.value",
            "system": "https://data.humantumoratlas.org/participant",
            "use": "official"
        })
        registry.add_fhir_property("Gender", "resourceType", "Patient")
        registry.add_field_mapping("Gender", {
            "path": "Patient.gender"
        })
        registry.add_fhir_property("Biospecimen", "resourceType", "Specimen")
        return registry

    def test_load_schema(self, registry, sample_schema):
        assert registry.schema is not None
        assert "@graph" in registry.schema
        assert len(registry.schema["@graph"]) == 4

    def test_add_fhir_property(self, registry):
        registry.add_fhir_property("Patient", "resourceType", "Patient")
        property_value = registry.get_fhir_property("Patient", "resourceType")
        assert property_value == "Patient"

    def test_add_field_mapping(self, registry):
        registry.add_fhir_property("HTANParticipantID", "resourceType", "Patient")
        registry.add_field_mapping("HTANParticipantID", {
            "path": "Patient.identifier.value",
            "system": "https://data.humantumoratlas.org/participant"
        })
        mappings = registry.get_field_mappings("HTANParticipantID")
        assert len(mappings) == 1
        assert mappings[0]["fhir:path"] == "Patient.identifier.value"
        assert mappings[0]["fhir:system"] == "https://data.humantumoratlas.org/participant"

    def test_remove_fhir_property(self, populated_registry):
        populated_registry.remove_fhir_property("Patient", "resourceType")
        property_value = populated_registry.get_fhir_property("Patient", "resourceType")
        assert property_value is None

    def test_remove_field_mapping(self, populated_registry):
        populated_registry.remove_field_mapping("HTANParticipantID", "Patient.identifier.value")
        mappings = populated_registry.get_field_mappings("HTANParticipantID")
        assert len(mappings) == 0

    def test_list_fhir_properties(self, populated_registry):
        properties = populated_registry.list_fhir_properties("Patient")
        assert "fhir:resourceType" in properties
        assert properties["fhir:resourceType"] == "Patient"

    def test_list_all_fhir_mappings(self, populated_registry):
        all_mappings = populated_registry.list_all_fhir_mappings()
        assert "fhir:resourceType" in all_mappings
        assert "Patient" in all_mappings["fhir:resourceType"]
        assert all_mappings["fhir:resourceType"]["Patient"] == "Patient"

    def test_create_mapping_template(self, registry):
        temp_fd, mapping_path = tempfile.mkstemp(suffix='.json')
        os.close(temp_fd)
        try:
            mappings, range_values = registry.create_mapping_template(mapping_path)
            assert len(mappings) == 4
            assert len(range_values) == 0
            with open(mapping_path, 'r', encoding='utf-8') as f:
                loaded_mappings = json.load(f)
            assert len(loaded_mappings) == 4
            assert loaded_mappings[0]["node"] in ["Patient", "HTANParticipantID", "Gender", "Biospecimen"]
            assert "fhir:resourceType" in loaded_mappings[0]
            assert "fhir:reference" in loaded_mappings[0]
            assert "fhir:validation" in loaded_mappings[0]
            assert "fhir:fieldMapping" in loaded_mappings[0]
        finally:
            if os.path.exists(mapping_path):
                os.unlink(mapping_path)

    def test_apply_mapping_template(self, registry):
        temp_fd, mapping_path = tempfile.mkstemp(suffix='.json')
        os.close(temp_fd)
        try:
            mapping_content = [
                {
                    "node": "Patient",
                    "fhir:resourceType": "Patient",
                    "fhir:reference": [],
                    "fhir:validation": [],
                    "fhir:fieldMapping": [],
                    "rdfs:subClassOf": "",
                    "fhir:schema_subClassOf": "",
                    "range_values": []
                },
                {
                    "node": "HTANParticipantID",
                    "fhir:resourceType": "Patient",
                    "fhir:reference": [],
                    "fhir:validation": [],
                    "fhir:fieldMapping": [{"fhir:path": "Patient.identifier.value"}],
                    "rdfs:subClassOf": "",
                    "fhir:schema_subClassOf": "",
                    "range_values": []
                }
            ]
            with open(mapping_path, 'w', encoding='utf-8') as f:
                json.dump(mapping_content, f, indent=2)
            registry.apply_mapping_template(mapping_path)
            patient_resource_type = registry.get_fhir_property("Patient", "resourceType")
            assert patient_resource_type == "Patient"
            htan_id_resource_type = registry.get_fhir_property("HTANParticipantID", "resourceType")
            assert htan_id_resource_type == "Patient"
            field_mappings = registry.get_field_mappings("HTANParticipantID")
            assert len(field_mappings) == 1
            assert field_mappings[0]["fhir:path"] == "Patient.identifier.value"
        finally:
            if os.path.exists(mapping_path):
                os.unlink(mapping_path)

    def test_save_schema(self, populated_registry):
        temp_fd, output_path = tempfile.mkstemp(suffix='.json')
        os.close(temp_fd)
        try:
            populated_registry.save_schema(output_path)
            with open(output_path, 'r', encoding='utf-8') as f:
                saved_schema = json.load(f)
            assert "@graph" in saved_schema
            assert len(saved_schema["@graph"]) == 4
            patient_node = None
            for node in saved_schema["@graph"]:
                if node["@id"] == "Patient":
                    patient_node = node
                    break
            assert patient_node is not None
            assert "fhir:resourceType" in patient_node
            assert patient_node["fhir:resourceType"] == "Patient"
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def teardown_method(self, method):
        pass