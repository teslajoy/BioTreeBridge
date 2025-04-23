import re
import logging
from typing import Optional
from fhir.resources.patient import Patient
from fhir.resources.specimen import Specimen, SpecimenCollection
from fhir.resources.identifier import Identifier
from fhir.resources.extension import Extension
from fhir.resources.observation import Observation
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.documentreference import DocumentReference
from fhir.resources.group import Group, GroupMember
from fhir.resources.attachment import Attachment
from fhir.resources.bodystructure import BodyStructure, BodyStructureIncludedStructure
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.reference import Reference
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.condition import Condition, ConditionStage
from fhir.resources.substancedefinition import SubstanceDefinition, SubstanceDefinitionStructure, \
    SubstanceDefinitionStructureRepresentation, SubstanceDefinitionName
from fhir.resources.medication import Medication, MedicationIngredient
from fhir.resources.medicationadministration import MedicationAdministration, MedicationAdministrationDosage
from fhir.resources.resource import Resource
from fhir.resources.substance import Substance
from fhir.resources.quantity import Quantity
from fhir.resources.timing import Timing, TimingRepeat
from fhir.resources.range import Range

import uuid
from uuid import uuid3, uuid5, NAMESPACE_DNS

from biotreebridge.bridge import utils


class FHIRTransformer:
    """FHIR transformer class with common functionality"""

    def __init__(self, registry, subprogram_name: str | None, subproject_name: str | None, out_dir: str, verbose: bool):
        self.registry = registry
        self.subprogram_name = subprogram_name if subprogram_name else "HTAN_BForePC"
        self.project_id = "-".join([self.subprogram_name, subproject_name]) if subproject_name else utils.project_id
        self.namespace = utils.NAMESPACE_HTAN
        self.mint_id = utils.mint_id
        self._mint_id = utils._mint_id
        self.get_data_types = utils.get_data_types
        self.get_chembl_compund_info = utils.get_chembl_compound_info
        self.out_dir = out_dir
        self.verbose = verbose
        self.SYSTEM_HTAN = 'humantumoratlas.org'
        self.SYSTEM_SNOME = 'http://snomed.info/sct'
        self.SYSTEM_LOINC = 'http://loinc.org'
        self.SYSTEM_chEMBL = 'https://www.ebi.ac.uk/chembl'
        self.SYSTEM_UBERON = 'http://purl.obolibrary.org/obo/'
        self.NAMESPACE_HTAN = uuid3(NAMESPACE_DNS, self.SYSTEM_HTAN)
        self.lab_category = [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "laboratory",
                        "display": "laboratory"
                    }
                ],
                "text": "Laboratory"
            }
        ]
        self.med_admin_code = {
            "coding": [
                {
                    "system": "http://loinc.org",
                    "code": "80565-5",
                    "display": "Medication administration record"
                }
            ],
            "text": "Medication administration record"
        }
        # parent researchstudy of all HTAN sub-programs and sub-projects
        # possible subprogram https://github.com/ncihtan/htan2_project_setup/blob/main/projects.yml
        parent_researchstudy_identifier = Identifier(**{"system": self.SYSTEM_HTAN, "use": "official", "value": "HTAN"})
        parent_researchstudy_id = self.mint_id(identifier=parent_researchstudy_identifier,
                                               resource_type="ResearchStudy",
                                               project_id="HTAN", namespace=self.NAMESPACE_HTAN)
        self.program_research_study = ResearchStudy(**{"id": parent_researchstudy_id,
                                                       "identifier": [parent_researchstudy_identifier],
                                                       "name": "HTAN",
                                                       "status": "open"}) # todo: check status

        # todo: self.cases_mappings =
        # todo: self.file_mappings =
        # todo: self.biospecimen_mappings =

        # todo: most codeableConcepts are here https://github.com/ncihtan/phase2_clinical_data_model



    @staticmethod
    def is_valid_uuid(value: str) -> bool:
        if value is None:
            return False
        try:
            _obj = uuid.UUID(value, version=5)
        except ValueError:
            return False
        return True


    def map_field(self, source_entity, source_field, target_resource, value, url, use, other: dict):
        """map a single field using registry"""
        path = self.registry.get_field_path(source_entity, source_field)
        metadata = self.registry.get_field_metadata(source_entity, source_field)

        if not path:
            return

        # apply the mapping based on path pattern
        if "identifier" in path:
            self._map_identifier(target_resource, path, value, url, metadata)
        elif "extension" in path:
            self._map_extension(target_resource, path, value, metadata)
        else:
            self._map_simple_field(target_resource, path, value)

    def _map_identifier(self, resource, path, value, metadata):
        """map a field to a FHIR identifier"""
        if "identifier" not in resource:
            resource["identifier"] = []

        identifier = {"value": value, "url": metadata["url"], "use": metadata["use"]}
        if metadata:
            for key, meta_value in metadata.items():
                identifier[key] = meta_value

        resource["identifier"].append(identifier)

    def _map_extension(self, resource, path, value, metadata):
        """map a field to a FHIR extension"""
        if "extension" not in resource:
            resource["extension"] = []

        extension = {"valueString": value}
        if metadata and "url" in metadata:
            extension["url"] = metadata["url"]

        resource["extension"].append(extension)

    def _map_simple_field(self, resource, path, value):
        """map a field to a simple FHIR path"""
        path_parts = path.split('.')

        if len(path_parts) == 1:
            resource[path] = value
        else:
            current = resource
            for i, part in enumerate(path_parts):
                if i == len(path_parts) - 1:
                    current[part] = value
                else:
                    if part not in current:
                        current[part] = {}
                    current = current[part]


class PatientTransformer(FHIRTransformer):
    """patient transformer"""

    def transform(self, htan_patient_data):
        """Transform HTAN patient data to FHIR Patient resource"""
        fhir_patient = {
            "resourceType": self.registry.get_fhir_type("Patient")
        }

        # map standard fields using the registry
        # will have condition, observations, medicationadmin in child mapping fields of HTAN Patient
        for field, value in htan_patient_data.items():
            self.map_field("Patient", field, fhir_patient, value)

        # self._handle_vital_status(htan_patient_data, fhir_patient)

        return fhir_patient

    def _handle_vital_status(self, htan_data, fhir_patient):
        """special handling for vital status field"""
        if "Vital Status" in htan_data:
            status = htan_data["Vital Status"]
            # Convert string values to boolean
            if status.lower() in ["dead", "deceased"]:
                fhir_patient["deceasedBoolean"] = True
            elif status.lower() in ["alive", "living"]:
                fhir_patient["deceasedBoolean"] = False


class SpecimenTransformer(FHIRTransformer):
    """Specimen transformer"""

    def transform(self, htan_specimen_data, patient_id=None):
        """transform HTAN specimen data to FHIR Specimen resource"""
        fhir_specimen = {
            "resourceType": self.registry.get_fhir_type("Biospecimen")
        }

        # map standard fields using the registry
        for field, value in htan_specimen_data.items():
            self.map_field("Biospecimen", field, fhir_specimen, value)

        # link to patient if patient_id is provided
        if patient_id:
            fhir_specimen["subject"] = {
                "reference": f"Patient/{patient_id}"
            }

        return fhir_specimen