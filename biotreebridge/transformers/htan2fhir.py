import uuid
import json
import warnings

import numpy as np
import orjson
import mimetypes
import copy
import glob
import pathlib
import inflection
import itertools
import pandas as pd
from . import utils
from pathlib import Path
import importlib.resources
from uuid import uuid3, NAMESPACE_DNS
from typing import Any, List, Optional
from datetime import datetime, timezone

from fhir.resources.reference import Reference
from fhir.resources.identifier import Identifier
from fhir.resources.patient import Patient
from fhir.resources.address import Address
from fhir.resources.researchstudy import ResearchStudy
from fhir.resources.researchsubject import ResearchSubject
from fhir.resources.observation import Observation
from fhir.resources.encounter import Encounter
from fhir.resources.codeableconcept import CodeableConcept
from fhir.resources.codeablereference import CodeableReference
from fhir.resources.age import Age
from fhir.resources.procedure import Procedure
from fhir.resources.bodystructure import BodyStructure, BodyStructureIncludedStructure
from fhir.resources.specimen import Specimen, SpecimenProcessing, SpecimenCollection
from fhir.resources.condition import Condition, ConditionStage
from fhir.resources.documentreference import DocumentReference, DocumentReferenceContent, \
    DocumentReferenceContentProfile, DocumentReferenceRelatesTo
from fhir.resources.attachment import Attachment
from fhir.resources.timing import Timing
from fhir.resources.medicationadministration import MedicationAdministration
from fhir.resources.medication import Medication, MedicationIngredient
from fhir.resources.substance import Substance, SubstanceIngredient
from fhir.resources.substancedefinition import SubstanceDefinition, SubstanceDefinitionStructure, \
    SubstanceDefinitionStructureRepresentation, SubstanceDefinitionName
from fhir.resources.group import Group, GroupMember
from fhir.resources.timing import Timing, TimingRepeat
from fhir.resources.range import Range
from fhir.resources.quantity import Quantity


# File data on synapse after authentication
# https://github.com/Sage-Bionetworks/synapsePythonClient?tab=readme-ov-file#store-a-file-to-synapse


class HTANTransformer:
    def __init__(self, subprogram_name: str, out_dir: str, verbose: bool):
        self.mint_id = utils.mint_id
        self._mint_id = utils._mint_id
        self.get_data_type = utils.get_data_types
        self.get_component = utils.get_component
        self.fhir_ndjson = utils.fhir_ndjson
        self.get_chembl_compound_info = utils.get_chembl_compound_info
        self.subprogram_name = subprogram_name
        self.project_id = subprogram_name  # incase there will be more granular project/program relations
        assert Path(out_dir).is_dir(), f"Path to out_dir {out_dir} is not a directory."
        self.out_dir = out_dir
        self.verbose = verbose
        self.SYSTEM_HTAN = 'https://data.humantumoratlas.org'
        self.SYSTEM_SNOME = 'http://snomed.info/sct'
        self.SYSTEM_LOINC = 'http://loinc.org'
        self.SYSTEM_chEMBL = 'https://www.ebi.ac.uk/chembl'
        self.NAMESPACE_HTAN = uuid3(NAMESPACE_DNS, self.SYSTEM_HTAN)
        self.read_json = utils._read_json
        self.fhir_ndjson = utils.fhir_ndjson
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
        parent_researchstudy_identifier = Identifier(**{"system": self.SYSTEM_HTAN, "use": "official", "value": "HTAN"})
        parent_researchstudy_id = self.mint_id(identifier=parent_researchstudy_identifier,
                                               resource_type="ResearchStudy",
                                               project_id="HTAN", namespace=self.NAMESPACE_HTAN)
        self.program_research_study = ResearchStudy(**{"id": parent_researchstudy_id,
                                                       "identifier": [parent_researchstudy_identifier],
                                                       "name": "HTAN",
                                                       "status": "open"})

        self.project_path = str(
            Path(importlib.resources.files('fhirizer').parent / 'projects' / 'HTAN' / subprogram_name))
        assert Path(self.project_path).is_dir(), f"Path {self.project_path} is not a valid directory path."

        self.cases_path = str(
            Path(importlib.resources.files('fhirizer').parent / 'resources' / 'htan_resources' / 'cases.json'))
        assert Path(self.cases_path).is_file(), f"Path {self.cases_path} does not exist."

        self.biospecimens_path = str(
            Path(importlib.resources.files('fhirizer').parent / 'resources' / 'htan_resources' / 'biospecimens.json'))
        assert Path(self.biospecimens_path).is_file(), f"Path {self.biospecimens_path} does not exist."

        self.files_path = str(
            Path(importlib.resources.files('fhirizer').parent / 'resources' / 'htan_resources' / 'files.json'))
        assert Path(self.files_path).is_file(), f"Path {self.files_path} does not exist."

        self.cases_mappings = self.get_cases_mappings

        # cases_mappings
        # https://data.humantumoratlas.org/standard/clinical
        # cases to Patient / ResearchSubject / ResearchStudy / Observation -> Condition / Medication / MedicationAdministration / Procedure / Encounter
        # 'HTAN Participant ID':  #NOTE:  HTAN ID associated with a patient based on HTAN ID SOP
        # 'Therapeutic Agents':  #NOTE: Some have multiple comma-separated Medication.ingredient
        self.cases_table_data_path = Path(Path(self.project_path).parent / subprogram_name).joinpath(
            "./raw/cases/table_data.tsv")
        assert self.cases_table_data_path.is_file(), f"Path {self.cases_table_data_path} is not a valid file path."
        self.cases = self.get_dataframe(self.cases_table_data_path, sep="\t")
        self.patient_identifier_field = "HTAN Participant ID"  # identifiers of the cases matrix/df

        self.biospecimen_mappings = self.get_biospecimen_mappings

        # biospecimens_mapping
        # biospecimens to Specimen / Observation -> Specimen
        # 'HTAN Parent ID': #NOTE: Parent could be another biospecimen or a research participant. # check for participant id for type of reference
        # 'Biospecimen Type': #NOTE: Doesn't seem informative
        self.biospecimens_table_data_path = Path(Path(self.project_path).parent / subprogram_name).joinpath(
            "./raw/biospecimens/table_data.tsv")
        assert self.biospecimens_table_data_path.is_file(), f"Path {self.biospecimens_table_data_path} is not a valid file path."
        self.biospecimens = self.get_dataframe(self.biospecimens_table_data_path, sep="\t")
        self.biospecimen_identifier_field = "HTAN Biospecimen ID"

        self.files_mappings = self.get_files_mappings

        # files_mapping
        # files to DocumentReference / Attachment / Observation -> DocumentReference

        self.files_table_data_path = Path(Path(self.project_path).parent / subprogram_name).joinpath(
            "./raw/files/table_data.tsv")
        self.files_drs_uri_path = Path(Path(self.project_path).parent / subprogram_name).joinpath(
            "./raw/files/cds_manifest.csv")
        assert self.files_table_data_path.is_file(), f"Path {self.files_table_data_path} is not a valid file path."
        assert self.files_drs_uri_path.is_file(), f"Path {self.files_drs_uri_path} is not a valid file path."

        self.files = self.get_dataframe(self.files_table_data_path, sep="\t")
        self.files_drs_uri = pd.read_csv(self.files_drs_uri_path, sep=",")

        self.patient_demographics = self.get_patient_demographics()

        # combine and create standard fhir files metadata
        # print(self.files["Filename"].str.split('/')[1])
        self.files = self.files[self.files["Filename"].str.contains(
            '.')]  # NOTE: HTAPP contains file names ex. HTA1_982_7629309080080, that do not have any metadata
        self.files = self.files[self.files["Filename"].str.contains('/')]

        self.files['mime_type'] = self.files["Filename"].apply(lambda x: mimetypes.guess_type(x)[0])
        self.files['name'] = self.files["Filename"].str.split('/').apply(lambda x: x[1])
        self.files_drs_meta = self.files.merge(self.files_drs_uri, how="left", on="name")

    def get_cases_mappings(self) -> dict:
        """HTAN cases FHIR mapping"""
        return self.read_json(self.cases_path)

    def get_biospecimen_mappings(self) -> dict:
        """HTAN biospesimens FHIR mapping"""
        return self.read_json(self.biospecimens_path)

    def get_files_mappings(self) -> dict:
        """HTAN files FHIR mapping"""
        return self.read_json(self.files_path)

    @staticmethod
    def get_dataframe(_path, sep) -> pd.DataFrame:
        """Returns a Pandas DataFrame with lower-case and inflection.underscore columns for standard UI input"""
        _data = pd.read_csv(_path, sep=sep)
        # _data.columns = _data.columns.to_series().apply(lambda x: inflection.underscore(inflection.parameterize(x)))
        return _data

    def get_patient_demographics(self) -> pd.DataFrame:
        """HTAN cases table_data.tsv data with Patient FHIR demographics mappings column/field match"""
        field_list = []
        for field in self.get_htan_mapping(match='Patient', field_maps=self.cases_mappings(), map_info='fhir_map',
                                           fetch='field'):
            field_list.append(field)
            if self.verbose:
                print(f"field name': {field}")

        patient_demographics = self.cases[field_list]
        return patient_demographics

    @staticmethod
    def get_htan_mapping(match, field_maps, map_info, fetch):
        """Yields FHIR HTAN maps from HTAN field or FHIR mapping string"""
        for field, mappings in field_maps.items():
            assert isinstance(mappings, list), f"HTAN resource mappings is not a list: {type(mappings)}, {mappings}"
            for entry_map in mappings:
                if entry_map[map_info] and match in entry_map[map_info]:
                    if fetch == "field":
                        yield field
                        break
                    elif fetch == "mapping":
                        yield entry_map
                        break

    @staticmethod
    def get_fields_by_fhir_map(mapping_data, fhir_mapping=None):
        """
        Yields the field(s) associated with a specific HTAN FHIR map or all HTAN FHIR maps

        Return: Yields the field, FHIR map, identifier use, and focus.
            example:
                for field, fhir_map, use, focus in get_fields_by_fhir_map(cases_mapping, "Observation.component"):
                    print(f"Field: {field}, FHIR Map: {fhir_map}, Identifier use: {use}, Focus: {focus}")
        """
        for _field, mappings in mapping_data.items():
            for mapping in mappings:
                _current_fhir_map = mapping["fhir_map"]
                _focus = mapping.get("focus", None)
                _use = mapping.get("use", None)

                if fhir_mapping is None or _current_fhir_map == fhir_mapping:
                    yield _field, _current_fhir_map, _use, _focus

    @staticmethod
    def get_fhir_maps_by_field(mapping_data, field_name=None):
        """
        Yields the FHIR map(s) associated with a specific HTAN field or all HTAN FHIR maps

        Return: Yields the field, FHIR map, identifier use, and focus.
            example use:
                for field, fhir_map, use, focus in get_fhir_maps_by_field(cases_mapping, "Year of Diagnosis"):
                    print(f"Field: {field}, FHIR Map: {fhir_map}, Identifier use: {use}, Focus: {focus}")
        """
        for _field, mappings in mapping_data.items():
            if field_name is None or _field == field_name:
                for mapping in mappings:
                    _fhir_map = mapping["fhir_map"]
                    _focus = mapping.get("focus", None)
                    _use = mapping.get("use", None)
                    yield _field, _fhir_map, _use, _focus

    def get_field_value(self, _row: pd.Series, mapping_type: str, fhir_field: str) -> dict:
        mapping_data = None
        if mapping_type == "case":
            mapping_data = self.cases_mappings()
        elif mapping_data == "biospecimen":
            mapping_data = self.biospecimen_mappings()
        elif mapping_type == "file":
            mapping_data = self.files_mappings()

        _this_htan_field = None
        for field, fhir_map, use, focus in self.get_fields_by_fhir_map(mapping_data=mapping_data,
                                                                       fhir_mapping=fhir_field):
            _this_htan_field = field
        _filed_value = _row.get(_this_htan_field)

        return {"htan_field": _this_htan_field, "htan_field_value": _filed_value}

    @staticmethod
    def decipher_htan_id(_id) -> dict:
        """
        <participant_id> ::= <htan_center_id>_integer
        <derivative_entity_id>	::= <participant_id>_integer
        wild-card string ex. '0000' is used for the same file derived from multiple participants
        substring 'EXT' is used for external participants
        """
        deciphered_id = {}
        _id_substrings = _id.split("_")
        participant_id = "_".join([_id_substrings[0], _id_substrings[1]])
        if 'EXT' not in _id_substrings[1] or '0000' not in _id_substrings[1]:
            deciphered_id = {"participant_id": participant_id, "subsets": _id_substrings}
        else:
            participant_id = "_".join([_id_substrings[0], _id_substrings[1], _id_substrings[2]])
            deciphered_id = {"participant_id": participant_id, "subsets": _id_substrings}
        return deciphered_id

    def create_observation(self, _row: pd.Series, patient: Optional[Patient], patient_id: Optional[str],
                           specimen: Optional[Specimen], official_focus: str,
                           focus: List[Reference], components: Optional[List], category: Optional[list],
                           relax: bool) -> Observation:
        # assert patient_id, f"Observation is missing patient id: {patient_id}." # HTAN files doesn't always point to patient
        assert focus, f"Observation for patient {patient_id} is missing focus."

        if not category:
            category = [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                            "code": "exam",
                            "display": "exam"
                        }
                    ],
                    "text": "Exam"
                }
            ]

        observation_fields = []

        if official_focus in ["Patient", "Condition"]:
            mappings = self.cases_mappings()
            code = {
                "coding": [
                    {
                        "system": "http://loinc.org",
                        "code": "75323-6",
                        "display": "Condition"
                    }
                ],
                "text": "Condition"
            }

        elif official_focus in ["MedicationAdministration"]:
            mappings = self.cases_mappings()
            code = self.med_admin_code

        elif official_focus in ["DocumentReference"]:
            mappings = self.files_mappings()
            code = {
                "coding": [
                    {
                        "system": self.SYSTEM_LOINC,
                        "code": "68992-7",
                        "display": "Specimen-related information panel"  # TODO: find general code
                    }
                ],
                "text": "Specimen-related information panel"
            }

        elif official_focus in ["Specimen"]:
            mappings = self.biospecimen_mappings()
            code = {
                "coding": [
                    {
                        "system": self.SYSTEM_LOINC,
                        "code": "68992-7",
                        "display": "Specimen-related information panel"
                    }
                ],
                "text": "Specimen-related information panel"
            }

        for _field, _fhir_map, _use, _focus in self.get_fields_by_fhir_map(mappings,
                                                                           "Observation.component"):
            if _focus == official_focus:
                observation_fields.append(_field)

        if not relax:
            _obervation_row = _row[observation_fields] if observation_fields else None
        else:
            _obervation_row = _row  # user-specific columns in files - add all to component

        if _obervation_row is not None:
            components = []
            for key, value in _obervation_row.to_dict().items():
                if key != 'HTAN Participant ID':
                    try:
                        if not pd.isnull(value):
                            if not isinstance(value, str) and value.is_integer():
                                value = int(value)

                            if " " in key:
                                key = key.replace(" ", "_")
                            if "-" in key:
                                key = key.replace("-", "_")

                            _component = self.get_component(key=key, value=value,
                                                            component_type=self.get_data_types(type(value).__name__),
                                                            system=self.SYSTEM_HTAN)
                            components.append(_component)
                    except (ValueError, TypeError):
                        if self.verbose:
                            print(f"Components {key}: {value} can't be added to list - value/type error.")

        focus_ids = [r.reference.split("/")[1] for r in focus]

        if patient:
            identifier_value = "-".join([patient.identifier[0].value] + focus_ids)
        else:
            identifier_value = "-".join(focus_ids)

        observation_identifier = Identifier(**{"system": self.SYSTEM_HTAN,
                                               "use": "official",
                                               "value": identifier_value})
        observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation",
                                      project_id=self.project_id, namespace=self.NAMESPACE_HTAN)
        specimen_ref = None
        if specimen:
            specimen_ref = Reference(**{"reference": f"Specimen/{specimen.id}"})

        subject = None
        if patient_id:
            subject = Reference(**{"reference": f"Patient/{patient_id}"})
        # add valueCodeableConcept as needed after creation
        return Observation(**{"id": observation_id,
                              "identifier": [observation_identifier],
                              "status": "final",
                              "category": category,
                              "code": code,
                              "focus": focus,
                              "subject": subject,
                              "component": components,
                              "specimen": specimen_ref})

    def get_patient_id(self, participant_id) -> str:
        patient_identifier = Identifier(**{"system": self.SYSTEM_HTAN, "value": str(participant_id), "use": "official"})
        patient_id = self.mint_id(identifier=patient_identifier, resource_type="Patient", project_id=self.project_id,
                                  namespace=self.NAMESPACE_HTAN)
        return patient_id

    @staticmethod
    def create_substance_definition_representations(df: pd.DataFrame) -> list:
        representations = []
        for index, _row in df.iterrows():
            if pd.notna(_row['STANDARD_INCHI']):
                representations.append(SubstanceDefinitionStructureRepresentation(
                    **{"representation": _row['STANDARD_INCHI'],
                       "format": CodeableConcept(**{"coding": [{"code": "InChI",
                                                                "system": 'http://hl7.org/fhir/substance-representation-format',
                                                                "display": "InChI"}]})}))

            if pd.notna(_row['CANONICAL_SMILES']):
                representations.append(SubstanceDefinitionStructureRepresentation(
                    **{"representation": _row['CANONICAL_SMILES'],
                       "format": CodeableConcept(**{"coding": [{"code": "SMILES",
                                                                "system": 'http://hl7.org/fhir/substance-representation-format',
                                                                "display": "SMILES"}]})}))
        return representations

    def create_substance_definition(self, compound_name: str, representations: list) -> SubstanceDefinition:
        sub_def_identifier = Identifier(
            **{"system": self.SYSTEM_chEMBL, "value": str(compound_name), "use": "official"})
        sub_def_id = self.mint_id(identifier=sub_def_identifier, resource_type="SubstanceDefinition",
                                  project_id=self.project_id,
                                  namespace=self.NAMESPACE_HTAN)

        return SubstanceDefinition(**{"id": sub_def_id,
                                      "identifier": [sub_def_identifier],
                                      "structure": SubstanceDefinitionStructure(**{"representation": representations}),
                                      "name": [SubstanceDefinitionName(**{"name": compound_name})]
                                      })

    def create_substance(self, compound_name: str, substance_definition: SubstanceDefinition) -> Substance:
        code = None
        if substance_definition:
            code = CodeableReference(
                **{"concept": CodeableConcept(**{"coding": [
                    {"code": compound_name, "system": "/".join([self.SYSTEM_chEMBL, "compound_name"]),
                     "display": compound_name}]}),
                   "reference": Reference(**{"reference": f"SubstanceDefinition/{substance_definition.id}"})})

        sub_identifier = Identifier(
            **{"system": self.SYSTEM_chEMBL, "value": str(compound_name), "use": "official"})
        sub_id = self.mint_id(identifier=sub_identifier, resource_type="Substance",
                              project_id=self.project_id,
                              namespace=self.NAMESPACE_HTAN)

        return Substance(**{"id": sub_id,
                            "identifier": [sub_identifier],
                            "instance": True,  # place-holder
                            "category": [CodeableConcept(**{"coding": [{"code": "drug",
                                                                        "system": "http://terminology.hl7.org/CodeSystem/substance-category",
                                                                        "display": "Drug or Medicament"}]})],
                            "code": code})

    def create_medication(self, compound_name: Optional[str], treatment_type: Optional[str],
                          _substance: Optional[Substance]) -> Medication:
        code = None
        med_identifier = None

        if compound_name:
            if ":" in compound_name:
                compound_name.replace(":", "_")
            code = CodeableConcept(**{"coding": [
                {"code": str(compound_name), "system": "/".join([self.SYSTEM_chEMBL, "compound_name"]),
                 "display": str(compound_name)}]})

            med_identifier = Identifier(
                **{"system": self.SYSTEM_chEMBL, "value": str(compound_name), "use": "official"})
        else:
            if ":" in treatment_type:
                treatment_type.replace(":", "_")

            code = CodeableConcept(**{"coding": [
                {"code": str(treatment_type), "system": "/".join([self.SYSTEM_HTAN, "treatment_type"]),
                 "display": str(treatment_type)}]})

            med_identifier = Identifier(
                **{"system": self.SYSTEM_HTAN, "value": str(treatment_type), "use": "official"})

        med_id = self.mint_id(identifier=med_identifier, resource_type="Medication",
                              project_id=self.project_id,
                              namespace=self.NAMESPACE_HTAN)

        ingredients = []
        if _substance:
            ingredients.append(MedicationIngredient(**{
                "item": CodeableReference(**{"reference": Reference(**{"reference": f"Substance/{_substance.id}"})})}))

        return Medication(**{"id": med_id,
                             "identifier": [med_identifier],
                             "code": code,
                             "ingredient": ingredients})

    def write_ndjson(self, entities):
        resource_type = entities[0].get_resource_type()
        entities = [orjson.loads(entity.model_dump_json()) for entity in entities]
        entities = list({v['id']: v for v in entities}.values())
        cleaned_entity = utils.clean_resources(entities)
        utils.fhir_ndjson(cleaned_entity, "".join([self.out_dir, "/", resource_type, ".ndjson"]))
        print(f"Successfully transformed HTAN data to {len(entities)} FHIR {resource_type}(s) @ {"".join([self.out_dir, "/", resource_type, ".ndjson"])}")

    def transform_medication(self, cases: pd.DataFrame, db_file_path: str) -> pd.DataFrame:
        # create medication placeholder for cases where treatment type is defined ex chemo, but medication is not documented
        # MedicationAdministration - Medication - Substance - SubstanceDefinition
        drugname_fhir_ids = {}
        substance_definitions = []
        substances = []
        medications = []
        if not cases["Therapeutic Agents"].isnull().all():
            cases["Therapeutic Agents"] = cases["Therapeutic Agents"].str.upper()
            drug_names = list(cases["Therapeutic Agents"][~cases["Therapeutic Agents"].isna()].unique())
            # drug_names = [d.upper() for d in drug_names]

            for drug_name in drug_names:
                if ":" in drug_name:
                    drug_name.replace(":", "_")

            dat = self.get_chembl_compound_info(db_file_path=db_file_path, drug_names=drug_names, limit=1000)
            drug_df = pd.DataFrame(dat)
            drug_df.columns = ["CHEMBL_ID", "STANDARD_INCHI", "CANONICAL_SMILES", "COMPOUND_NAME"]

            for drug in drug_names:
                drug_info = drug_df[drug_df.COMPOUND_NAME.isin([drug])]
                drug_info["has_info"] = drug_info[['STANDARD_INCHI', 'CANONICAL_SMILES']].notna().any(axis=1)
                if drug_info["has_info"].any():
                    drug_representations = self.create_substance_definition_representations(drug_info)
                    substance_definition = self.create_substance_definition(compound_name=drug,
                                                                            representations=drug_representations)

                    if substance_definition:
                        substance_definitions.append(substance_definition)

                    substance = self.create_substance(compound_name=drug, substance_definition=substance_definition)

                    if substance:
                        substances.append(substance)
                        medication = self.create_medication(compound_name=drug, _substance=substance,
                                                            treatment_type=None)
                        if medication:
                            medications.append(medication)
                            drugname_fhir_ids.update({drug: medication.id})

                else:
                    medication = self.create_medication(compound_name=drug, _substance=None, treatment_type=None)
                    medications.append(medication)
                    drugname_fhir_ids.update({drug: medication.id})

            if substance_definitions:
                self.write_ndjson(substance_definitions)
            if substances:
                self.write_ndjson(substances)

            cases['Medication_ID'] = cases['Therapeutic Agents'].map(drugname_fhir_ids, na_action='ignore')

        for index, row in cases.iterrows():
            if pd.isnull(row["Therapeutic Agents"]) and not pd.isnull(row["Treatment Type"]):
                medication_agent = self.create_medication(compound_name=None, _substance=None,
                                                          treatment_type=row["Treatment Type"])
                if medication_agent:
                    medications.append(medication_agent)
                    cases.loc[index, 'Medication_ID'] = medication_agent.id

            if row['Therapeutic Agents'] in drugname_fhir_ids.keys():
                cases.loc[index, 'Medication_ID'] = drugname_fhir_ids[row['Therapeutic Agents']]

        if medications:
            self.write_ndjson(medications)
        if 'Medication_ID' in cases.columns:
            return cases


class PatientTransformer(HTANTransformer):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(**kwargs)
        self.cases_mapping = self.cases_mappings
        self.NAMESPACE_HTAN = self.NAMESPACE_HTAN
        self.get_data_types = utils.get_data_types
        self.get_component = self.get_component
        self.get_fields_by_fhir_map = self.get_fields_by_fhir_map
        self.create_observation = self.create_observation

    def create_patient(self, _row: pd.Series) -> Patient:
        """Transform HTAN case demographics to FHIR Patient"""
        use = None
        for _field, _fhir_map, _use, _focus in self.get_fields_by_fhir_map(self.cases_mapping(), "Patient.identifier"):
            use = _use
        assert use, f"Patient.identifier use is not defined in ./resources/HTAN/cases.json mappings."

        patient_identifier = Identifier(
            **{"system": self.SYSTEM_HTAN, "value": str(_row['HTAN Participant ID']), "use": use})
        patient_id = self.mint_id(identifier=patient_identifier, resource_type="Patient", project_id=self.project_id,
                                  namespace=self.NAMESPACE_HTAN)

        deceasedBoolean_fields = []
        for _field, _fhir_map, _use, _focus in self.get_fields_by_fhir_map(self.cases_mapping(),
                                                                           "Patient.deceasedBoolean"):
            deceasedBoolean_fields.append(_field)
        assert deceasedBoolean_fields, f"Patient.deceasedBoolean has no fields defined in ./resources/HTAN/cases.json mappings."

        vital_status = _row[deceasedBoolean_fields].dropna().unique().any()
        deceasedBoolean = {"Dead": True}.get(vital_status, False if vital_status else None)

        # TODO: us-core-ethnicity and race resource
        patient_extension = []
        ethnicity = _row.get("Ethnicity")
        race = _row.get("Race")

        if ethnicity and isinstance(ethnicity, str):
            patient_extension.append({"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-ethnicity",
                                         "valueString": ethnicity})
        if race and isinstance(race, str):
            patient_extension.append({"url": "http://hl7.org/fhir/us/core/StructureDefinition/us-core-race",
                                         "valueString": race})


        address_country = _row.get("Country of Residence")
        address = [Address(**{"country": address_country})] if not pd.isna(address_country) else []

        return Patient(**{"id": patient_id,
                          "identifier": [patient_identifier],
                          "deceasedBoolean": deceasedBoolean,
                          "extension": patient_extension,
                          "address": address})

    def patient_observation(self, patient: Patient, _row: pd.Series) -> Observation:
        patient_observation_fields = []
        for field, fhir_map, use, focus in self.get_fields_by_fhir_map(self.cases_mappings(),
                                                                       "Observation.component"):
            if focus == "Patient":
                patient_observation_fields.append(field)

        if patient_observation_fields:
            _obervation_row = _row[patient_observation_fields]

        components = []
        for key, value in _obervation_row.to_dict().items():
            if key != 'HTAN Participant ID':
                if isinstance(value, float) and not pd.isna(value) and (
                        "Year" in key or "Day" in key or "year" in key or "day" in key):
                    value = int(value)

                    if " " in key:
                        key = key.replace(" ", "_")
                    if "-" in key:
                        key = key.replace("-", "_")

                    _component = self.get_component(key=key, value=value,
                                                    component_type=self.get_data_types(type(value).__name__),
                                                    system=self.SYSTEM_HTAN)

                    components.append(_component)

        observation_identifier = Identifier(**{"system": self.SYSTEM_HTAN, "use": "official", "value": str(patient.id)})
        observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation",
                                      project_id=self.project_id, namespace=self.NAMESPACE_HTAN)

        return Observation(**{"id": observation_id,
                              "identifier": [observation_identifier],
                              "status": "final",
                              "category": [
                                  {
                                      "coding": [
                                          {
                                              "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                                              "code": "exam",
                                              "display": "exam"
                                          }
                                      ],
                                      "text": "Exam"
                                  }
                              ],
                              "code": {
                                  "coding": [
                                      {
                                          "system": self.SYSTEM_LOINC,
                                          "code": "52460-3",  # TODO: may need to change to be more specific
                                          "display": "patient information"
                                      }
                                  ],
                                  "text": "Patient Information"
                              },
                              "focus": [Reference(**{"reference": f"Patient/{patient.id}"})],
                              "subject": Reference(**{"reference": f"Patient/{patient.id}"}),
                              "component": components})

    def create_researchstudy(self, _row: pd.Series) -> ResearchStudy:
        study_field = None
        for field, fhir_map, use, focus in self.get_fields_by_fhir_map(self.cases_mappings(), "ResearchStudy.name"):
            study_field = field
        study_name = _row.get(study_field)
        researchstudy_identifier = Identifier(
            **{"system": self.SYSTEM_HTAN, "use": "official", "value": str(study_name).replace("HTAN ", "")})
        researchstudy_id = self.mint_id(identifier=researchstudy_identifier, resource_type="ResearchStudy",
                                        project_id=self.project_id, namespace=self.NAMESPACE_HTAN)

        # TODO: add "condition" snomed id
        if "HTAN " in study_name:
             study_name = study_name.replace("HTAN ", "")
        return ResearchStudy(**{"id": researchstudy_id,
                                "identifier": [researchstudy_identifier],
                                "name": study_name,
                                "status": "open",
                                "partOf": [
                                    Reference(**{"reference": f"ResearchStudy/{self.program_research_study.id}"})]})

    def create_researchsubject(self, patient: Patient, study: ResearchStudy) -> ResearchSubject:
        researchsubject_identifier = Identifier(
            **{"system": self.SYSTEM_HTAN, "use": "official", "value": str(patient.identifier[0].value)})
        researchsubject_id = self.mint_id(identifier=researchsubject_identifier, resource_type="ResearchSubject",
                                          project_id=self.project_id, namespace=self.NAMESPACE_HTAN)
        return ResearchSubject(**{"id": researchsubject_id,
                                  "identifier": [researchsubject_identifier],
                                  "status": "active",
                                  "subject": Reference(**{"reference": f"Patient/{patient.id}"}),
                                  "study": Reference(**{"reference": f"ResearchStudy/{study.id}"})})

    def create_encounter(self, _row: pd.Series, patient: Patient, condition: Optional[Condition],
                         procedure: Optional[Procedure]) -> Encounter:
        # identifier string = project / patient / [condition/procedure] - assume parent encounter atm
        condition_procedure = ""
        if condition:
            condition_procedure = condition.id
        elif procedure:
            condition_procedure = procedure.id

        encounter_identifier = Identifier(**{"system": self.SYSTEM_HTAN, "use": "official",
                                             "value": "-".join([self.subprogram_name, patient.identifier[0].value])})
        encounter_id = self.mint_id(identifier=encounter_identifier, resource_type="Encounter",
                                    project_id=self.project_id, namespace=self.NAMESPACE_HTAN)

        return Encounter(**{"id": encounter_id,
                            "identifier": [encounter_identifier],
                            "status": "completed",
                            "subject": Reference(**{"reference": f"Patient/{patient.id}"})
                            })

    def create_body_structure(self, _row, patient: Patient) -> BodyStructure:
        body_structure_value = _row.get("Tissue or Organ of Origin")
        included_structure = []
        if body_structure_value:
            included_structure = [BodyStructureIncludedStructure(**{"structure": CodeableConcept(**{"coding": [
                {"code": str(body_structure_value), "system": self.SYSTEM_HTAN,
                 "display": str(body_structure_value)}]})})]
            body_struct_ident = Identifier(
                **{"system": self.SYSTEM_HTAN, "use": "official", "value": str(body_structure_value)})
        return BodyStructure(
            **{"id": utils.mint_id(identifier=[patient.identifier[0].value, body_struct_ident],
                                   resource_type="BodyStructure",
                                   project_id=self.project_id,
                                   namespace=self.NAMESPACE_HTAN),
               "identifier": [body_struct_ident],
               "includedStructure": included_structure,
               "patient": Reference(**{"reference": f"Patient/{patient.id}"})
               })

    def create_condition(self, _row: pd.Series, patient: Patient,
                         body_structure: Optional[BodyStructure], stage_observation: Optional[Observation]) -> dict:
        primary_diagnosis = _row.get("Primary Diagnosis")
        if pd.isnull(primary_diagnosis):
            return {}

        # identifier string = project / patient / primary diagnosis
        condition_identifier = Identifier(**{"system": self.SYSTEM_HTAN,
                                             "use": "official",
                                             "value": "-".join([self.subprogram_name, patient.id,
                                                                primary_diagnosis])})
        condition_id = self.mint_id(identifier=condition_identifier, resource_type="ResearchSubject",
                                    project_id=self.project_id, namespace=self.NAMESPACE_HTAN)

        onset_age = None
        primary_diagnosis_age = self.get_field_value(_row=_row, mapping_type="case", fhir_field="Condition.onsetAge")

        primary_diagnosis_age_value = None
        if not np.isnan(primary_diagnosis_age["htan_field_value"]):
            primary_diagnosis_age_value = int(primary_diagnosis_age["htan_field_value"])

        if primary_diagnosis_age_value:
            onset_age = Age(**{"value": primary_diagnosis_age_value,
                               "unit": "years",
                               "system": "http://unitsofmeasure.org",
                               "code": "a"
                               })

        recorded_date_field_value = self.get_field_value(_row=_row, mapping_type="case",
                                                         fhir_field="Condition.recordedDate")
        recorded_date = None
        if not np.isnan(recorded_date_field_value["htan_field_value"]):
            recorded_date = datetime(int(recorded_date_field_value["htan_field_value"]), 1, 1, tzinfo=timezone.utc)

        body_structure = self.create_body_structure(_row, patient)
        patient_body_structure_ref = Reference(
            **{"reference": f"BodyStructure/{body_structure.id}"}) if body_structure.includedStructure else None

        patient_body_site_cc = []
        patient_body_site = self.get_field_value(_row=_row, mapping_type="case", fhir_field="Condition.bodySite")[
            "htan_field_value"]

        if patient_body_site:
            patient_body_site_cc = [CodeableConcept(**{"coding": [{"code": str(patient_body_site),
                                                                   "system": self.SYSTEM_HTAN,
                                                                   "display": str(patient_body_site)}]})]

        condition = Condition(**{"id": condition_id,
                                 "identifier": [condition_identifier],
                                 "code": CodeableConcept(**{"coding": [{"code": str(primary_diagnosis),
                                                                        "system": self.SYSTEM_HTAN,
                                                                        "display": str(primary_diagnosis)}]}),
                                 "subject": Reference(**{"reference": f"Patient/{patient.id}"}),
                                 "clinicalStatus": CodeableConcept(**{"coding": [{"code": "active",
                                                                                  "system": "http://terminology.hl7.org/CodeSystem/condition-clinical",
                                                                                  "display": "Active"}]}),
                                 "onsetAge": onset_age,
                                 "recordedDate": recorded_date,
                                 "bodySite": patient_body_site_cc
                                 # "bodyStructure": patient_body_structure_ref,
                                 # "encounter": Reference(**{"reference": f"Encounter/{encounter.id}"})
                                 })

        stage_observations_dict = self.create_stage_observation(_row=_row, condition=condition, patient=patient)

        condition_stage = []
        stages = self.create_stage(_row=_row, stage_observations_dict=stage_observations_dict)
        if stages:
            condition_stage = stages

        condition.stage = condition_stage

        return {"condition": condition, "stage_observations_dict": stage_observations_dict}

    def create_medication_administration(self, _row: pd.Series, patient_id: str) -> MedicationAdministration:
        # if Treatment Type exists - make MedicationAdministration
        # if Days to Treatment End, then status -> completed, else status unknown
        # if Therapeutic Agents is null, then Medication.code -> snomed_code: Unknown 261665006
        # Medication.ingredient.item -> Substance.code -> SubstanceDefination
        # TODO: create medicationAdministration regimen for combinatorial drug therapy syntax "medication A + medication B"
        status = None
        substance_definition = None
        substance = None
        medication = None
        medication_code = None

        if not pd.isnull(_row["Days to Treatment End"]):
            status = "completed"
        else:
            status = "unknown"

        if pd.isnull(_row["Therapeutic Agents"]):
            medication_code = CodeableConcept(**{"coding": [{
                "code": "261665006",
                "system": self.SYSTEM_SNOME,
                "display": "Unknown"
            }]})
        else:
            # drug_info_df = pd.DataFrame(self.get_chembl_compound_info(db_file_path="./reources/chemble/chembl_34.db", drug_names=list(_row["Theraputic Agent"])))
            medication_code = CodeableConcept(**{"coding": [{"code": str(_row["Therapeutic Agents"]),
                                                             "system": self.SYSTEM_HTAN,
                                                             "display": _row["Therapeutic Agents"]}]})

        # place holder - required by FHIR
        timing = Timing(**{
            "repeat": TimingRepeat(**{
                "boundsRange": Range(**{
                    "low": Quantity(**{"value": 0}),
                    "high": Quantity(**{"value": 1})
                })
            })
        })

        if not pd.isnull(_row["Days to Treatment End"]) and not pd.isnull(_row["Days to Treatment Start"]):
            timing = Timing(**{"repeat": TimingRepeat(**{"boundsRange": Range(
                **{"low": Quantity(**{"value": int(_row["Days to Treatment Start"])}),
                   "high": Quantity(**{"value": int(_row["Days to Treatment End"])})})})})

        medication_admin_identifier = Identifier(
            **{"system": self.SYSTEM_HTAN, "use": "official",
               "value": "-".join([_row["Atlas Name"], _row["HTAN Participant ID"], _row["Treatment Type"]])})
        medication_admin_id = self.mint_id(identifier=medication_admin_identifier,
                                           resource_type="MedicationAdministration",
                                           project_id=self.project_id, namespace=self.NAMESPACE_HTAN)
        data = {"id": medication_admin_id,
                "identifier": [medication_admin_identifier],
                "status": status,
                "occurenceTiming": timing,
                "category": [CodeableConcept(**{"coding": [{"code": str(_row["Treatment Type"]),
                                                            "system": "/".join([self.SYSTEM_HTAN, "Treatment_Type"]),
                                                            "display": str(_row["Treatment Type"])}]})],
                "medication": CodeableReference(**{"concept": medication_code, "reference": Reference(
                    **{"reference": f"Medication/{_row['Medication_ID']}"})}),
                "subject": Reference(**{"reference": f"Patient/{patient_id}"})}

        return MedicationAdministration(**data)

    def create_stage(self, _row: pd.Series, stage_observations_dict: dict) -> list:
        assessment = []

        # find fields w Condition.stage.summary mappings
        cancer_pathological_staging = utils._read_json(str(Path(importlib.resources.files(
            'fhirizer').parent / 'resources' / 'gdc_resources' / 'content_annotations' / 'diagnosis' / 'cancer_pathological_staging.json')))

        stage_fields = []
        for field, fhir_map, use, focus in self.get_fields_by_fhir_map(self.cases_mappings(),
                                                                       "Condition.stage.summary"):
            if "Tumor Grade" in field or "AJCC Pathologic" in field:
                # TODO: check for 8th/other edition
                stage_fields.append(field)

        if stage_fields:
            _stage_df = _row[stage_fields]

        stages = []
        for stage_field in stage_fields:
            stage_name = "_".join(stage_field.lower().split(" "))
            stage_observation = stage_observations_dict.get(stage_name)
            if stage_observation:
                assessment = [Reference(**{"reference": f"Observation/{stage_observation.id}"})]
            if not pd.isnull(_row[stage_field]):

                types = []
                summaries = []
                for stage_info in cancer_pathological_staging:
                    if _row[stage_field] == stage_info["value"]:
                        type_system = {"code": stage_info["stage_type_sctid"],
                                       "system": self.SYSTEM_SNOME,
                                       "display": stage_info["stage_type_sctid_display"]}

                        summary_htan_system = {"code": _row[stage_field],
                                               "system": "/".join(
                                                   [self.SYSTEM_HTAN, "_".join(stage_field.lower().split(" "))]),
                                               "display": _row[stage_field]}

                        summary_snomed_system = {"code": stage_info["sctid"],
                                                 "system": self.SYSTEM_SNOME,
                                                 "display": stage_info["sctid_display"]}

                        types.append(type_system)
                        summaries.append(summary_htan_system)
                        summaries.append(summary_snomed_system)
                if not types:
                    types.append({"code": "_".join(stage_field.lower().split(" ")),
                                  "system": "/".join([self.SYSTEM_HTAN, "_".join(stage_field.lower().split(" "))]),
                                  "display": "_".join(stage_field.lower().split(" "))})

                    summaries.append({"code": str(_row[stage_field]),
                                      "system": "/".join([self.SYSTEM_HTAN, "_".join(stage_field.lower().split(" "))]),
                                      "display": str(_row[stage_field])})

                condition_stage = ConditionStage(
                    **{"summary": CodeableConcept(**{"coding": summaries}),
                       "assessment": assessment,
                       "type": CodeableConcept(**{"coding": types})})
                if condition_stage:
                    stages.append(condition_stage)

        return stages

    def create_stage_observation(self, _row: pd.Series, condition: Condition, patient: Patient) -> dict:
        observation_dict = {}

        # find fields w Condition.stage.summary mappings
        cancer_pathological_staging = utils._read_json(str(Path(importlib.resources.files(
            'fhirizer').parent / 'resources' / 'gdc_resources' / 'content_annotations' / 'diagnosis' / 'cancer_pathological_staging.json')))

        ajcc_pathologic_stage_fields = []
        grade_stage_fields = []
        for field, fhir_map, use, focus in self.get_fields_by_fhir_map(self.cases_mappings(),
                                                                       "Condition.stage.summary"):
            if "AJCC Pathologic" in field:
                # TODO: check for 8th/other edition
                ajcc_pathologic_stage_fields.append(field)
            elif "Tumor Grade" in field:
                grade_stage_fields.append(field)

        _ajcc_pathologic_stage = None
        if pd.notna(ajcc_pathologic_stage_fields).all():
            _ajcc_pathologic_stage = _row[ajcc_pathologic_stage_fields]

        member = []
        if pd.notna(ajcc_pathologic_stage_fields).all():
            # print(_ajcc_pathologic_stage, type(_ajcc_pathologic_stage))
            for col_name, value in _ajcc_pathologic_stage.items():
                # "these are children stages and are members"
                if value and col_name != "AJCC Pathologic Stage":
                    stage = "_".join(col_name.lower().split(" "))
                    identifier_value = "-".join([patient.identifier[0].value, condition.id, stage])
                    observation_identifier = Identifier(**{"system": self.SYSTEM_HTAN,
                                                           "use": "official",
                                                           "value": str(identifier_value)})
                    observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation",
                                                  project_id=self.project_id, namespace=self.NAMESPACE_HTAN)

                    code = None
                    value_code = None
                    for stage_info in cancer_pathological_staging:
                        if value == stage_info["value"]:
                            code = CodeableConcept(**{"coding": [{"code": str(stage_info["stage_type_sctid"]),
                                                                  "system": self.SYSTEM_SNOME,
                                                                  "display": str(
                                                                      stage_info["stage_type_sctid_display"])}]})

                            value_code = CodeableConcept(**{"coding": [{"code": str(stage_info["sctid"]),
                                                                        "system": self.SYSTEM_SNOME,
                                                                        "display": str(stage_info["sctid_display"])}]})
                    if not code:
                        code = CodeableConcept(**{"coding": [{"code": str(stage),
                                                              "system": "/".join([self.SYSTEM_HTAN, stage]),
                                                              "display": str(stage)}]})

                        value_code = CodeableConcept(**{"coding": [{"code": str(value),
                                                                    "system": "/".join(
                                                                        [self.SYSTEM_HTAN, stage]),
                                                                    "display": str(value)}]})

                    _stage_observation = Observation(**{"id": observation_id,
                                                        "identifier": [observation_identifier],
                                                        "status": "final",
                                                        "code": code,
                                                        "subject": Reference(**{"reference": f"Patient/{patient.id}"}),
                                                        "focus": [
                                                            Reference(**{"reference": f"Condition/{condition.id}"})],
                                                        "valueCodeableConcept": value_code})
                    observation_dict.update({stage: _stage_observation})
                    member.append(Reference(**{"reference": f"Observation/{_stage_observation.id}"}))
            # print(member)

            if not pd.isnull(_row["AJCC Pathologic Stage"]):
                stage = "_".join("AJCC Pathologic Stage".lower().split(" "))
                identifier_value = "-".join([patient.identifier[0].value, condition.id, stage])
                observation_identifier = Identifier(**{"system": self.SYSTEM_HTAN,
                                                       "use": "official",
                                                       "value": str(identifier_value)})
                observation_id = self.mint_id(identifier=observation_identifier, resource_type="Observation",
                                              project_id=self.project_id, namespace=self.NAMESPACE_HTAN)

                code = None
                value_code = None
                for stage_info in cancer_pathological_staging:
                    if _row["AJCC Pathologic Stage"] == stage_info["value"]:
                        code = CodeableConcept(**{"coding": [{"code": str(stage_info["stage_type_sctid"]),
                                                              "system": self.SYSTEM_SNOME,
                                                              "display": str(stage_info["stage_type_sctid_display"])}]})

                        value_code = CodeableConcept(**{"coding": [{"code": str(stage_info["sctid"]),
                                                                    "system": self.SYSTEM_SNOME,
                                                                    "display": str(stage_info["sctid_display"])}]})
                if not code:
                    code = CodeableConcept(**{"coding": [{"code": str(stage),
                                                          "system": "/".join([self.SYSTEM_HTAN, stage]),
                                                          "display": str(stage)}]})

                    value_code = CodeableConcept(**{"coding": [{"code": str(_row["AJCC Pathologic Stage"]),
                                                                "system": "/".join(
                                                                    [self.SYSTEM_HTAN, stage]),
                                                                "display": str(_row["AJCC Pathologic Stage"])}]})

                _stage_observation = Observation(**{"id": observation_id,
                                                    "identifier": [observation_identifier],
                                                    "status": "final",
                                                    "code": code,
                                                    "subject": Reference(**{"reference": f"Patient/{patient.id}"}),
                                                    "focus": [Reference(**{"reference": f"Condition/{condition.id}"})],
                                                    "valueCodeableConcept": value_code,
                                                    "hasMember": member})
                observation_dict.update({stage: _stage_observation})

        return observation_dict


class SpecimenTransformer(HTANTransformer):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(**kwargs)
        self.cases_mapping = self.cases_mappings
        self.NAMESPACE_HTAN = self.NAMESPACE_HTAN
        self.get_data_types = utils.get_data_types
        self.get_component = self.get_component
        self.get_fields_by_fhir_map = self.get_fields_by_fhir_map
        self.create_observation = self.create_observation
        self.get_patient_id = self.get_patient_id

    def create_specimen(self, _row: pd.Series) -> Specimen:
        """Transform HTAN biospecimen to FHIR Specimen"""

        specimen_identifier = Identifier(
            **{"system": self.SYSTEM_HTAN, "value": str(_row['HTAN Biospecimen ID']), "use": "official"})
        specimen_id = self.mint_id(identifier=specimen_identifier, resource_type="Specimen", project_id=self.project_id,
                                   namespace=self.NAMESPACE_HTAN)

        # participant id from specimen identifier
        participant_id = self.decipher_htan_id(_row["HTAN Biospecimen ID"])["participant_id"]
        assert participant_id, f"Specimen {_row["HTAN Biospecimen ID"]} does not have a patient participant associated with it."

        patient_id = self.get_patient_id(participant_id=participant_id)
        subject = Reference(**{"reference": f"Patient/{patient_id}"})  # Check if Group exists

        parent_specimen_reference = []
        if not pd.isnull(_row["HTAN Parent ID"]):
            parent_specimen_identifier = Identifier(
                **{"system": self.SYSTEM_HTAN, "value": str(_row['HTAN Biospecimen ID']), "use": "official"})
            parent_specimen_id = self.mint_id(identifier=parent_specimen_identifier, resource_type="Specimen",
                                              project_id=self.project_id,
                                              namespace=self.NAMESPACE_HTAN)
            parent_specimen_reference.append(Reference(**{"reference": f"Specimen/{parent_specimen_id}"}))

        specimen_fields = []
        for _field, _fhir_map, _use, _focus in self.get_fields_by_fhir_map(self.cases_mapping(),
                                                                           "Specimen"):
            specimen_fields.append(_field)

        return Specimen(**{"id": specimen_id,
                           "identifier": [specimen_identifier],
                           "type": CodeableConcept(**{"coding": [
                               {"code": str(_row["Biospecimen Type"]), "system": self.SYSTEM_HTAN,
                                "display": _row["Biospecimen Type"]}]}),
                           "processing": [SpecimenProcessing(**{"method": CodeableConcept(**{"coding": [
                               {"code": str(_row["Preservation Method"]), "system": self.SYSTEM_HTAN,
                                "display": str(_row["Preservation Method"])}]})})],
                           "parent": parent_specimen_reference,
                           "subject": subject})


class DocumentReferenceTransformer(HTANTransformer):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(**kwargs)
        self.cases_mapping = self.cases_mappings
        self.NAMESPACE_HTAN = self.NAMESPACE_HTAN
        self.get_data_types = utils.get_data_types
        self.get_component = self.get_component
        self.get_fields_by_fhir_map = self.get_fields_by_fhir_map
        self.create_observation = self.create_observation
        self.get_patient_id = self.get_patient_id

    def create_document_reference(self, _row: pd.Series, specimen_ids: list, case_patient_ids: list) -> dict | None:
        """Transform HTAN files to FHIR DocumentReference"""
        # print(f"Specimen List length: {len(specimen_ids)} List: {specimen_ids}")
        document_reference_identifier = Identifier(
            **{"system": self.SYSTEM_HTAN, "value": str(_row['HTAN Data File ID']), "use": "official"})

        document_reference_synapse_identifier = Identifier(
            **{"system": "/".join([self.SYSTEM_HTAN, "synapse_id"]), "value": str(_row['Synapse Id']), "use": "secondary"})

        document_reference_id = self.mint_id(identifier=document_reference_identifier,
                                             resource_type="DocumentReference", project_id=self.project_id,
                                             namespace=self.NAMESPACE_HTAN)
        subject = None

        # participant id
        patient_id = None
        if "HTAN Participant ID" in _row.keys() and not pd.isnull(_row["HTAN Participant ID"]):
            participant_id = _row["HTAN Participant ID"]
            # assert participant_id, f"DocumentRefernce {_row["HTAN Data File ID"]} does not have a patient participant associated with it."
            patient_id = self.get_patient_id(participant_id=participant_id)

        name = None
        if _row["Filename"]:
            name = _row["Filename"]

        profiles = []
        if not pd.isnull(_row['drs_uri']):
            uri_profile = DocumentReferenceContentProfile(**{"valueUri": _row['drs_uri']})
            profiles.append(uri_profile)

        category = []
        if not pd.isnull(_row['Assay']):
            category.append(CodeableConcept(**{"coding": [
                {"code": str(_row['Assay']), "display": str(_row['Assay']),
                 "system": "/".join([self.SYSTEM_HTAN, "Assay"])}]}))
        if not pd.isnull(_row['Level']):
            category.append(CodeableConcept(**{"coding": [
                {"code": str(_row['Level']), "display": str(_row['Level']),
                 "system": "/".join([self.SYSTEM_HTAN, "Level"])}]}))

        patient_reference = None
        if patient_id and patient_id in case_patient_ids:
            patient_reference = Reference(**{"reference": f"Patient/{patient_id}"})

        security_label = []
        if not pd.isnull(_row['Data Access']):
            security_label.append(CodeableConcept(**{"coding": [
                {"code": str(_row['Data Access']), "display": str(_row['Data Access']),
                 "system": "/".join([self.SYSTEM_HTAN, "Data_Access"])}]}))

        parent_data_file = []
        if not pd.isnull(_row["Parent Data File ID"]):
            parent_document_reference_identifier = Identifier(
                **{"system": self.SYSTEM_HTAN, "value": str(_row["Parent Data File ID"]), "use": "official"})

            parent_document_reference_id = self.mint_id(identifier=parent_document_reference_identifier,
                                                        resource_type="DocumentReference", project_id=self.project_id,
                                                        namespace=self.NAMESPACE_HTAN)

            parent_data_file.append(DocumentReferenceRelatesTo(**{
                "code": CodeableConcept(**{"coding": [{"code": "parent_data_file",
                                                       "system": "/".join([self.SYSTEM_HTAN, "Parent_Data_File_ID"]),
                                                       "display": "parent_data_file"}]}),
                "target": Reference(**{"reference": f"Documentreference/{parent_document_reference_id}"})}))

        def create_docref_specimen_id(_specimen_identifier_value):
            specimen_identifier = Identifier(
                **{"system": self.SYSTEM_HTAN, "value": _specimen_identifier_value, "use": "official"})
            specimen_id = self.mint_id(identifier=specimen_identifier, resource_type="Specimen",
                                       project_id=self.project_id,
                                       namespace=self.NAMESPACE_HTAN)
            return specimen_id


        specimen_references = []
        if not pd.isnull(_row['Biospecimen']):
            if "," in _row['Biospecimen']:
                _htan_specimens_ids = [item.strip() for item in _row['Biospecimen'].split(",")]
                for _specimen_id in _htan_specimens_ids:
                    _specimen_mint_id = create_docref_specimen_id(_specimen_identifier_value=str(_specimen_id))
                    if _specimen_mint_id in specimen_ids:
                        specimen_references.append(Reference(**{"reference": f"Specimen/{_specimen_mint_id}"}))
            else:
                specimen_id = create_docref_specimen_id(_specimen_identifier_value=str(_row['Biospecimen']))
                if specimen_id in specimen_ids:
                    specimen_references.append(Reference(**{"reference": f"Specimen/{specimen_id}"}))
        group = None
        if specimen_references:
            if len(specimen_references) == 1:
                subject = specimen_references[0]
            if len(specimen_references) > 1:
                specimen_members = [GroupMember(**{'entity': s}) for s in specimen_references]
                reference_ids = [s.reference for s in specimen_references]
                group_identifier = Identifier(
                    **{"system": "".join([self.SYSTEM_HTAN, "sample_group"]),
                       "value": "/".join(reference_ids + ["Documentreference/" + document_reference_identifier.value]),
                       "use": "official"})

                group_id = utils.mint_id(identifier=group_identifier, resource_type="Group",
                                         project_id=self.project_id,
                                         namespace=self.NAMESPACE_HTAN)
                if specimen_members:
                    group = Group(**{'id': group_id, "identifier": [group_identifier], "membership": 'definitional',
                                     'member': specimen_members, "type": "specimen"})

                    subject = Reference(**{"reference": "/".join(["Group", group.id])})

        if patient_reference and not subject:
            subject = patient_reference

        if subject:
            document_reference = DocumentReference(**{"id": document_reference_id,
                                        "identifier": [document_reference_identifier,
                                                       document_reference_synapse_identifier],
                                        "status": "current",
                                        "docStatus": "final",
                                        "subject": subject,
                                        # "relatesTo": parent_data_file,  # TODO: requires check for file - missing data relations
                                        "category": category,
                                        "securityLabel": security_label,
                                        "content": [DocumentReferenceContent(
                                            **{"attachment": Attachment(
                                                **{"title": name,
                                                   "url": "file:///" + name,
                                                   "extension": [
                                                       {
                                                           "url": "http://aced-idp.org/fhir/StructureDefinition/source_path",
                                                           "valueUrl": "file:///" + name
                                                       }
                                                   ],
                                                   "contentType": _row["mime_type"]}),
                                               "profile": profiles
                                              })]
                                        })

            return {"file": document_reference, "group": group}
        else:
            # don't make the docref where the subject relation is undefined/unknown (stand-alone entity with no links)
            return None


# 2 Projects that don't have files download or cds manifest SRRS and TNP_TMA (Oct/2024)
# 12/14 total Atlas
def htan2fhir(verbose, entity_atlas_name, spinner):
    warnings.filterwarnings('ignore')

    atlas_names = ["OHSU", "DFCI", "WUSTL", "BU", "CHOP", "Duke", "HMS", "HTAPP", "MSK", "Stanford",
                   "Vanderbilt", "TNP_SARDANA"]
    assert entity_atlas_name not in atlas_names, f"Please provide a valid HTAN Atlas name in:  {atlas_names}"

    # TNP_SARDANA drug name syntax error
    db_path = str(
        Path(importlib.resources.files('fhirizer').parent / 'resources' / 'chembl_resources' / 'chembl_34.db'))
    assert Path(importlib.resources.files(
        'fhirizer').parent / 'resources' / 'chembl_resources' / 'chembl_34.db').is_file(), f"chEMBL db file chembl_34.db does not exist."

    for name in entity_atlas_name:
        if len(entity_atlas_name) > 1:
            spinner.stop()
            print(f"\nTransforming {name}\n")

        transformer = HTANTransformer(subprogram_name=name, out_dir=f"./projects/HTAN/{name}/META", verbose=verbose)
        patient_transformer = PatientTransformer(subprogram_name=name, out_dir=f"./projects/HTAN/{name}/META",
                                                 verbose=verbose)
        specimen_transformer = SpecimenTransformer(subprogram_name=name, out_dir=f"./projects/HTAN/{name}/META",
                                                   verbose=verbose)
        documentreference_transformer = DocumentReferenceTransformer(subprogram_name=name,
                                                                     out_dir=f"./projects/HTAN/{name}/META",
                                                                     verbose=verbose)

        patient_demographics_df = transformer.patient_demographics
        cases = transformer.cases
        htan_biospecimens = transformer.biospecimens
        files = transformer.files
        files_drs_meta = transformer.files_drs_meta

        patients = []
        research_studies = []
        research_subjects = []
        conditions = []
        encounters = []
        observations = []
        med_admins = []

        if not cases["Therapeutic Agents"].isnull().all() or not cases["Treatment Type"].isnull().all():
            cases = transformer.transform_medication(cases, db_file_path=db_path)

        project_research_study = None
        for index, row in cases.iterrows():
            research_study = patient_transformer.create_researchstudy(_row=row)

            if research_study:
                research_studies.append(transformer.program_research_study)
                research_studies.append(research_study)
                project_research_study = next(rs for rs in research_studies if rs.name != "HTAN")

                patient_row = cases.iloc[index][patient_demographics_df.columns]
                patient = patient_transformer.create_patient(_row=patient_row)
                patient_obs = patient_transformer.patient_observation(patient=patient, _row=row)
                if patient_obs:
                    observations.append(patient_obs)
                if patient:
                    patients.append(patient)
                    # print(f"HTAN FHIR Patient: {patient.model_dump_json()}")
                    # print(f"HTAN FHIR Patient Observation: {patient_obs.json()}")

                    research_subject = patient_transformer.create_researchsubject(patient, research_study)
                    if research_subject:
                        research_subjects.append(research_subject)

                    # encounter = patient_transformer.create_encounter(_row=row, patient=patient, condition=None,
                    #                                                  procedure=None)
                    # if encounter:
                    #     encounters.append(encounter)
                    condition_dict = patient_transformer.create_condition(_row=row, patient=patient,
                                                                         # encounter=encounter,
                                                                         body_structure=None,
                                                                         stage_observation=None)

                    if condition_dict and condition_dict["condition"]:
                        conditions.append(condition_dict["condition"])

                        if condition_dict["stage_observations_dict"]:
                            for key, obs_item in condition_dict["stage_observations_dict"].items():
                                if obs_item:
                                    observations.append(obs_item)

                        condition_observation = patient_transformer.create_observation(_row=row, patient=patient,
                                                                                       patient_id=patient.id,
                                                                                       official_focus="Condition",
                                                                                       focus=[Reference(**{
                                                                                           "reference": f"Condition/{condition_dict["condition"].id}"})],
                                                                                       specimen=None,
                                                                                       components=None,
                                                                                       category=None,
                                                                                       relax=False)
                        if condition_observation:
                            observations.append(condition_observation)

                    if not pd.isnull(row["Treatment Type"]):
                        med_admin = patient_transformer.create_medication_administration(_row=row,
                                                                                         patient_id=patient.id)
                        if med_admin:
                            med_admins.append(med_admin)
                            med_admin_observation = patient_transformer.create_observation(_row=row, patient=None,
                                                                                           official_focus="MedicationAdministration",
                                                                                           focus=[Reference(**{
                                                                                               "reference": f"MedicationAdministration/{med_admin.id}"})],
                                                                                           patient_id=patient.id,
                                                                                           specimen=None,
                                                                                           components=None,
                                                                                           category=None,
                                                                                           relax=False)
                            if med_admin_observation:
                                observations.append(med_admin_observation)

        specimens = []
        for specimen_index, specimen_row in htan_biospecimens.iterrows():
            # specimen_row = htan_biospecimens.iloc[specimen_index]
            specimen = specimen_transformer.create_specimen(_row=specimen_row)
            if specimen:
                specimens.append(specimen)

                participant_id = specimen_transformer.decipher_htan_id(specimen_row["HTAN Biospecimen ID"])[
                    "participant_id"]
                assert participant_id, f"Specimen {specimen_row["HTAN Biospecimen ID"]} does not have a patient participant associated with it."

                specimen_participant_id = specimen_transformer.get_patient_id(participant_id=participant_id)
                specimen_observation = specimen_transformer.create_observation(_row=specimen_row, patient=None,
                                                                               official_focus="Specimen",
                                                                               focus=[Reference(**{
                                                                                   "reference": f"Specimen/{specimen.id}"})],
                                                                               patient_id=specimen_participant_id,
                                                                               specimen=specimen, components=None,
                                                                               category=transformer.lab_category,
                                                                               relax=False)
                if specimen_observation:
                    observations.append(specimen_observation)

        specimen_ids = [s.id for s in specimens]
        patient_ids = [p.id for p in patients]
        document_references = []
        missing_docrefs = []
        groups = []
        for document_reference_index, document_reference_row in files_drs_meta.iterrows():
            _obj = documentreference_transformer.create_document_reference(_row=document_reference_row,
                                                                           specimen_ids=specimen_ids,
                                                                           case_patient_ids=patient_ids)

            if _obj:
                group = _obj["group"]
                if group:
                    groups.append(group)

                docref = _obj["file"]
                if docref:
                    document_references.append(docref)

                    docref_patient_id = None
                    if 'HTAN Participant ID' in document_reference_row.keys() and pd.isnull(
                            document_reference_row['HTAN Participant ID']):
                        docref_patient = documentreference_transformer.get_patient_id(
                            participant_id=document_reference_row['HTAN Participant ID'])
                        if docref_patient in patient_ids:
                            docref_patient_id = docref_patient

                    document_reference_observation = documentreference_transformer.create_observation(
                        _row=document_reference_row, patient=None,
                        official_focus="DocumentReference",
                        focus=[Reference(**{
                            "reference": f"DocumentReference/{docref.id}"})],
                        patient_id=docref_patient_id,
                        specimen=None, components=None,
                        category=transformer.lab_category,
                        relax=True)

                    if document_reference_observation:
                        observations.append(document_reference_observation)
            else:
                missing_docrefs.append(document_reference_row['Filename'])

        #  missing document references for debugging / checking relations on HTAN (user can create files that are missing patient information on HTAN)
        if missing_docrefs:
            print(f"⚠️ WARNING: {len(missing_docrefs)} DocumentReferences skipped due to missing Patient Relation")
            if len(missing_docrefs) <= 20:
                print(missing_docrefs)
            else:
                print(missing_docrefs[:10]) # print the first 10 for trial/error

        if project_research_study:
            entities = {'patient': patients,
                        'observations': observations,
                        'condition': conditions,
                        'research_studies': research_studies,
                        'research_subject': research_subjects,
                        'specimens': specimens,
                        'med_admin': med_admins,
                        'document_reference': document_references,
                        'groups': groups}


            for key, value in entities.items():
                if value:
                    entities[key] = utils.assign_part_of(entity=value, research_study_id=project_research_study.id)

        spinner.stop()

        if research_subjects:
            transformer.write_ndjson(research_subjects)
        if research_studies:
            transformer.write_ndjson(research_studies)
        if patients:
            transformer.write_ndjson(patients)
        if conditions:
            transformer.write_ndjson(conditions)
        if observations:
            transformer.write_ndjson(observations)
        if specimens:
            transformer.write_ndjson(specimens)
        if document_references:
            transformer.write_ndjson(document_references)
        if groups:
            transformer.write_ndjson(groups)
        if med_admins:
            transformer.write_ndjson(med_admins)
        # if encounters:
        #     transformer.write_ndjson(encounters)

# Useful cmds:
# for i in $(ls projects/HTAN); do fhirizer validate --path projects/HTAN/$i/META; done

# preview ndjson files in subdirectory:
# find projects/HTAN -type f -name "*.ndjson"
# delete them:
# find projects/HTAN -type f -name "*.ndjson" -delete
