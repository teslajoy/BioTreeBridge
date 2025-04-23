import os
import orjson
import sqlite3
import json
import gzip
import uuid
from importlib.resources import files
import importlib
from pathlib import Path
from fhir.resources.identifier import Identifier
from fhir.resources import get_fhir_model_class
from fhir.resources.group import Group, GroupMember
from fhir.resources.reference import Reference
from fhir.resources.codeableconcept import CodeableConcept
from uuid import uuid5, UUID, uuid3, NAMESPACE_DNS
from typing import List
from fhir.resources.fhirresourcemodel import FHIRAbstractModel
import decimal
from collections import defaultdict


project_id = "HTAN2_BForePC"
NAMESPACE_HTAN = uuid3(NAMESPACE_DNS, 'humantumoratlas.org')


def mint_id(identifier, resource_type, project_id, namespace) -> str:
    """Create a UUID from an identifier. - mint id via Walsh's convention
    https://github.com/ACED-IDP/g3t_etl/blob/d095895b0cf594c2fd32b400e6f7b4f9384853e2/g3t_etl/__init__.py#L61"""

    if isinstance(identifier, Identifier):
        assert resource_type, "resource_type is required for Identifier"
        identifier = f"{resource_type}/{identifier.system}|{identifier.value}"
    return _mint_id(identifier, project_id, namespace)


def _mint_id(identifier_string: str, project_id: str, namespace: UUID) -> str:
    """Create a UUID from an identifier, insert project_id."""
    return str(uuid5(namespace, f"{project_id}/{identifier_string}"))


def fhir_ndjson(entity, out_path):
    if isinstance(entity, list):
        with open(out_path, 'w', encoding='utf8') as file:
            file.write('\n'.join(map(lambda e: json.dumps(e, ensure_ascii=False), entity)))
    else:
        with open(out_path, 'w', encoding='utf8') as file:
            file.write(json.dumps(entity, ensure_ascii=False))


def is_valid_fhir_resource_type(resource_type):
    try:
        model_class = get_fhir_model_class(resource_type)
        return model_class is not None
    except KeyError:
        return False


def get_data_types(data_type):
    if data_type in ['int64', 'int32', 'int16', 'int']:
        return 'int'
    elif data_type in ['float64', 'float32', 'float16', 'float']:
        return 'float'
    elif data_type in ['str', 'string']:
        return 'string'
    elif data_type == 'bool':
        return 'bool'
    elif data_type in ['datetime64[ns]', 'timedelta64[ns]', 'period', 'datetime', 'date']:
        return 'dateTime'
    else:
        print(f"New or Null Data type: {data_type}.")
        return data_type


def create_or_extend(new_items, folder_path='META', resource_type='Observation', update_existing=False):
    assert is_valid_fhir_resource_type(resource_type), f"Invalid resource type: {resource_type}"

    file_name = "".join([resource_type, ".ndjson"])
    file_path = os.path.join(folder_path, file_name)

    file_existed = os.path.exists(file_path)

    existing_data = {}

    if file_existed:
        with open(file_path, 'r') as file:
            for line in file:
                try:
                    item = orjson.loads(line)
                    existing_data[item.get("id")] = item
                except orjson.JSONDecodeError:
                    continue

    for new_item in new_items:
        new_item_id = new_item["id"]
        if new_item_id not in existing_data or update_existing:
            existing_data[new_item_id] = new_item

    with open(file_path, 'w') as file:
        for item in existing_data.values():
            file.write(orjson.dumps(item).decode('utf-8') + '\n')

    if file_existed:
        if update_existing:
            print(f"{file_name} has new updates to existing data.")
        else:
            print(f"{file_name} has been extended, without updating existing data.")
    else:
        print(f"{file_name} has been created.")


def get_chembl_compound_info(db_file_path: str, drug_names: list, limit: int) -> list:
    """Query Chembl COMPOUND_RECORDS by COMPOUND_NAME to make FHIR Substance"""
    assert drug_names, "The drug_names list is empty. Please provide at least one drug name."

    if len(drug_names) == 1:
        _drug_names = f"('{drug_names[0].upper()}')"
    else:
        _drug_names = tuple([x.upper() for x in drug_names])

    query = f"""
    SELECT DISTINCT 
        a.CHEMBL_ID,
        c.STANDARD_INCHI,
        c.CANONICAL_SMILES,
        cr.COMPOUND_NAME
    FROM 
        MOLECULE_DICTIONARY as a
    LEFT JOIN 
        COMPOUND_STRUCTURES as c ON a.MOLREGNO = c.MOLREGNO
    LEFT JOIN 
        ACTIVITIES as p ON a.MOLREGNO = p.MOLREGNO
    LEFT JOIN 
        compound_records as cr ON a.MOLREGNO = cr.MOLREGNO
    LEFT JOIN
        source as sr ON cr.SRC_ID = sr.SRC_ID
    WHERE cr.COMPOUND_NAME IN {_drug_names}
    LIMIT {limit};
    """

    conn = sqlite3.connect(db_file_path)
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    conn.close()

    return rows

def create_researchstudy_group(patient_references: list, study_name: str, project_id: str, namespace) -> Group:
    """Creates a research study group based on FHIR NCPI WG standards:
    https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-research-study-group.html
    """
    code = CodeableConcept(**{"coding": [{"code": "C142710",
                                          "system": "http://purl.obolibrary.org/obo/ncit.owl",
                                          "display": "Study Participant"}]})

    patients_ids = [p.reference.replace("Patient/", "") for p in patient_references]
    group_identifier = Identifier(
        **{"system": "".join(["https://humantumoratlas.org/", "sample_group"]),
           "value": "/".join([study_name] + patients_ids),
           "use": "official"})

    group_id = mint_id(identifier=group_identifier, resource_type="Group",
                       project_id=project_id,
                       namespace=namespace)

    patient_members = [GroupMember(**{'entity': p}) for p in patient_references]
    study_group = Group(**{"id": group_id, "identifier": [group_identifier],
                           "code": code, "membership": "definitional",
                           "type": "person", "member": patient_members})
    return study_group


def load_ndjson(path):
    try:
        with open(path, 'r') as file:
            obj = [json.loads(line) for line in file]
            return obj
    except json.JSONDecodeError as e:
        print(e)

def study_groups(meta_path: str, out_path: str) -> List[Group]:
    assert os.path.exists(meta_path), "META folder for ResearchStudy, ResearchSubject, and Patient ndjson files path doesn't exist."
    assert os.path.exists(out_path), "Path Does not exist."

    researchstudy = load_ndjson(os.path.join(meta_path, "ResearchStudy.ndjson"))
    researchsubjects = load_ndjson(os.path.join(meta_path, "ResearchSubject.ndjson"))
    patients = load_ndjson(os.path.join(meta_path, "Patient.ndjson"))

    study_info = {}
    for study in researchstudy:
        submitter_id = [r['value'] for r in study['identifier'] if r['use'] == 'official'][0]
        study_info.update({study['id']: submitter_id})

    l = []
    groups = []
    for study_id, study_submitter_id in study_info.items():
        study_researchsubjects = {study_id: []}
        study_patient_references = []
        study_group = None
        for researchstubject in researchsubjects:
            if researchstubject['study']['reference'].replace("ResearchStudy/", "") == study_id:
                for patient in patients:
                    if researchstubject["subject"]['reference'].replace("Patient/", "") == patient['id']:
                        study_researchsubjects[study_id].append(researchstubject['id'])
                        study_patient_references.append(Reference(**({"reference": f"Patient/{patient['id']}"})))
        if len(study_patient_references) > 0:
            study_group = create_researchstudy_group(study_patient_references, study_name=study_submitter_id, project_id=project_id, namespace=NAMESPACE_HTAN)
        if study_group:
            print(f"Created Group for {study_submitter_id}")
            groups.append(study_group)
        print(f"ReseachStudy: {study_submitter_id} with N = {len(study_researchsubjects[study_id])} ResearchSubjects.")
        l.append(study_researchsubjects)

    json_groups = [orjson.loads(group.model_dump_json()) for group in groups]

    def deduplicate_entities(_entities):
        return list({v['id']: v for v in _entities}.values())

    json_groups = deduplicate_entities(json_groups)
    fhir_ndjson(json_groups, f"{out_path}/Group.ndjson")
    print(f"Successfully transformed HTAN cases info to FHIR's ResearchSubject's Group ndjson file!")

    return groups


def remove_empty_dicts(data):
    """
    Recursively remove empty dictionaries and lists from nested data structures.
    """
    if isinstance(data, dict):
        new_data = {}
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                cleaned = remove_empty_dicts(v)
                # keep non-empty structures or zero
                if cleaned or cleaned == 0:
                    new_data[k] = cleaned
            # keep values that are not empty or zero
            elif v or v == 0:
                new_data[k] = v
        return new_data

    elif isinstance(data, list):
        cleaned_list = [remove_empty_dicts(item) for item in data]
        cleaned_list = [item for item in cleaned_list if item or item == 0]  # remove empty items
        return cleaned_list if cleaned_list else None  # return none if list is empty

    else:
        return data


def convert_decimal_to_float(data):
    """Convert pydantic Decimal to float"""
    if isinstance(data, dict):
        return {k: convert_decimal_to_float(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [convert_decimal_to_float(item) for item in data]
    elif isinstance(data, decimal.Decimal):
        return float(data)
    else:
        return data


def convert_value_quantity_to_float(data):
    """
    Recursively converts all 'valueQuantity' -> 'value' fields in a nested dictionary or list
    from strings to floats.
    """
    if isinstance(data, list):
        return [convert_value_quantity_to_float(item) for item in data]
    elif isinstance(data, dict):
        for key, value in data.items():
            if key == 'valueQuantity' and isinstance(value, dict) and 'value' in value:
                if isinstance(value['value'], str):
                    # and value['value'].replace('.', '', 1).isdigit():
                    value['value'] = float(value['value'])
            else:
                data[key] = convert_value_quantity_to_float(value)
    return data


def convert_value_to_float(data):
    """
    Recursively converts all general 'entity' -> 'value' fields in a nested dictionary or list
    from strings to float or int.
    """
    if isinstance(data, list):
        return [convert_value_to_float(item) for item in data]
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, dict) and 'value' in value:
                if isinstance(value['value'], str):
                    if value['value'].replace('.', '').replace('-', '', 1).isdigit() and "." in value['value']:
                        value['value'] = float(value['value'])
                    elif value['value'].replace('.', '').replace('-', '', 1).isdigit() and "." not in value['value']:
                        value['value'] = int(value['value'])
            else:
                data[key] = convert_value_to_float(value)
    return data


def validate_fhir_resource_from_type(resource_type: str, resource_data: dict) -> FHIRAbstractModel:
    """
    Generalized function to validate any FHIR resource type using its name.
    """
    try:
        resource_module = importlib.import_module(f"fhir.resources.{resource_type.lower()}")
        resource_class = getattr(resource_module, resource_type)
        return resource_class.model_validate(resource_data)

    except (ImportError, AttributeError) as e:
        raise ValueError(f"Invalid resource type: {resource_type}. Error: {str(e)}")


def clean_resources(entities):
    cleaned_resource = []
    for resource in entities:
        resource_type = resource["resourceType"]
        cleaned_resource_dict = remove_empty_dicts(resource)
        try:
            validated_resource = validate_fhir_resource_from_type(resource_type, cleaned_resource_dict).model_dump_json()
        except ValueError as e:
            print(f"Validation failed for {resource_type}: {e}")
            continue
        # handle pydantic Decimal cases
        validated_resource = convert_decimal_to_float(orjson.loads(validated_resource))
        validated_resource = convert_value_to_float(validated_resource)
        validated_resource = orjson.loads(orjson.dumps(validated_resource).decode("utf-8"))
        cleaned_resource.append(validated_resource)

    return cleaned_resource


def read_ndjson(file_path):
    """
    Load an NDJSON file.
    """
    data = []
    open_func = gzip.open if file_path.endswith('.gz') else open
    with open_func(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            data.append(json.loads(line))
    return data


def consolidate_fhir_data(base_dir, output_dir):
    """Load, deduplicate, and integrate FHIR NDJSON files from META folders."""
    def save_ndjson(data, file_path):
        """Save data to an NDJSON file, ensuring it is unique."""
        with open(file_path, 'w', encoding='utf-8') as f:
            for entry in data:
                f.write(json.dumps(entry) + "\n")

    resource_data = defaultdict(dict)  # {resource_type: {id: resource}}

    for root, dirs, _files in os.walk(base_dir):
        if 'META' in dirs:
            meta_path = os.path.join(root, 'META')
            print(f"Processing META folder: {meta_path}")

            for file in os.listdir(meta_path):
                if file.endswith('.ndjson') or file.endswith('.ndjson.gz'):
                    resource_type = file.split('.')[0]
                    file_path = os.path.join(meta_path, file)
                    resource_id = None
                    try:
                        data = read_ndjson(file_path)
                        for entry in data:
                            resource_id = entry.get('id')

                            if resource_id:
                                resource_data[resource_type][resource_id] = entry

                    except Exception as e:
                        print(f"Error processing {file_path}: {e}")

    os.makedirs(output_dir, exist_ok=True)

    for resource_type, entries in resource_data.items():
        output_file = os.path.join(output_dir, f"{resource_type}.ndjson")
        print(f"Saving {resource_type} data to {output_file} with {len(entries)} unique records.")
        save_ndjson(entries.values(), output_file)

    print("FHIR NDJSON consolidation completed.")


def add_extension(entity, extension):
    if isinstance(entity, list):
        return [add_extension(item, extension) for item in entity]

    if isinstance(entity, dict):
        if "extension" in entity and isinstance(entity["extension"], list):
            entity["extension"].append(extension)
        else:
            entity["extension"] = [extension]
        return entity

    if hasattr(entity, "extension"):
        if entity.extension and isinstance(entity.extension, list):
            entity.extension.append(extension)
        else:
            entity.extension = [extension]
        return entity

    raise ValueError(f"Unsupported entity type: {type(entity)}")


def assign_part_of(entity, research_study_id):
    part_of_study_extension = {
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study", # TODO: check url namespace
        "valueReference": {"reference": f"ResearchStudy/{research_study_id}"}
    }

    def get_extension_url(ext):
        if isinstance(ext, dict):
            return ext.get("url")
        return getattr(ext, "url", None)

    if isinstance(entity, dict):
        extensions = entity.get("extension", [])
    elif hasattr(entity, "extension"):
        extensions = entity.extension if entity.extension else []
    elif isinstance(entity, list):
        for item in entity:
            assign_part_of(item, research_study_id)
        return entity
    else:
        raise ValueError(f"Unsupported entity type: {type(entity)}")

    if not any(get_extension_url(ext) == part_of_study_extension["url"] for ext in extensions):
        add_extension(entity, part_of_study_extension)

    return entity


def get_component(key, value=None, component_type=None, system='https://cadsr.cancer.gov/sample_laboratory_observation'):
        # TODO: change system
        if component_type == 'string':
            value = {"valueString": value}
        elif component_type == 'int':
            value = {"valueInteger": value}
        elif component_type == 'float':
            value = {"valueQuantity": {"value": value}}
        elif component_type == 'bool':
            value = {"valueBoolean": value}
        elif component_type == 'dateTime':
            value = {"valueDateTime": value}
        else:
            pass

        component = {
            "code": {
                "coding": [
                    {
                        "system": system,
                        "code": key,
                        "display": key
                    }
                ],
                "text": key
            }
        }

        if value:
            component.update(value)

        return component

def csv2ndjson(dat):
    return