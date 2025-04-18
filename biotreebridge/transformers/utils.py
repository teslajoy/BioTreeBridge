import os
import orjson
import time
import random
import sqlite3
import json
import glob
import gzip
import uuid
import pprint
import requests
from bs4 import BeautifulSoup
from importlib.resources import files
import importlib
from pathlib import Path
from fhir.resources.identifier import Identifier
from fhir.resources import get_fhir_model_class
from fhir.resources.group import Group, GroupMember
from fhir.resources.reference import Reference
from fhir.resources.extension import Extension
from fhir.resources.codeableconcept import CodeableConcept
from uuid import uuid5, UUID, uuid3, NAMESPACE_DNS
from typing import List
from fhir.resources.fhirresourcemodel import FHIRAbstractModel
import decimal
from collections import defaultdict



def _read_json(path):
    """
    Reads in json file

    :param path: path to json file
    :return:
    """

    try:
        with open(path, encoding='utf-8') as f:
            this_json = json.load(f)
            return this_json
    except json.JSONDecodeError as e:
        print("Error decoding JSON: {}".format(e))


#----------------------------------------------------------------------
# FHIR Utility functions
# --------------------------------------------------------------------------
# https://pypi.org/project/fhir.resources/ version 7.1.0 uses FHIR® (Release R5, version 5.0.0)
version = "5.0.0"


def clean_description(description):
    """
    top level description regex cleanup

    :param description: fhir class description txt
    :return: cleaned description txt
    """
    description = description.replace(
        "Disclaimer: Any field name ends with ``__ext`` doesn't part of\nResource StructureDefinition, instead used "
        "to enable Extensibility feature\nfor FHIR Primitive Data Types.\n\n",
        "")
    description = description.replace('\n\n', '\n')
    description = description.replace('\n', ' ')
    return description


def get_us_core(path=None, url=None, param=None):
    """
    Given a path or url to FHIR Extension.extension:[x] loads in data to map to :[x]

    :param param: Json dictionary
    :param path: Path to json
    :param url: url ex. https://build.fhir.org/ig/HL7/US-Core/StructureDefinition-us-core-ethnicity.json
    :return: TBD
    """

    if param is None:
        param = {}
    if path:
        with open(path, 'r') as file:
            for line in file:
                try:
                    json_data = json.loads(line)
                    return json_data
                except json.JSONDecodeError as e:
                    print("Error decoding JSON: {}".format(e))
    elif url:
        response = requests.get(url, param)

        if response.status_code == 200:
            html_content = response.content.decode("utf-8")
            soup = BeautifulSoup(html_content, 'lxml')
            return soup
    else:
        pass


def is_camel_case(name):
    """
    If the first letter of a word/key is camel case

    :param name: Name of FHIR module/property
    :return: boolean if name is not none and if it's first letter is uppercase
    """
    return name and name[0].isupper()


def decipher_relation(key_name_relation):
    """
    Splits key names by dot notation convention

    :param key_name_relation: string for the distination key name
    :return: list of key names
    """
    names = key_name_relation.split(".")
    return [is_camel_case(n) for n in names]


def has_extension(name):
    """
    Returns true if ':' exists in FHIR naming key convention

    :param name: key name
    :return: bool
    """
    return ":" in name


def schema_enum_reference_types(schem_properties):
    """
    Extracts all enum_reference_types from a FHIR schema property

    :param schem_properties: FHIR schema property
    :return: dictionary of property keys and list of module/nodes they reference to
    """
    d = {}
    for k, v in schem_properties.items():
        if "enum_reference_types" in v.keys():
            d.update({k: v["enum_reference_types"]})
    return d


def schema_element_required(schema_properties):
    """
    Extract element_required from a FHIR schema property and destination keys required

    :param schema_properties:
    :return:
    """
    d = {}
    for k, v in schema_properties.items():
        if "element_required" in v.keys():
            d.update({k: v["element_required"]})
    return d


def append_required_fhir_keys(element_required, required_keys):
    """
    Appends required keys to list of it doesn't exist in list

    :param element_required: dictionary of required elements of a schema property
    :param required_keys: list holding required keys dictionary
    :return: updated required key list
    """
    return [required_keys.append(obj) for obj in element_required if obj not in required_keys]


# -------------------------------------------------
# mapping.py utils
# -------------------------------------------------

def validate_and_write(schema, out_path, update=False, generate=False):
    Schema.model_validate(schema)

    schema_extra = schema.Config.json_schema_extra.get('$schema', None)
    schema_dict = schema.model_dump()
    schema_dict = {'$schema': schema_extra, **schema_dict}

    if os.path.exists(out_path) and update:
        with open(out_path, 'w') as json_file:
            json.dump(schema_dict, json_file, indent=4)
    elif generate:
        if os.path.exists(out_path):
            print(f"File: {out_path} exists.")
        else:
            with open(out_path, 'w') as json_file:
                json.dump(schema_dict, json_file, indent=4)
    else:
        print(f"File: {out_path} update not required")


def load_schema_from_json(path) -> Schema:
    with open(path, "r") as j:
        data = json.load(j)
        return Schema.model_validate(data)


def load_ndjson(path):
    try:
        with open(path, 'r') as file:
            obj = [json.loads(line) for line in file]
            return obj
    except json.JSONDecodeError as e:
        print(e)


def load_ndjsongz(path):
    try:
        with gzip.open(path, 'rt') as file:
            obj = [json.loads(line) for line in file]
            return obj
    except json.JSONDecodeError as e:
        print(e)
        return None


def is_deeply_nested_dict_list(nested_value):
    return isinstance(nested_value, list) and all(isinstance(item, dict) for item in nested_value)


def has_nested_lists_of_dicts(d):
    return any(isinstance(value, list) and any(isinstance(item, dict) for item in value) for value in d.values())


def similar_key_set(dat_dict, new_dict):
    return set(dat_dict.keys()).intersection(new_dict.keys())


def sort_parent_keys(data):
    sorted_keys = sorted(data.keys(), key=lambda k: (isinstance(data[k], (dict, list)), k))
    return {i: data[i] for i in sorted_keys}


def sort_parent_keys_with_head(data, head_key="Specimen.id.sample"):
    sorted_keys = sorted(data.keys(), key=lambda k: (k != head_key, isinstance(data[k], (dict, list)), k))
    return {i: data[i] for i in sorted_keys}


def append_data_to_key(data, target_key, data_to_append, verbose):
    if isinstance(data, dict):
        data = sort_parent_keys_with_head(data, head_key="Specimen.id.sample")
        # data = sort_parent_keys(data)

        for key, value in data.items():
            if verbose:
                print("=========== DICT KEY - append_data_to_key =============", key)
                print("=========== DICT TARGET KEY - append_data_to_key =============", target_key)
                print("=========== DICT VALUE - append_data_to_key =============", value)
                print("=========== DICT data[key] =============", data[key])
                print("=========== DICT data_to_append =============", data_to_append)
            if key == target_key:
                if data[key] and isinstance(data[key][0], dict):
                    if verbose:
                        print(f"======== instance Dict {target_key} ============== case A")

                    if (data[key][0].keys() == data_to_append.keys() and
                            not data_to_append.items() <= data[key][0].items()):
                        if verbose:
                            print(f"======== instance Dict {target_key} ============== case B")
                        data[key].append(data_to_append)
                        continue

                    # else the keys match
                    else:
                        shared_keys = None
                        if len(data[key]) > 1:
                            shared_keys = similar_key_set(data[key][-1], data_to_append)
                        elif len(data[key]) == 1:
                            shared_keys = similar_key_set(data[key][0], data_to_append)
                        if verbose:
                            print(f"=========== THE KEYS MATCH {target_key} ============= case B", "data[key][0]")
                            pprint.pprint(data[key][0])
                            print("data_to_append")
                            pprint.pprint(data_to_append)

                        # which keys match?
                        if shared_keys:
                            shared_keys_items = next(iter(shared_keys))
                            if verbose:
                                print(f"======== instance Dict {target_key} ============== case C", "shared_keys: ",
                                      shared_keys)

                            if isinstance(data[key][0][shared_keys_items], str) and isinstance(
                                    data_to_append[shared_keys_items], str) and data[key][0][shared_keys_items] != \
                                    data_to_append[shared_keys_items]:

                                reached = False
                                for item in data[key]:
                                    if verbose:
                                        print("Specimen.id" in item.keys())
                                        print(len(item.keys()))

                                    if len(item.keys()) == 1 and "Specimen.id" in list(item.keys())[
                                        0] and data_to_append.keys() != item.keys():
                                        # this is where metadata is updated if the head key with Specimen.id exists
                                        item.update(data_to_append)
                                        reached = True
                                        continue

                                if not reached:
                                    # this is where first Specimen.id is appended
                                    data[key].append(data_to_append)
                                if verbose:
                                    print(f"======== instance Dict {target_key} ============== case D AFTER",
                                          "data[key]: ", data[key], "\n\n")
                                continue

                            elif isinstance(data[key][0][shared_keys_items], str) and isinstance(
                                    data_to_append[shared_keys_items], str) and data[key][0][shared_keys_items] == \
                                    data_to_append[shared_keys_items]:
                                if len(data[key]) == 1:
                                    data[key].append(data_to_append)
                                else:
                                    # check membership
                                    if not data_to_append.items() <= data[key][-1].items():
                                        # print("00000")
                                        data[key][-1].update(data_to_append)

                                # print(f"======== instance Dict {target_key} ============== case E AFTER", "data[key]: ", data[key], "\n\n")
                                continue

                            elif isinstance(data[key][0][shared_keys_items], list) and isinstance(
                                    data_to_append[shared_keys_items], list):
                                if verbose:
                                    print("=========== data subset but not the same =============")

                                for d in data[key]:
                                    if verbose:
                                        print("==== d ====", d, "\n")
                                    if len(d.keys()) == 1:
                                        d.update(data_to_append)  # update parent key
                                        continue
                                    elif 'portions' in data_to_append.keys():
                                        if 'portions' not in d.keys():
                                            d.update(data_to_append)
                                            continue
                                    elif 'aliquots' in data_to_append.keys() - d.keys():
                                        print("data_to_append.keys() - d.keys()", data_to_append.keys() - d.keys())
                                        d.update(data_to_append)
                                        continue
                                if verbose:
                                    print(f"======== instance Dict {target_key} ============== case F After",
                                          "data[key]: ", data[key], "\n\n")
                                continue

                            elif (isinstance(data[key][0][shared_keys_items], list) and isinstance(
                                    data_to_append[shared_keys_items], list) and
                                  not data[key][0][shared_keys_items][0].items() <= data_to_append[shared_keys_items][
                                      0].items()):
                                if verbose:
                                    print(f"======== instance Dict {target_key} ============== case G", "data[key]: ",
                                          data[key])

                        if data[key][0]:
                            if len(data[key]) > 1 and len(data[key][-1]) == 1:
                                data[key][-1].update(data_to_append)
                                continue

                            for i, item in enumerate(data[key]):
                                if (isinstance(item, dict)
                                        and not set(data_to_append.keys()).intersection(set(item.keys()))
                                        and not data_to_append.items() <= item.items()):
                                    item.update(data_to_append)
                                    if verbose:
                                        print(f"======== instance Dict {target_key} ============== case H AFTER",
                                              "item: ", item, "\n\n")
                                    continue

                        elif (data[key] and key == "samples"
                              and not data[key][0].items() <= data_to_append.items()
                              and not data_to_append.items() <= data[key][0].items()):
                            print(f"======== instance Dict {target_key} ============== I")
                            data[key].append(data_to_append)
                            continue

                elif isinstance(data[key], list):
                    if verbose:
                        print(f"======== instance LIST {target_key} ============== J", "data_key", data[key])
                        print("===== DATA TO APPEND: ", data_to_append)
                    data[key].append(data_to_append)
                    continue

    elif isinstance(data, list):
        if verbose:
            print(f"======== instance LIST  {target_key} ============== K", "\n\n")
        for i, value in enumerate(data):
            value = sort_parent_keys_with_head(value, head_key="Specimen.id.sample")
            # value = sort_parent_keys(value)
            append_data_to_key(value, target_key, data_to_append, verbose)


def process_nested_list(traverse_key, nested_value, current_keys, available_maps, verbose):
    tks = traverse_key.split(".")
    tks = tks[-1]
    this_nest = {tks: []}

    for elm in nested_value:
        if isinstance(elm, dict):
            # print(f"tks: {tks}")
            if tks == "diagnoses": # tmp fix
                elm = sort_parent_keys_with_head(elm, head_key="diagnosis_id")
            else:
                elm = sort_parent_keys_with_head(elm, head_key="sample_id")
            # elm = sort_parent_keys(elm)

            for key, value in elm.items():
                if isinstance(value, list):
                    current_key = '.'.join(current_keys + [traverse_key] + [key])
                    tks = traverse_key.split(".")
                    tks = tks[-1]

                    result = process_nested_list(current_key, value, current_keys, available_maps, verbose)
                    if verbose:
                        print("----- RESULT ----- ", type(result), result)
                    append_data_to_key(this_nest, tks, result, verbose)
                    continue

                current_key = '.'.join(current_keys + [traverse_key] + [key])
                schema_map = next((m for m in available_maps if m and m.source.name == current_key), None)

                if schema_map:
                    destination_key = schema_map.destination.name

                    if not is_deeply_nested_dict_list(value) and not isinstance(value, list):
                        if isinstance(this_nest[tks], list):
                            append_data_to_key(this_nest, tks, {destination_key: value}, verbose)
                        elif isinstance(this_nest[tks], dict):
                            append_data_to_key(this_nest, tks, {destination_key: value}, verbose)

    return this_nest


def traverse_and_map(node, current_keys, mapped_data, available_maps, changed_key, verbose):
    for key, value in node.items():
        is_nested_list = is_deeply_nested_dict_list(value)

        if is_nested_list:
            # print(f" Key: {key} \n Value: {value}\n current_keys: {current_keys}\n available_maps: {available_maps}")
            maps = process_nested_list(key, value, current_keys, available_maps, verbose)
            mapped_data.update(maps)

            if verbose:
                print("********** is_nested_list: ", is_nested_list, "key: ", key, "value: ", value, "\n")
                print("--- maps ---- ", maps)
                print("--- All Done ---- ")
            continue

        if isinstance(changed_key, tuple):
            # swap parent key that was changed to FHIR back to GDC
            current_key = '.'.join([changed_key[0]] + [key])
        else:
            current_key = '.'.join(current_keys + [key])
        schema_map = next((m for m in available_maps if m and m.source.name == current_key), None)

        if schema_map:
            # fetch the Map's destination
            destination_key = schema_map.destination.name
            # separate hierarchy key to track
            hierarchy_key = current_keys[0] if current_keys else None

            if hierarchy_key and hierarchy_key not in mapped_data:
                mapped_data[hierarchy_key] = {}

            current_dat = mapped_data
            for nested_key in current_keys:
                if nested_key not in current_dat and nested_key not in mapped_data.keys():
                    current_dat[nested_key] = {}
                if nested_key not in current_dat and nested_key in mapped_data.keys():
                    current_dat.update({nested_key: {destination_key: value}})
                else:
                    current_dat = current_dat[nested_key]
                if verbose:
                    print("current_dat: ", current_dat, "\n")

            if destination_key not in current_dat and not isinstance(current_dat, list):
                # check Map's destination
                if not isinstance(value, dict):
                    current_dat[destination_key] = value

                if isinstance(value, list):
                    current_dat[destination_key] = value

                if verbose:
                    print("assigned destination and it's value: ", current_dat, "\n")

                if isinstance(value, dict):
                    if verbose:
                        print("instance dict - recall (DICT):", current_keys + [key], "\n")
                    traverse_and_map(value, current_keys + [destination_key], mapped_data, available_maps,
                                     changed_key=(current_key, destination_key), verbose=verbose)

        elif isinstance(value, dict):
            if verbose:
                print("instance dict - recall:", current_keys + [key], "\n")
            traverse_and_map(value, current_keys + [key], mapped_data, available_maps,
                             changed_key=changed_key, verbose=verbose)


def map_data(data, available_maps, verbose):
    mapped_data = {}
    traverse_and_map(data, [], mapped_data, available_maps, changed_key=None, verbose=verbose)
    if verbose:
        print('Available Map items of entity: ', len(available_maps), '\n')
    return {'mapped_data': mapped_data}


# Cellosaurus

def make_request(api_url, retries=3):
    delay = 0.5
    for _ in range(retries):
        response = requests.get(api_url)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Received status code: {response.status_code}. Retrying...")
            delay *= 2 ** retries  # change delay
            time.sleep(delay + random.uniform(0, 1))  # add jitter
    raise Exception("Failed to fetch data after multiple retries")


def write_dat(dat, path):
    json_dat = json.dumps(dat, indent=4)
    with open(path, "w") as f:
        f.write(json_dat)


def fetch_cellines(cellosaurus_ids, out_dir):
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    existing_ids = set(
        os.path.splitext(os.path.basename(file))[0] for file in os.listdir(out_dir) if file.endswith('.json'))
    to_fetch_ids = set(cellosaurus_ids) - existing_ids

    for cellosaurus_id in to_fetch_ids:
        file_name = "".join([out_dir, f'{cellosaurus_id}.json'])
        api_url = f"https://api.cellosaurus.org/cell-line/{cellosaurus_id}?format=json"

        try:
            response_data = make_request(api_url)
            write_dat(response_data, file_name)
            print(f"Json dat for {cellosaurus_id} successfully fetched and saved.")
        except Exception as e:
            print(f"Error fetching data for {cellosaurus_id}: {e}")


def fetch_cellines_by_id(cellosaurus_id, out_path, save=False):
    api_url = f"https://api.cellosaurus.org/cell-line/{cellosaurus_id}?format=json"
    response = requests.get(api_url)

    dat = None
    if response.status_code == 200:
        dat = response.json()

    if dat and save:
        celline_json = json.dumps(dat, indent=4)
        with open("".join([out_path, cellosaurus_id, ".json"]), "w") as outfile:
            outfile.write(celline_json)
    else:
        return dat


def cellosaurus_cancer_ids(path, out_path, save=False):
    # condition -- subject --> patient <-- subject -- specimen
    cl = load_ndjsongz(path=path)
    cl_cancer = []
    cl_cancer_depmap = []
    ids = []

    # human
    cl_human = [d for d in cl if "NCBI_TaxID:9606:Homo sapiens:Human" in d["xref"]]

    # cancer
    for celline in cl_human:
        for item in celline["xref"]:
            if item.startswith("NCIt:"):
                cl_cancer.append(celline)

    # depmap reference file
    for celline in cl_cancer:
        for item in celline["xref"]:
            if item.startswith("DepMap:"):
                cl_cancer_depmap.append(celline)

    # has sex annotation
    for celline in cl_cancer_depmap:
        for subset in celline["subset"]:
            if subset in ["Female", "Male"]:
                ids.append(celline["id"][0])

    # 67763 cell-lines
    # 62019 cell-lines w gender
    # 1733 ids referenced in DepMap - broad Cancer Cell Line Encyclopedia (CCLE)
    ids = list(set(ids))

    if save:
        write_dat(ids, out_path)

    return ids


def cellosaurus_cancer_jsons(out_path):
    if "/" in out_path[-1]:
        out_path = out_path[:-1]

    all_paths = glob.glob("".join([out_path, "**/*.json"]))

    cell_lines = []
    for file in all_paths:
        dat = _read_json(file)
        if dat:
            cell_lines.append(dat)

    if cell_lines:
        return cell_lines


def ncit2mondo(path):
    with gzip.open(path, 'r') as fin:
        data = json.loads(fin.read().decode('utf-8'))
        return data


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


def get_component(key, value=None, component_type=None,
                  system="https://cadsr.cancer.gov/sample_laboratory_observation"):
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


def fhir_ndjson(entity, out_path):
    if isinstance(entity, list):
        with open(out_path, 'w', encoding='utf8') as file:
            file.write('\n'.join(map(lambda e: json.dumps(e, ensure_ascii=False), entity)))
    else:
        with open(out_path, 'w', encoding='utf8') as file:
            file.write(json.dumps(entity, ensure_ascii=False))


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


def is_valid_fhir_resource_type(resource_type):
    try:
        model_class = get_fhir_model_class(resource_type)
        return model_class is not None
    except KeyError:
        return False


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
    """Creates a research study group based on:
    https://nih-ncpi.github.io/ncpi-fhir-ig-2/StructureDefinition-research-study-group.html
    """
    code = CodeableConcept(**{"coding": [{"code": "C142710",
                                          "system": "http://purl.obolibrary.org/obo/ncit.owl",
                                          "display": "Study Participant"}]})

    patients_ids = [p.reference.replace("Patient/", "") for p in patient_references]
    group_identifier = Identifier(
        **{"system": "".join(["https://gdc.cancer.gov/", "sample_group"]),
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
    project_id = "GDC"
    NAMESPACE_GDC = uuid3(NAMESPACE_DNS, 'gdc.cancer.gov')
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
            study_group = create_researchstudy_group(study_patient_references, study_name=study_submitter_id, project_id=project_id, namespace=NAMESPACE_GDC)
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
    print(f"Successfully transformed GDC case info to FHIR's ResearchSubject's Group ndjson file!")

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
    TODO: use this for all other calls to load_ndjson or load_ndjsongz
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
        # _dat = [orjson.loads(fhir_dat.model_dump_json()) for fhir_dat in entries]
        # fhir_ndjson(_dat, output_file)

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
        "url": "http://fhir-aggregator.org/fhir/StructureDefinition/part-of-study",
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
