import json
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any, Set
import requests


class BioThingsSchemaParser:
    def __init__(self, source: str):
        """
        load JSON‑LD from a URL or a local file path. normalize so that self.graph is always a list of node dicts.
        """
        if source.startswith(("http://", "https://")):
            resp = requests.get(source)
            resp.raise_for_status()
            self.schema = resp.json()
        else:
            with open(source, "r", encoding="utf-8") as fp:
                self.schema = json.load(fp)

        if isinstance(self.schema, list):
            self.graph = self.schema
        else:
            self.graph = (
                    self.schema.get("@graph")
                    or self.schema.get("graph")
                    or []
            )

    @staticmethod
    def _strip_prefix(curie: str) -> str:
        """
        turn txt like "bts:Sample" into "Sample". if no colon present, returns the string unchanged.
        """
        return curie.split(":", 1)[1] if ":" in curie else curie

    def extract_subclass_relationships(self) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """
        scan all nodes for any key ending in "subClassOf" (case‑insensitive).
        returns two dicts:
          - parents_to_children: { parent_id: [ child_id, ... ], ... }
          - children_to_parents: { child_id: [ parent_id, ... ], ... }
        """
        parents_to_children: Dict[str, List[str]] = defaultdict(list)
        children_to_parents: Dict[str, List[str]] = defaultdict(list)

        for node in self.graph:
            raw_cid = node.get("@id")
            if not raw_cid:
                continue
            cid = self._strip_prefix(raw_cid)

            # find any predicate key that ends with "subClassOf"
            for key in (k for k in node if k.lower().endswith("subclassof")):
                raw_val = node[key]

                # normalize raw parent references into a list of raw IDs
                parents_raw: List[str] = []
                if isinstance(raw_val, dict) and "@id" in raw_val:
                    parents_raw.append(raw_val["@id"])
                elif isinstance(raw_val, list):
                    for item in raw_val:
                        if isinstance(item, dict) and "@id" in item:
                            parents_raw.append(item["@id"])
                        elif isinstance(item, str):
                            parents_raw.append(item)
                elif isinstance(raw_val, str):
                    parents_raw.append(raw_val)

                # strip prefixes and record each parent < - > child link
                for raw_pid in parents_raw:
                    pid = self._strip_prefix(raw_pid)
                    parents_to_children[pid].append(cid)
                    children_to_parents[cid].append(pid)

        return dict(parents_to_children), dict(children_to_parents)

    def get_children(self, parent_id: str, recursive: bool = False) -> List[str]:
        """
        return direct children of a `parent_id`. if recursive=True, returns all descendants via BFS.
        """
        p2c, _ = self.extract_subclass_relationships()
        if not recursive:
            return p2c.get(parent_id, [])
        seen, queue, out = {parent_id}, [parent_id], []
        while queue:
            current = queue.pop(0)
            for child in p2c.get(current, []):
                if child not in seen:
                    seen.add(child)
                    out.append(child)
                    queue.append(child)
        return out

    def get_parents(self, child_id: str, recursive: bool = False) -> List[str]:
        """
        return direct parents of `child_id`. if recursive=True, returns all ancestors via BFS.
        """
        _, c2p = self.extract_subclass_relationships()
        if not recursive:
            return c2p.get(child_id, [])
        seen, queue, out = {child_id}, [child_id], []
        while queue:
            current = queue.pop(0)
            for parent in c2p.get(current, []):
                if parent not in seen:
                    seen.add(parent)
                    out.append(parent)
                    queue.append(parent)
        return out

    def get_children_hierarchy(self, parent_id: str, max_depth: int = -1, include_attrs: bool = False) -> Dict:
        """
        build and return a nested dict representing the tree under `parent_id`.

        format:
        {
          "id": "NodeID",
          "name": "Node Name",
          "requirements": [
            {"required": true/false},
            {"components": [{...}, ...]},
            {"dependencies": [{...}, ...]}
          ],
          "children": [
            {
              "id": "ChildID",
              ...
            },
            ...
          ]
        }

        If include_attrs is True, includes the requirements array with detailed information.
        """
        p2c, _ = self.extract_subclass_relationships()

        def build_tree(node_id: str, depth=0) -> Dict:
            node_with_prefix = f"bts:{node_id}" if ":" not in node_id else node_id

            tree = {"id": self._strip_prefix(node_with_prefix)}
            node = self.get_node(node_with_prefix)

            if node:
                name = node.get("rdfs:label") or node.get("schema:name")
                if name:
                    tree["name"] = name

            if max_depth != -1 and depth >= max_depth:
                return tree

            if include_attrs and node:
                requirements = []

                requirements.append({
                    "required": node.get("sms:required") == "sms:true"
                })

                if "sms:requiresComponent" in node:
                    component_ids = self._extract_reference_ids(node.get("sms:requiresComponent"))
                    if component_ids:
                        components = []
                        for comp_id in component_ids:
                            comp_with_prefix = f"bts:{comp_id}" if ":" not in comp_id else comp_id
                            comp_name = self.get_name(comp_with_prefix)

                            components.append({
                                "id": comp_id,
                                "name": comp_name if comp_name != comp_with_prefix else comp_id,
                                "required": self.is_required(comp_with_prefix)
                            })

                        requirements.append({
                            "components": components
                        })

                if "sms:requiresDependency" in node:
                    dependency_ids = self._extract_reference_ids(node.get("sms:requiresDependency"))
                    if dependency_ids:
                        dependencies = []
                        for dep_id in dependency_ids:
                            dep_with_prefix = f"bts:{dep_id}" if ":" not in dep_id else dep_id
                            dep_name = self.get_name(dep_with_prefix)

                            dependencies.append({
                                "id": dep_id,
                                "name": dep_name if dep_name != dep_with_prefix else dep_id,
                                "required": self.is_required(dep_with_prefix)
                            })

                        requirements.append({
                            "dependencies": dependencies
                        })

                if requirements:
                    tree["requirements"] = requirements

            stripped_id = self._strip_prefix(node_with_prefix)
            children = p2c.get(stripped_id, [])
            tree["children"] = [
                build_tree(child_id, depth + 1) for child_id in children
            ]

            return tree

        return build_tree(parent_id)

    def get_roots(self) -> List[str]:
        """
        get all root nodes (those without parents).
        """
        _, c2p = self.extract_subclass_relationships()
        all_nodes = set([self._strip_prefix(node.get('@id')) for node in self.graph if node.get('@id')])
        root_nodes = [node for node in all_nodes if node not in c2p]
        return root_nodes

    def get_name(self, node_id: str) -> str:
        """retrieve the name for a node based on its ID"""
        node = next((n for n in self.graph if n.get('@id') == node_id), None)
        if node:
            return node.get("rdfs:label") or node.get("schema:name") or node_id
        return node_id

    def search(self, term: str) -> List[str]:
        """
        search for nodes by name or ID. returns a list of matching node IDs.
        """
        term = term.lower()
        results = []

        for node in self.graph:
            node_id = node.get("@id")
            if not node_id:
                continue
            name = node.get("rdfs:label") or node.get("schema:name") or ""
            if term in name.lower() or term in node_id.lower():
                results.append(node_id)

        return results

    def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        get the node dictionary by its ID.
        returns None if the node is not found.
        """
        return next((n for n in self.graph if n.get('@id') == node_id), None)

    def _extract_reference_ids(self, value: Any) -> List[str]:
        """
        extract reference IDs from different possible value formats:
        - dict with @id
        - list of dicts with @id or strings
        - string

        returns a list of IDs with prefixes stripped.
        """
        result = []

        if isinstance(value, dict) and "@id" in value:
            result.append(self._strip_prefix(value["@id"]))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and "@id" in item:
                    result.append(self._strip_prefix(item["@id"]))
                elif isinstance(item, str):
                    result.append(self._strip_prefix(item))
        elif isinstance(value, str):
            result.append(self._strip_prefix(value))

        return result

    def is_required(self, node_id: str) -> bool:
        """
        check if a node is marked as required. returns True if sms:required is "sms:true", false otherwise.
        """
        node = self.get_node(node_id)
        if not node:
            return False

        required = node.get("sms:required")
        return required == "sms:true"

    def get_required_components(self, node_id: str) -> List[str]:
        """
        get the list of required components for a node. returns a list of component IDs with prefixes stripped.
        """
        node = self.get_node(node_id)
        if not node or "sms:requiresComponent" not in node:
            return []

        return self._extract_reference_ids(node.get("sms:requiresComponent"))

    def get_required_dependencies(self, node_id: str) -> List[str]:
        """
        get the list of required dependencies for a node.
        returns a list of dependency IDs with prefixes stripped.
        """
        node = self.get_node(node_id)
        if not node or "sms:requiresDependency" not in node:
            return []

        return self._extract_reference_ids(node.get("sms:requiresDependency"))

    def get_node_attributes(self, node_id: str) -> Dict[str, Any]:
        """
        get all relevant attributes for a node, including required status,
        required components, and required dependencies.

        returns a structured dictionary with these attributes.
        """
        result = {}
        node = self.get_node(node_id)
        if not node:
            return result

        result["id"] = self._strip_prefix(node_id)
        name = node.get("rdfs:label") or node.get("schema:name")
        if name:
            result["name"] = name

        requirements = []
        requirements.append({
            "required": node.get("sms:required") == "sms:true"
        })

        if "sms:requiresComponent" in node:
            component_ids = self._extract_reference_ids(node.get("sms:requiresComponent"))
            if component_ids:
                components = []
                for comp_id in component_ids:
                    comp_with_prefix = f"bts:{comp_id}" if ":" not in comp_id else comp_id
                    comp_name = self.get_name(comp_with_prefix)

                    components.append({
                        "id": comp_id,
                        "name": comp_name if comp_name != comp_with_prefix else comp_id,
                        "required": self.is_required(comp_with_prefix)
                    })

                requirements.append({
                    "components": components
                })

        if "sms:requiresDependency" in node:
            dependency_ids = self._extract_reference_ids(node.get("sms:requiresDependency"))
            if dependency_ids:
                dependencies = []
                for dep_id in dependency_ids:
                    dep_with_prefix = f"bts:{dep_id}" if ":" not in dep_id else dep_id
                    dep_name = self.get_name(dep_with_prefix)

                    dependencies.append({
                        "id": dep_id,
                        "name": dep_name if dep_name != dep_with_prefix else dep_id,
                        "required": self.is_required(dep_with_prefix)
                    })

                requirements.append({
                    "dependencies": dependencies
                })

        if requirements:
            result["requirements"] = requirements

        return result

    def get_all_sms_attributes(self, node_id: str) -> Dict[str, Any]:
        """
        get all SMS (Schema Metadata Schema) attributes for a node.
        this includes all keys that start with 'sms:'.

        returns a dictionary of attribute name to value, with 'sms:' prefix removed from keys.
        """
        node = self.get_node(node_id)
        if not node:
            return {}

        sms_attrs = {
            k[4:]: v for k, v in node.items()
            if k.startswith("sms:") and k != "@id"
        }

        for key, value in sms_attrs.items():
            if key in ["requiresComponent", "requiresDependency"]:
                sms_attrs[key] = self._extract_reference_ids(value)

        return sms_attrs

    def find_nodes_with_component(self, component_id: str) -> List[str]:
        """
        find all nodes that require a specific component.
        returns a list of node IDs that have the given component_id in their requiresComponent list.
        """
        result = []

        for node in self.graph:
            node_id = node.get("@id")
            if not node_id:
                continue

            if "sms:requiresComponent" in node:
                components = self._extract_reference_ids(node.get("sms:requiresComponent"))
                if self._strip_prefix(component_id) in components:
                    result.append(node_id)

        return result

    def find_nodes_with_dependency(self, dependency_id: str) -> List[str]:
        """
        find all nodes that require a specific dependency.
        returns a list of node IDs that have the given dependency_id in their requiresDependency list.
        """
        result = []

        for node in self.graph:
            node_id = node.get("@id")
            if not node_id:
                continue

            if "sms:requiresDependency" in node:
                dependencies = self._extract_reference_ids(node.get("sms:requiresDependency"))
                if self._strip_prefix(dependency_id) in dependencies:
                    result.append(node_id)

        return result

    def get_dependency_graph(self) -> Dict[str, Set[str]]:
        """
        build a dependency graph based on requiresComponent and requiresDependency relationships.
        returns a dictionary mapping node IDs to sets of node IDs they depend on.
        """
        dependency_graph = defaultdict(set)

        for node in self.graph:
            node_id = node.get("@id")
            if not node_id:
                continue

            stripped_id = self._strip_prefix(node_id)

            if "sms:requiresComponent" in node:
                components = self._extract_reference_ids(node.get("sms:requiresComponent"))
                for comp in components:
                    dependency_graph[stripped_id].add(comp)

            if "sms:requiresDependency" in node:
                dependencies = self._extract_reference_ids(node.get("sms:requiresDependency"))
                for dep in dependencies:
                    dependency_graph[stripped_id].add(dep)

        return dict(dependency_graph)
