import json
from collections import defaultdict
from typing import Dict, List, Tuple
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

    def get_children_hierarchy(self, parent_id: str, max_depth: int = -1) -> Dict:
        """
        build and return a nested dict representing the tree under `parent_id`:
          {
            "id": parent_id,
            "children": [
              { "id": child1, "children": [...] },
              ...
            ]
          }
        """
        p2c, _ = self.extract_subclass_relationships()

        def build_tree(node_id: str, depth=0) -> Dict:
            if max_depth != -1 and depth >= max_depth:
                return {"id": node_id}

            children = p2c.get(node_id, [])
            return {
                "id": node_id,
                "children": [
                    build_tree(child_id, depth + 1) for child_id in children
                ]
            }

        return build_tree(parent_id)

    def get_roots(self) -> List[str]:
        """
        get all root nodes (those without parents).
        """
        _, c2p = self.extract_subclass_relationships()
        all_nodes = set([node.get('@id') for node in self.graph])
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
