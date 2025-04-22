import json
import pytest
from click.testing import CliRunner
from biotreebridge.cli import cli


@pytest.fixture
def runner():
    """fixture for setting up the CLI runner"""
    return CliRunner()


@pytest.fixture
def sample_schema():
    """fixture for providing a sample schema (as JSON) with RPPALevel2 node"""
    return {
        "@graph": [
            {"@id": "RPPALevel2", "rdfs:label": "RPPALevel2", "rdf:subClassOf": "Base"},
            {"@id": "Base", "rdfs:label": "Base", "rdf:subClassOf": []},
            {"@id": "NormalizationMethod", "rdfs:label": "NormalizationMethod", "rdf:subClassOf": "RPPALevel2"},
            {"@id": "HTANRPPAAntibodyTable", "rdfs:label": "HTANRPPAAntibodyTable", "rdf:subClassOf": "RPPALevel2"},
        ]
    }


@pytest.fixture
def mock_schema_file(tmp_path, sample_schema):
    """fixture to create a temporary file for the sample schema"""
    schema_file = tmp_path / "schema.json"
    with open(schema_file, "w", encoding="utf-8") as f:
        json.dump(sample_schema, f, indent=2)
    return schema_file


def test_generate_tree_with_max_depth(runner, mock_schema_file):
    """test the 'tree' command with max depth limit"""
    result = runner.invoke(
        cli,
        ["schema", "tree", "--source", str(mock_schema_file), "--parent", "RPPALevel2", "--max-depth", "1", "--output",
         "output.json"],
    )

    assert result.exit_code == 0
    assert "➜ output.json written" in result.output
    with open("output.json", "r", encoding="utf-8") as f:
        tree = json.load(f)

    assert "RPPALevel2" in tree["id"]
    assert "children" in tree
    assert len(tree["children"]) == 2  # expect 2 children: NormalizationMethod and HTANRPPAAntibodyTable


def test_generate_tree_with_max_depth_2(runner, mock_schema_file):
    """test the 'tree' command with max depth set to 2"""
    result = runner.invoke(
        cli,
        ["schema", "tree", "--source", str(mock_schema_file), "--parent", "RPPALevel2", "--max-depth", "2", "--output",
         "output.json"],
    )

    assert result.exit_code == 0
    assert "➜ output.json written" in result.output
    with open("output.json", "r", encoding="utf-8") as f:
        tree = json.load(f)

    assert "RPPALevel2" in tree["id"]
    assert "children" in tree
    assert len(tree["children"]) == 2  # expect 2 children: NormalizationMethod and HTANRPPAAntibodyTable

    for child in tree["children"]:
        assert "children" in child


# def test_list_roots(runner, mock_schema_file):
#     """test the 'roots' command"""
#     result = runner.invoke(
#         cli,
#         ["schema", "roots", "--source", str(mock_schema_file)],
#     )
#
#     assert result.exit_code == 0
#     assert "Found 1 root nodes:" in result.output
#     assert "Base" in result.output


def test_search_nodes(runner, mock_schema_file):
    """test the 'search' command"""
    result = runner.invoke(
        cli,
        ["schema", "search", "--source", str(mock_schema_file), "--term", "RPPALevel2", "--output",
         "search_results.json"],
    )

    assert result.exit_code == 0
    assert "➜ search_results.json written" in result.output
    with open("search_results.json", "r", encoding="utf-8") as f:
        search_results = json.load(f)
    assert len(search_results) > 0
    assert search_results[0]["name"] == "RPPALevel2"
    assert search_results[0]["id"] == "RPPALevel2"


def test_generate_tree_with_invalid_max_depth(runner, mock_schema_file):
    """test the 'tree' command with invalid max depth value"""
    result = runner.invoke(
        cli,
        ["schema", "tree", "--source", str(mock_schema_file), "--parent", "RPPALevel2", "--max-depth", "-10",
         "--output", "output.json"],
    )

    assert result.exit_code != 0
    assert "max_depth must be either -1 (no limit) or a positive integer." in result.output
