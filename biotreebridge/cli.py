import click
import json
from biotreebridge.schema_parser.parser import BioThingsSchemaParser
from biotreebridge.bridge.registry import SchemaRegistry


@click.group()
def cli():
    """biotreebridge - HTAN schema parser and visualization tool"""
    pass


@cli.group('schema')
def schema_commands():
    """schema parsing commands"""
    pass


@cli.group('fhir')
def fhir_commands():
    """FHIR mapping commands"""
    pass

@fhir_commands.command('create-mapping-template')
@click.option('--source', '-s', default='schema.json',
              help='Schema source. Default is schema.json.')
@click.option('--output', '-o', default='mappings.json',
              help='Output mapping template file. Default is mappings.json.')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose output.')
def create_mapping_template(source, output, verbose):
    """Create a FHIR mapping template from schema"""
    registry = SchemaRegistry(schema_path=source, verbose=verbose)

    try:

        mappings, range_values = registry.create_mapping_template(output)

        click.echo(f"➜ Created mapping template with {len(mappings)} nodes at {output}")
        click.echo(f"➜ Excluded {len(range_values)} schema:rangeIncludes values")
        click.echo("\nNext steps:")
        click.echo("  1. Edit the mapping template to add FHIR mappings")
        click.echo(
            f"  2. Apply the mappings with: biotreebridge fhir apply-mapping --source {source} --mapping {output}")
    except Exception as e:
        click.echo(f"Error creating mapping template: {str(e)}", err=True)
        raise click.Abort()



@fhir_commands.command('apply-mapping')
@click.option('--source', '-s', default='schema.json',
              help='Schema source. Default is schema.json.')
@click.option('--mapping', '-m', default='mappings.json',
              help='Mapping template file. Default is mappings.json.')
@click.option('--output', '-o', default='schema_fhir.json',
              help='Output schema file with FHIR mappings. Default is schema_fhir.json.')
@click.option('--verbose', '-v', is_flag=True,
              help='Enable verbose output.')
def apply_mapping(source, mapping, output, verbose):
    """Apply FHIR mappings to schema"""
    registry = SchemaRegistry(schema_path=source, verbose=verbose)

    try:
        registry.apply_mapping_template(mapping)
        registry.save_schema(output)

        click.echo(f"➜ Applied FHIR mappings from {mapping}")
        click.echo(f"➜ Updated schema saved to {output}")
    except Exception as e:
        click.echo(f"Error applying mappings: {str(e)}", err=True)
        raise click.Abort()

@schema_commands.command('tree')
@click.option('--source', '-s', default='schema.json',
              help='Schema source (URL or file). Default is schema.json.')
@click.option('--output', '-o', default="schemas/hierarchy.json",
              help='Output JSON file')
@click.option('--parent', '-p', help='Parent node to start from')
@click.option('--max-depth', '-d', type=int, default=-1,
              help='Max depth to traverse (use -1 for no limit)')
@click.option('--include-attributes', '-a', is_flag=True,
              help='Include required and dependency attributes in the output')
def generate_tree(source, output, parent, max_depth, include_attributes):
    """generate tree hierarchy"""
    if max_depth != -1 and max_depth < 0:  # ensure max_depth is either -1 or a non-negative integer
        raise click.BadParameter("max_depth must be either -1 (no limit) or a positive integer.")

    parser = BioThingsSchemaParser(source)
    tree = parser.get_children_hierarchy(parent, max_depth, include_attributes)

    with open(output, "w", encoding="utf-8") as fp:
        json.dump(tree, fp, indent=2, ensure_ascii=False)
    print(f"➜ {output} written")


@schema_commands.command('roots')
@click.option('--source', '-s', default='schema.json',
              help='Schema source (URL or file). Default is schema.json generated via curl -s https://raw.githubusercontent.com/ncihtan/data-models/main/HTAN.model.jsonld | jq . > schema.json.')
def list_roots(source):
    parser = BioThingsSchemaParser(source)
    roots = parser.get_roots()

    print(f"found {len(roots)} root nodes:")
    for root in sorted(roots):
        name = parser.get_name(root)
        if name != root:
            print(f"  {name} ({root})")
        else:
            print(f"  {root}")


@schema_commands.command('search')
@click.option('--source', '-s', default='schema.json',
              help='Schema source (URL or file). Default is schema.json.')
@click.option('--term', '-t', required=True,
              help='Search term')
@click.option('--output', '-o',
              help='Save results to JSON file')
def search_nodes(source, term, output):
    """search nodes by name"""
    parser = BioThingsSchemaParser(source)
    results = parser.search(term)

    formatted_results = []
    for node_id in results:
        name = parser.get_name(node_id)
        formatted_results.append({"id": node_id, "name": name})

    if results:
        print(f"found {len(results)} nodes matching '{term}':")
        for res in formatted_results:
            if res["name"] != res["id"]:
                print(f"  {res['name']} ({res['id']})")
            else:
                print(f"  {res['id']}")
    else:
        print(f"no nodes found matching '{term}'")

    if output:
        with open(output, "w", encoding="utf-8") as fp:
            json.dump(formatted_results, fp, indent=2, ensure_ascii=False)
        print(f"➜ {output} written")


if __name__ == "__main__":
    cli()

