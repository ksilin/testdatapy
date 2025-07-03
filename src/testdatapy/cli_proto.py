"""CLI commands for protobuf schema management."""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import click

from testdatapy.schema.compiler import ProtobufCompiler
from testdatapy.schema.exceptions import CompilationError, SchemaError, ValidationError
from testdatapy.schema.manager import SchemaManager
from testdatapy.schema.validator import ProtobufConfigValidator, generate_validation_report
from testdatapy.schema_evolution import SchemaEvolutionManager
from testdatapy.config.correlation_config import CorrelationConfig


@click.group()
def proto():
    """Protobuf schema management commands."""
    pass


@proto.command()
@click.option(
    "--proto-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    required=True,
    help="Directory containing proto files"
)
@click.option(
    "--output-dir", 
    type=click.Path(file_okay=False, dir_okay=True),
    help="Output directory for compiled files (default: temp directory)"
)
@click.option(
    "--include-path",
    "-I",
    multiple=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Additional include paths for proto compilation"
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output"
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate syntax only, don't compile"
)
@click.option(
    "--timeout",
    type=int,
    default=60,
    help="Compilation timeout in seconds"
)
def compile(proto_dir: str, output_dir: str, include_path: List[str], verbose: bool, dry_run: bool, timeout: int):
    """Compile proto files to Python modules."""
    try:
        # Validate proto directory
        proto_path = Path(proto_dir)
        if not proto_path.exists():
            click.echo(f"Error: Proto directory does not exist: {proto_dir}", err=True)
            sys.exit(1)
        
        # Initialize schema manager and compiler
        manager = SchemaManager()
        compiler = ProtobufCompiler(config={'timeout': timeout})
        
        # Discover proto files
        if verbose:
            click.echo(f"Discovering proto files in {proto_dir}...")
        
        proto_files = manager.discover_proto_files(proto_dir, recursive=True)
        
        if not proto_files:
            click.echo("No proto files found in the specified directory.")
            return
        
        click.echo(f"Found {len(proto_files)} proto file(s):")
        for proto_file in proto_files:
            click.echo(f"  - {Path(proto_file).relative_to(proto_path)}")
        
        if dry_run:
            click.echo("\nDRY RUN: Validating syntax only...")
            failed_files = []
            
            for proto_file in proto_files:
                try:
                    if verbose:
                        click.echo(f"Validating {Path(proto_file).name}...")
                    manager.validate_proto_syntax(proto_file)
                    click.echo(f"✓ {Path(proto_file).name}")
                except ValidationError as e:
                    click.echo(f"✗ {Path(proto_file).name}: {e}", err=True)
                    failed_files.append(proto_file)
            
            if failed_files:
                click.echo(f"\nValidation failed for {len(failed_files)} file(s).", err=True)
                sys.exit(1)
            else:
                click.echo(f"\nAll {len(proto_files)} files passed validation.")
            return
        
        # Compile proto files
        click.echo(f"\nCompiling proto files...")
        
        if not output_dir:
            import tempfile
            output_dir = tempfile.mkdtemp(prefix='testdatapy_proto_')
            click.echo(f"Using temporary output directory: {output_dir}")
        else:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        failed_compilations = []
        include_paths = list(include_path) + [proto_dir]
        
        for proto_file in proto_files:
            try:
                if verbose:
                    click.echo(f"Compiling {Path(proto_file).name}...")
                
                result = compiler.compile_proto(
                    proto_file, 
                    output_dir, 
                    include_paths=include_paths,
                    timeout=timeout
                )
                
                if result['success']:
                    click.echo(f"✓ {Path(proto_file).name}")
                    if verbose and result.get('stdout'):
                        click.echo(f"  Output: {result['stdout']}")
                else:
                    click.echo(f"✗ {Path(proto_file).name}: Compilation failed", err=True)
                    failed_compilations.append(proto_file)
                    
            except CompilationError as e:
                click.echo(f"✗ {Path(proto_file).name}: {e}", err=True)
                failed_compilations.append(proto_file)
                if verbose and hasattr(e, 'details') and e.details.get('stderr'):
                    click.echo(f"  Details: {e.details['stderr']}", err=True)
        
        # Summary
        if failed_compilations:
            click.echo(f"\nCompilation failed for {len(failed_compilations)} file(s):", err=True)
            for failed_file in failed_compilations:
                click.echo(f"  - {Path(failed_file).name}", err=True)
            sys.exit(1)
        else:
            click.echo(f"\nCompilation completed successfully!")
            click.echo(f"Output directory: {output_dir}")
            
    except Exception as e:
        click.echo(f"Error compiling proto files: {e}", err=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@proto.command()
@click.option(
    "--proto-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    required=True,
    help="Directory to search for proto files"
)
@click.option(
    "--format",
    type=click.Choice(["table", "json", "list"]),
    default="table",
    help="Output format"
)
@click.option(
    "--recursive/--no-recursive",
    default=True,
    help="Search recursively in subdirectories"
)
@click.option(
    "--register",
    is_flag=True,
    help="Register discovered schemas with Schema Registry"
)
@click.option(
    "--schema-registry-url",
    help="Schema Registry URL (required if --register is used)"
)
def discover(proto_dir: str, format: str, recursive: bool, register: bool, schema_registry_url: str):
    """Discover proto files in a directory."""
    try:
        if register and not schema_registry_url:
            click.echo("Error: --schema-registry-url is required when using --register", err=True)
            sys.exit(1)
        
        # Initialize schema manager
        manager = SchemaManager()
        
        # Discover proto files
        proto_files = manager.discover_proto_files(proto_dir, recursive=recursive)
        
        if not proto_files:
            click.echo("No proto files found.")
            return
        
        # Analyze proto files
        proto_info = []
        for proto_file in proto_files:
            try:
                file_path = Path(proto_file)
                relative_path = file_path.relative_to(Path(proto_dir))
                
                # Basic file info
                info = {
                    "file": str(relative_path),
                    "absolute_path": str(file_path),
                    "size": file_path.stat().st_size,
                    "modified": file_path.stat().st_mtime
                }
                
                # Try to extract package and messages (basic parsing)
                try:
                    with open(proto_file, 'r') as f:
                        content = f.read()
                    
                    # Extract package name
                    package_lines = [line.strip() for line in content.split('\n') if line.strip().startswith('package ')]
                    if package_lines:
                        package = package_lines[0].replace('package ', '').replace(';', '').strip()
                        info["package"] = package
                    
                    # Count messages and services
                    message_count = content.count('message ')
                    service_count = content.count('service ')
                    info["messages"] = message_count
                    info["services"] = service_count
                    
                except Exception:
                    # If parsing fails, just continue with basic info
                    pass
                
                proto_info.append(info)
                
            except Exception as e:
                click.echo(f"Warning: Could not analyze {proto_file}: {e}", err=True)
        
        # Output results
        if format == "json":
            result = {
                "proto_files": proto_info,
                "total_files": len(proto_info),
                "search_directory": proto_dir,
                "recursive": recursive
            }
            click.echo(json.dumps(result, indent=2))
            
        elif format == "list":
            for info in proto_info:
                click.echo(info["file"])
                
        else:  # table format
            click.echo(f"Discovered {len(proto_info)} proto files in {proto_dir}:")
            click.echo()
            
            # Table header
            click.echo(f"{'File':<40} {'Package':<20} {'Msgs':<5} {'Svcs':<5} {'Size':<8}")
            click.echo("-" * 78)
            
            for info in proto_info:
                file_name = info["file"]
                if len(file_name) > 39:
                    file_name = "..." + file_name[-36:]
                
                package = info.get("package", "")
                if len(package) > 19:
                    package = package[:16] + "..."
                
                messages = info.get("messages", "?")
                services = info.get("services", "?")
                size = f"{info['size']:,}B"
                
                click.echo(f"{file_name:<40} {package:<20} {messages:<5} {services:<5} {size:<8}")
        
        # Register with Schema Registry if requested
        if register:
            click.echo(f"\nRegistering schemas with Schema Registry at {schema_registry_url}...")
            evolution_manager = SchemaEvolutionManager(schema_registry_url)
            
            registered_count = 0
            for info in proto_info:
                try:
                    # Generate subject name from file path
                    file_path = Path(info["file"])
                    subject = f"{file_path.stem}-value"
                    
                    # Read proto content
                    with open(info["absolute_path"], 'r') as f:
                        proto_content = f.read()
                    
                    # Register schema
                    schema_id = evolution_manager.register_schema(subject, proto_content)
                    click.echo(f"✓ Registered {info['file']} as {subject} (ID: {schema_id})")
                    registered_count += 1
                    
                except Exception as e:
                    click.echo(f"✗ Failed to register {info['file']}: {e}", err=True)
            
            click.echo(f"\nRegistered {registered_count}/{len(proto_info)} schemas successfully.")
        
    except Exception as e:
        click.echo(f"Error discovering proto files: {e}", err=True)
        sys.exit(1)


@proto.command()
@click.option(
    "--schema-registry-url",
    required=True,
    help="Schema Registry URL"
)
@click.option(
    "--format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.option(
    "--subject-filter",
    help="Filter subjects by pattern (supports wildcards)"
)
def list(schema_registry_url: str, format: str, subject_filter: str):
    """List protobuf schemas in Schema Registry."""
    try:
        evolution_manager = SchemaEvolutionManager(schema_registry_url)
        
        # Get all subjects
        subjects = evolution_manager.list_subjects()
        
        if not subjects:
            click.echo("No schemas found in Schema Registry.")
            return
        
        # Filter subjects if pattern provided
        if subject_filter:
            import fnmatch
            subjects = [s for s in subjects if fnmatch.fnmatch(s, subject_filter)]
        
        if not subjects:
            click.echo(f"No schemas found matching pattern: {subject_filter}")
            return
        
        # Get schema information
        schema_info = []
        for subject in subjects:
            try:
                latest_schema = evolution_manager.get_latest_schema(subject)
                if latest_schema:
                    info = {
                        "subject": subject,
                        "id": latest_schema.get("id"),
                        "version": latest_schema.get("version"),
                        "schema_type": latest_schema.get("schemaType", "AVRO")
                    }
                    
                    # Try to parse schema content for additional info
                    schema_str = latest_schema.get("schema", "")
                    if schema_str:
                        try:
                            if info["schema_type"] == "PROTOBUF":
                                # Count messages in protobuf schema
                                message_count = schema_str.count("message ")
                                info["messages"] = message_count
                            else:
                                # For other schema types, try to parse as JSON
                                schema_obj = json.loads(schema_str)
                                if isinstance(schema_obj, dict):
                                    info["name"] = schema_obj.get("name")
                                    if "fields" in schema_obj:
                                        info["fields"] = len(schema_obj["fields"])
                        except:
                            pass
                    
                    schema_info.append(info)
                    
            except Exception as e:
                click.echo(f"Warning: Could not get info for subject {subject}: {e}", err=True)
        
        # Output results
        if format == "json":
            result = {
                "subjects": schema_info,
                "total_subjects": len(schema_info),
                "schema_registry_url": schema_registry_url
            }
            click.echo(json.dumps(result, indent=2))
        else:
            # Table format
            click.echo(f"Found {len(schema_info)} schema(s) in Schema Registry:")
            click.echo()
            
            # Table header
            click.echo(f"{'Subject':<30} {'Type':<10} {'Version':<8} {'ID':<8} {'Messages/Fields':<15}")
            click.echo("-" * 71)
            
            for info in schema_info:
                subject = info["subject"]
                if len(subject) > 29:
                    subject = subject[:26] + "..."
                
                schema_type = info["schema_type"]
                version = info.get("version", "?")
                schema_id = info.get("id", "?")
                
                # Show message count for protobuf, field count for others
                extra_info = ""
                if info.get("messages"):
                    extra_info = f"{info['messages']} msgs"
                elif info.get("fields"):
                    extra_info = f"{info['fields']} fields"
                elif info.get("name"):
                    extra_info = info["name"]
                
                click.echo(f"{subject:<30} {schema_type:<10} {version:<8} {schema_id:<8} {extra_info:<15}")
        
    except Exception as e:
        click.echo(f"Error listing schemas: {e}", err=True)
        sys.exit(1)


@proto.command()
@click.option(
    "--proto-file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="Proto file to test"
)
@click.option(
    "--include-path",
    "-I",
    multiple=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Additional include paths"
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output"
)
def test(proto_file: str, include_path: List[str], verbose: bool):
    """Test proto file syntax and compilation."""
    try:
        # Validate file exists
        file_path = Path(proto_file)
        if not file_path.exists():
            click.echo(f"Error: File does not exist: {proto_file}", err=True)
            sys.exit(1)
        
        click.echo(f"Testing proto file: {file_path.name}")
        click.echo("-" * 50)
        
        # Initialize manager and compiler
        manager = SchemaManager()
        compiler = ProtobufCompiler()
        
        # Test 1: Syntax validation
        click.echo("1. Syntax validation...", nl=False)
        try:
            manager.validate_proto_syntax(proto_file)
            click.echo(" PASSED", fg="green")
            if verbose:
                click.echo("   ✓ Proto syntax is valid")
        except ValidationError as e:
            click.echo(" FAILED", fg="red")
            click.echo(f"   Error: {e}", err=True)
            sys.exit(1)
        
        # Test 2: Compilation test
        click.echo("2. Compilation test...", nl=False)
        try:
            import tempfile
            with tempfile.TemporaryDirectory() as temp_dir:
                include_paths = list(include_path) + [str(file_path.parent)]
                result = compiler.compile_proto(
                    proto_file, 
                    temp_dir, 
                    include_paths=include_paths
                )
                
                if result['success']:
                    click.echo(" PASSED", fg="green")
                    if verbose:
                        click.echo("   ✓ Proto compiles successfully")
                        # List generated files
                        generated_files = list(Path(temp_dir).glob("*.py"))
                        if generated_files:
                            click.echo(f"   ✓ Generated {len(generated_files)} Python file(s)")
                            for gen_file in generated_files:
                                click.echo(f"     - {gen_file.name}")
                else:
                    click.echo(" FAILED", fg="red")
                    click.echo("   Error: Compilation failed", err=True)
                    sys.exit(1)
                    
        except CompilationError as e:
            click.echo(" FAILED", fg="red")
            click.echo(f"   Error: {e}", err=True)
            if verbose and hasattr(e, 'details'):
                if e.details.get('stderr'):
                    click.echo(f"   Details: {e.details['stderr']}", err=True)
            sys.exit(1)
        
        # Test 3: Dependency analysis (if available)
        if include_path:
            click.echo("3. Dependency analysis...", nl=False)
            try:
                dependencies = compiler.resolve_dependencies(proto_file, list(include_path))
                click.echo(" PASSED", fg="green")
                if verbose:
                    if dependencies:
                        click.echo(f"   ✓ Found {len(dependencies)} dependencies:")
                        for dep in dependencies:
                            click.echo(f"     - {Path(dep).name}")
                    else:
                        click.echo("   ✓ No dependencies found")
            except Exception as e:
                click.echo(" WARNING", fg="yellow")
                if verbose:
                    click.echo(f"   Warning: Could not analyze dependencies: {e}")
        
        click.echo()
        click.echo("All tests passed! ✓", fg="green")
        
    except Exception as e:
        click.echo(f"Error testing proto file: {e}", err=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@proto.command()
@click.option(
    "--proto-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    required=True,
    help="Directory containing proto files"
)
@click.option(
    "--output",
    type=click.Path(file_okay=False, dir_okay=True),
    required=True,
    help="Output directory for documentation"
)
@click.option(
    "--format",
    type=click.Choice(["markdown", "html"]),
    default="markdown",
    help="Documentation format"
)
@click.option(
    "--include-services/--no-include-services",
    default=True,
    help="Include service definitions in documentation"
)
@click.option(
    "--title",
    default="Proto Schema Documentation",
    help="Documentation title"
)
def docs(proto_dir: str, output: str, format: str, include_services: bool, title: str):
    """Generate documentation for proto files."""
    try:
        # Create output directory
        output_path = Path(output)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize schema manager
        manager = SchemaManager()
        
        # Discover proto files
        click.echo(f"Generating documentation for proto files in {proto_dir}...")
        proto_files = manager.discover_proto_files(proto_dir, recursive=True)
        
        if not proto_files:
            click.echo("No proto files found.")
            return
        
        # Parse proto files for documentation
        proto_docs = []
        for proto_file in proto_files:
            try:
                file_path = Path(proto_file)
                relative_path = file_path.relative_to(Path(proto_dir))
                
                with open(proto_file, 'r') as f:
                    content = f.read()
                
                # Extract documentation info (basic parsing)
                doc_info = {
                    "file": str(relative_path),
                    "package": "",
                    "messages": [],
                    "services": [],
                    "imports": [],
                    "comments": []
                }
                
                lines = content.split('\n')
                current_message = None
                current_service = None
                
                for line in lines:
                    line = line.strip()
                    
                    # Package declaration
                    if line.startswith('package '):
                        doc_info["package"] = line.replace('package ', '').replace(';', '').strip()
                    
                    # Import statements
                    elif line.startswith('import '):
                        import_path = line.replace('import ', '').replace('"', '').replace(';', '').strip()
                        doc_info["imports"].append(import_path)
                    
                    # Comments
                    elif line.startswith('//'):
                        comment = line.replace('//', '').strip()
                        if comment:
                            doc_info["comments"].append(comment)
                    
                    # Message definitions
                    elif line.startswith('message '):
                        message_name = line.replace('message ', '').replace('{', '').strip()
                        current_message = {
                            "name": message_name,
                            "fields": [],
                            "comments": []
                        }
                        doc_info["messages"].append(current_message)
                    
                    # Service definitions
                    elif line.startswith('service ') and include_services:
                        service_name = line.replace('service ', '').replace('{', '').strip()
                        current_service = {
                            "name": service_name,
                            "methods": [],
                            "comments": []
                        }
                        doc_info["services"].append(current_service)
                    
                    # Field definitions (basic parsing)
                    elif current_message and '=' in line and not line.startswith('//'):
                        # Simple field parsing
                        parts = line.split('=')
                        if len(parts) == 2:
                            field_def = parts[0].strip()
                            field_num = parts[1].replace(';', '').strip()
                            current_message["fields"].append({
                                "definition": field_def,
                                "number": field_num
                            })
                
                proto_docs.append(doc_info)
                
            except Exception as e:
                click.echo(f"Warning: Could not parse {proto_file}: {e}", err=True)
        
        # Generate documentation
        if format == "markdown":
            doc_file = output_path / "schema_docs.md"
            
            with open(doc_file, 'w') as f:
                f.write(f"# {title}\n\n")
                f.write(f"Generated from {len(proto_docs)} proto file(s)\n\n")
                f.write("## Table of Contents\n\n")
                
                # Table of contents
                for doc in proto_docs:
                    f.write(f"- [{doc['file']}](#{doc['file'].replace('/', '').replace('.', '')})\n")
                f.write("\n")
                
                # Document each proto file
                for doc in proto_docs:
                    f.write(f"## {doc['file']}\n\n")
                    
                    if doc["package"]:
                        f.write(f"**Package:** `{doc['package']}`\n\n")
                    
                    if doc["imports"]:
                        f.write("**Imports:**\n")
                        for import_path in doc["imports"]:
                            f.write(f"- `{import_path}`\n")
                        f.write("\n")
                    
                    if doc["comments"]:
                        f.write("**Description:**\n")
                        for comment in doc["comments"]:
                            f.write(f"{comment}\n")
                        f.write("\n")
                    
                    # Messages
                    if doc["messages"]:
                        f.write("### Messages\n\n")
                        for message in doc["messages"]:
                            f.write(f"#### {message['name']}\n\n")
                            if message["fields"]:
                                f.write("| Field | Type | Number |\n")
                                f.write("|-------|------|--------|\n")
                                for field in message["fields"]:
                                    parts = field["definition"].split()
                                    if len(parts) >= 2:
                                        field_type = parts[0]
                                        field_name = parts[1]
                                        f.write(f"| {field_name} | {field_type} | {field['number']} |\n")
                                f.write("\n")
                    
                    # Services
                    if doc["services"] and include_services:
                        f.write("### Services\n\n")
                        for service in doc["services"]:
                            f.write(f"#### {service['name']}\n\n")
                            # Add service method details if parsed
                    
                    f.write("---\n\n")
            
            click.echo(f"Markdown documentation generated: {doc_file}")
            
        elif format == "html":
            # Generate HTML documentation
            doc_file = output_path / "index.html"
            
            html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 40px; }}
        h1, h2, h3 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        code {{ background-color: #f4f4f4; padding: 2px 4px; }}
        .proto-file {{ margin-bottom: 30px; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <p>Generated from {len(proto_docs)} proto file(s)</p>
    
    <h2>Table of Contents</h2>
    <ul>
"""
            
            for doc in proto_docs:
                file_id = doc['file'].replace('/', '').replace('.', '')
                html_content += f'        <li><a href="#{file_id}">{doc["file"]}</a></li>\n'
            
            html_content += "    </ul>\n\n"
            
            # Document each proto file
            for doc in proto_docs:
                file_id = doc['file'].replace('/', '').replace('.', '')
                html_content += f'    <div class="proto-file" id="{file_id}">\n'
                html_content += f'        <h2>{doc["file"]}</h2>\n'
                
                if doc["package"]:
                    html_content += f'        <p><strong>Package:</strong> <code>{doc["package"]}</code></p>\n'
                
                if doc["imports"]:
                    html_content += '        <p><strong>Imports:</strong></p>\n        <ul>\n'
                    for import_path in doc["imports"]:
                        html_content += f'            <li><code>{import_path}</code></li>\n'
                    html_content += '        </ul>\n'
                
                # Messages
                if doc["messages"]:
                    html_content += '        <h3>Messages</h3>\n'
                    for message in doc["messages"]:
                        html_content += f'        <h4>{message["name"]}</h4>\n'
                        if message["fields"]:
                            html_content += '''        <table>
            <tr><th>Field</th><th>Type</th><th>Number</th></tr>
'''
                            for field in message["fields"]:
                                parts = field["definition"].split()
                                if len(parts) >= 2:
                                    field_type = parts[0]
                                    field_name = parts[1]
                                    html_content += f'            <tr><td>{field_name}</td><td><code>{field_type}</code></td><td>{field["number"]}</td></tr>\n'
                            html_content += '        </table>\n'
                
                html_content += '    </div>\n'
            
            html_content += """
</body>
</html>
"""
            
            with open(doc_file, 'w') as f:
                f.write(html_content)
            
            click.echo(f"HTML documentation generated: {doc_file}")
        
        click.echo(f"\nDocumentation generated successfully in {output_path}")
        
    except Exception as e:
        click.echo(f"Error generating documentation: {e}", err=True)
        sys.exit(1)


@proto.command()
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="Path to protobuf configuration file"
)
@click.option(
    "--format",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format for validation report"
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output"
)
@click.option(
    "--output",
    type=click.Path(dir_okay=False),
    help="Output file for validation report (default: stdout)"
)
def validate(config: str, format: str, verbose: bool, output: str):
    """Validate protobuf configuration and schemas."""
    try:
        click.echo(f"Validating protobuf configuration: {config}")
        click.echo("=" * 60)
        
        # Create validator and run comprehensive validation
        validator = ProtobufConfigValidator()
        results = validator.validate_all(config)
        
        # Generate report
        report = generate_validation_report(results, format)
        
        # Output report
        if output:
            with open(output, 'w') as f:
                f.write(report)
            click.echo(f"Validation report written to: {output}")
        else:
            click.echo(report)
        
        # Exit with appropriate code
        if not results["valid"]:
            sys.exit(1)
        else:
            click.echo("\n✅ All validations passed!")
            
    except Exception as e:
        click.echo(f"Error during validation: {e}", err=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@proto.command()
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to protobuf configuration file (optional)"
)
@click.option(
    "--proto-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Directory containing proto files (required if no config)"
)
@click.option(
    "--format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format"
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output with compilation details"
)
def check_compilation(config: str, proto_dir: str, format: str, verbose: bool):
    """Test proto schema compilation without side effects."""
    try:
        if not config and not proto_dir:
            click.echo("Error: Either --config or --proto-dir must be provided", err=True)
            sys.exit(1)
        
        if config:
            # Use configuration-based validation
            click.echo(f"Testing schema compilation from config: {config}")
            validator = ProtobufConfigValidator()
            results = validator.test_schema_compilation()
        else:
            # Use directory-based testing
            click.echo(f"Testing schema compilation from directory: {proto_dir}")
            manager = SchemaManager()
            compiler = ProtobufCompiler()
            
            # Discover proto files
            proto_files = manager.discover_proto_files(proto_dir, recursive=True)
            
            if not proto_files:
                click.echo("No proto files found in the specified directory.")
                return
            
            # Test compilation for each file
            results = {
                "valid": True,
                "errors": [],
                "warnings": [],
                "compiled_schemas": [],
                "failed_schemas": [],
                "test_details": {}
            }
            
            import tempfile
            with tempfile.TemporaryDirectory() as temp_dir:
                for proto_file in proto_files:
                    try:
                        result = compiler.compile_proto(
                            proto_file,
                            temp_dir,
                            proto_paths=[proto_dir],
                            validate_only=True
                        )
                        
                        if result:
                            results["compiled_schemas"].append({
                                "proto_file": proto_file,
                                "output_files": result.get("generated_files", [])
                            })
                        else:
                            results["failed_schemas"].append({
                                "proto_file": proto_file,
                                "error": "Compilation returned no result"
                            })
                            results["valid"] = False
                            results["errors"].append(f"Failed to compile {proto_file}")
                            
                    except Exception as e:
                        results["failed_schemas"].append({
                            "proto_file": proto_file,
                            "error": str(e)
                        })
                        results["valid"] = False
                        results["errors"].append(f"Error compiling {proto_file}: {str(e)}")
            
            # Generate test summary
            total_files = len(proto_files)
            successful_compilations = len(results["compiled_schemas"])
            failed_compilations = len(results["failed_schemas"])
            
            results["test_details"] = {
                "total_files": total_files,
                "successful_compilations": successful_compilations,
                "failed_compilations": failed_compilations,
                "success_rate": successful_compilations / total_files if total_files > 0 else 0
            }
        
        # Output results
        if format == "json":
            click.echo(json.dumps(results, indent=2))
        else:
            # Table format
            test_details = results.get("test_details", {})
            total = test_details.get("total_files", 0)
            successful = test_details.get("successful_compilations", 0)
            failed = test_details.get("failed_compilations", 0)
            success_rate = test_details.get("success_rate", 0.0)
            
            click.echo(f"\nCompilation Test Results:")
            click.echo("-" * 40)
            click.echo(f"Total Files:     {total}")
            click.echo(f"Successful:      {successful}")
            click.echo(f"Failed:          {failed}")
            click.echo(f"Success Rate:    {success_rate:.1%}")
            
            if results["failed_schemas"] and verbose:
                click.echo(f"\nFailed Compilations:")
                for failed in results["failed_schemas"]:
                    file_name = Path(failed["proto_file"]).name
                    click.echo(f"  ❌ {file_name}: {failed['error']}")
            
            if results["compiled_schemas"] and verbose:
                click.echo(f"\nSuccessful Compilations:")
                for success in results["compiled_schemas"]:
                    file_name = Path(success["proto_file"]).name
                    click.echo(f"  ✅ {file_name}")
        
        # Exit with appropriate code
        if not results["valid"]:
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"Error during compilation testing: {e}", err=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)


@proto.command()
@click.option(
    "--config",
    type=click.Path(exists=True, dir_okay=False),
    help="Path to protobuf configuration file (optional)"
)
@click.option(
    "--proto-file",
    type=click.Path(exists=True, dir_okay=False),
    help="Single proto file to check dependencies for"
)
@click.option(
    "--proto-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Directory containing proto files"
)
@click.option(
    "--include-path",
    "-I",
    multiple=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    help="Additional include paths for dependency resolution"
)
@click.option(
    "--format",
    type=click.Choice(["table", "json", "graph"]),
    default="table",
    help="Output format"
)
@click.option(
    "--check-circular",
    is_flag=True,
    help="Check for circular dependencies"
)
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose output"
)
def check_dependencies(config: str, proto_file: str, proto_dir: str, include_path: List[str], 
                      format: str, check_circular: bool, verbose: bool):
    """Check proto file dependencies and imports."""
    try:
        if not any([config, proto_file, proto_dir]):
            click.echo("Error: One of --config, --proto-file, or --proto-dir must be provided", err=True)
            sys.exit(1)
        
        compiler = ProtobufCompiler()
        dependency_graph = {}
        all_dependencies = set()
        
        if config:
            # Use configuration-based validation
            click.echo(f"Checking dependencies from config: {config}")
            validator = ProtobufConfigValidator()
            correlation_config = CorrelationConfig.from_yaml_file(config)
            validator.config = correlation_config
            results = validator.validate_dependencies()
            dependency_graph = results.get("dependency_graph", {})
            
        elif proto_file:
            # Single file dependency check
            click.echo(f"Checking dependencies for: {Path(proto_file).name}")
            try:
                include_paths = list(include_path) + [str(Path(proto_file).parent)]
                dependencies = compiler.resolve_dependencies(proto_file, include_paths)
                dependency_graph[proto_file] = dependencies
                all_dependencies.update(dependencies)
            except Exception as e:
                click.echo(f"Error resolving dependencies: {e}", err=True)
                sys.exit(1)
                
        elif proto_dir:
            # Directory-based dependency check
            click.echo(f"Checking dependencies for proto files in: {proto_dir}")
            manager = SchemaManager()
            proto_files = manager.discover_proto_files(proto_dir, recursive=True)
            
            if not proto_files:
                click.echo("No proto files found in the specified directory.")
                return
            
            include_paths = list(include_path) + [proto_dir]
            
            for proto_file in proto_files:
                try:
                    dependencies = compiler.resolve_dependencies(proto_file, include_paths)
                    dependency_graph[proto_file] = dependencies
                    all_dependencies.update(dependencies)
                except Exception as e:
                    if verbose:
                        click.echo(f"Warning: Could not resolve dependencies for {Path(proto_file).name}: {e}")
        
        # Check for missing dependencies
        missing_deps = []
        for deps in dependency_graph.values():
            for dep in deps:
                if not Path(dep).exists():
                    missing_deps.append(dep)
        
        # Check for circular dependencies if requested
        circular_deps = []
        if check_circular:
            def has_cycle(node: str, path: List[str], visited: set) -> List[str]:
                if node in path:
                    cycle_start = path.index(node)
                    return path[cycle_start:] + [node]
                
                if node in visited:
                    return []
                
                visited.add(node)
                
                for dep in dependency_graph.get(node, []):
                    cycle = has_cycle(dep, path + [node], visited)
                    if cycle:
                        return cycle
                
                return []
            
            global_visited = set()
            for node in dependency_graph:
                if node not in global_visited:
                    cycle = has_cycle(node, [], set())
                    if cycle:
                        circular_deps.append(cycle)
                        global_visited.update(cycle)
        
        # Output results
        if format == "json":
            result = {
                "dependency_graph": dependency_graph,
                "total_files": len(dependency_graph),
                "total_dependencies": len(all_dependencies),
                "missing_dependencies": missing_deps,
                "circular_dependencies": circular_deps if check_circular else None
            }
            click.echo(json.dumps(result, indent=2))
            
        elif format == "graph":
            # Simple text-based dependency graph
            click.echo("\nDependency Graph:")
            click.echo("=" * 50)
            for proto_file, deps in dependency_graph.items():
                file_name = Path(proto_file).name
                click.echo(f"\n{file_name}:")
                if deps:
                    for dep in deps:
                        dep_name = Path(dep).name
                        status = "✓" if Path(dep).exists() else "❌"
                        click.echo(f"  {status} {dep_name}")
                else:
                    click.echo("  (no dependencies)")
                    
        else:
            # Table format
            click.echo(f"\nDependency Analysis:")
            click.echo("-" * 50)
            click.echo(f"Total Proto Files:       {len(dependency_graph)}")
            click.echo(f"Total Dependencies:      {len(all_dependencies)}")
            click.echo(f"Missing Dependencies:    {len(missing_deps)}")
            if check_circular:
                click.echo(f"Circular Dependencies:   {len(circular_deps)}")
            
            if missing_deps:
                click.echo(f"\n❌ Missing Dependencies:")
                for dep in missing_deps:
                    click.echo(f"   {Path(dep).name}")
            
            if check_circular and circular_deps:
                click.echo(f"\n🔄 Circular Dependencies:")
                for cycle in circular_deps:
                    cycle_names = [Path(f).name for f in cycle]
                    click.echo(f"   {' -> '.join(cycle_names)}")
            
            if verbose and dependency_graph:
                click.echo(f"\n📋 Dependency Details:")
                for proto_file, deps in dependency_graph.items():
                    file_name = Path(proto_file).name
                    if deps:
                        click.echo(f"   {file_name}: {len(deps)} dependencies")
                        for dep in deps:
                            dep_name = Path(dep).name
                            status = "✓" if Path(dep).exists() else "❌"
                            click.echo(f"     {status} {dep_name}")
                    else:
                        click.echo(f"   {file_name}: no dependencies")
        
        # Exit with error if there are missing dependencies or circular dependencies
        if missing_deps or (check_circular and circular_deps):
            sys.exit(1)
            
    except Exception as e:
        click.echo(f"Error checking dependencies: {e}", err=True)
        if verbose:
            import traceback
            click.echo(traceback.format_exc(), err=True)
        sys.exit(1)