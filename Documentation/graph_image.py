import sys
import os
import subprocess
import re
from dotenv import load_dotenv

def _safe_text(text):
    try:
        return str(text).encode('ascii', 'ignore').decode('ascii')
    except Exception:
        return str(text)

def _fix_mermaid_syntax(mermaid_text):
    """
    Fix common Mermaid syntax issues that cause parsing errors
    """
    # CRITICAL: Replace reserved keywords FIRST before any other processing
    # Mermaid reserved keywords that cannot be used as node IDs
    reserved_keywords = ['end', 'start', 'graph', 'subgraph', 'style', 'class', 'click', 'call', 'default']
    
    # Replace reserved keywords in node definitions
    # Pattern: end(label) or end[label] or end
    for keyword in reserved_keywords:
        # Replace standalone keyword followed by parentheses (node with label)
        mermaid_text = re.sub(rf'\b{keyword}\s*\(', f'{keyword}_node(', mermaid_text)
        # Replace standalone keyword followed by square brackets
        mermaid_text = re.sub(rf'\b{keyword}\s*\[', f'{keyword}_node[', mermaid_text)
        # Replace keyword in edges: something --> end
        mermaid_text = re.sub(rf'-->\s*{keyword}\b', f'--> {keyword}_node', mermaid_text)
        mermaid_text = re.sub(rf'-\.->\s*{keyword}\b', f'-.-> {keyword}_node', mermaid_text)
        # Replace keyword at start of edges: end --> something
        mermaid_text = re.sub(rf'\b{keyword}\s*-->', f'{keyword}_node -->', mermaid_text)
        mermaid_text = re.sub(rf'\b{keyword}\s*-\.->',f'{keyword}_node -.->', mermaid_text)
    
    # Also handle __end__ and __start__ special nodes
    mermaid_text = re.sub(r'\b__end__\b', '__final__', mermaid_text)
    mermaid_text = re.sub(r'\b__start__\b', '__begin__', mermaid_text)
    
    # Now process line by line for other fixes
    lines = mermaid_text.split('\n')
    fixed_lines = []
    
    for line in lines:
        # Skip empty lines
        if not line.strip():
            fixed_lines.append(line)
            continue
        
        # Remove tabs and replace with spaces
        line = line.replace('\t', '    ')
        
        # Fix any double quotes issues
        line = line.replace('""', '"')
        
        # Remove any trailing semicolons (not needed in Mermaid)
        line = line.rstrip(';')
        
        # Remove extra whitespace
        line = ' '.join(line.split())
        
        fixed_lines.append(line)
    
    return '\n'.join(fixed_lines)

def _create_simple_mermaid_fallback(compiled_graph):
    """
    Create a simplified Mermaid diagram by inspecting the graph structure
    This is a fallback when the auto-generated diagram has issues
    """
    try:
        graph_drawable = compiled_graph.get_graph()
        
        # Get nodes and edges from the graph
        nodes = graph_drawable.nodes
        edges = graph_drawable.edges
        
        # Mermaid reserved keywords that must be avoided
        reserved_keywords = {'end', 'start', 'graph', 'subgraph', 'style', 'class', 'click', 'call', 'default'}
        
        # Build a simple Mermaid diagram manually
        mermaid_lines = [
            "%%{init: {'flowchart': {'curve': 'linear'}}}%%",
            "graph TD;",
        ]
        
        # Add nodes
        node_map = {}
        for i, node in enumerate(nodes):
            node_id = node.id if hasattr(node, 'id') else str(node)
            
            # Clean node ID for Mermaid and avoid reserved keywords
            clean_id = re.sub(r'[^\w]', '_', node_id)
            
            # If it's a reserved keyword, append _node
            base_id = clean_id.lower().strip('_')
            if base_id in reserved_keywords:
                clean_id = f"{clean_id}_node"
            
            # Handle __start__ and __end__ special nodes
            if clean_id == '__end__':
                clean_id = '__final__'
            elif clean_id == '__start__':
                clean_id = '__begin__'
            
            node_map[node_id] = clean_id
            
            # Create node with label
            label = node_id.replace('__', '').replace('_', ' ').title()
            if label.lower() in reserved_keywords:
                label = label + " Node"
            
            mermaid_lines.append(f"    {clean_id}[\"{label}\"];")
        
        # Add edges
        for edge in edges:
            source = edge.source if hasattr(edge, 'source') else str(edge[0])
            target = edge.target if hasattr(edge, 'target') else str(edge[1])
            
            source_id = node_map.get(source, re.sub(r'[^\w]', '_', source))
            target_id = node_map.get(target, re.sub(r'[^\w]', '_', target))
            
            # Ensure source and target IDs are not reserved keywords
            if source_id.lower().strip('_') in reserved_keywords:
                source_id = f"{source_id}_node"
            if target_id.lower().strip('_') in reserved_keywords:
                target_id = f"{target_id}_node"
            
            mermaid_lines.append(f"    {source_id} --> {target_id};")
        
        return '\n'.join(mermaid_lines)
    
    except Exception as e:
        print(_safe_text(f"Error creating simple fallback: {e}"))
        return None

def _validate_mermaid_syntax(mermaid_text):
    """
    Validate Mermaid syntax
    """
    try:
        lines = [l.strip() for l in mermaid_text.split('\n') if l.strip()]
        
        # Must have graph declaration
        has_declaration = any('graph ' in line or 'flowchart' in line for line in lines)
        if not has_declaration:
            return False, "Missing graph declaration"
        
        # Check for balanced parentheses
        open_count = sum(line.count('(') for line in lines)
        close_count = sum(line.count(')') for line in lines)
        if open_count != close_count:
            return False, f"Unbalanced parentheses: {open_count} open, {close_count} close"
        
        # Check for balanced brackets
        open_brackets = sum(line.count('[') for line in lines)
        close_brackets = sum(line.count(']') for line in lines)
        if open_brackets != close_brackets:
            return False, f"Unbalanced brackets: {open_brackets} open, {close_brackets} close"
        
        return True, "Valid"
        
    except Exception as e:
        return False, str(e)

load_dotenv()
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

try:
    from IPython.display import Image, display
    IPYTHON_AVAILABLE = True
except ImportError:
    IPYTHON_AVAILABLE = False

try:
    from langchain_core.runnables.graph import MermaidDrawMethod
    MERMAID_DRAW_METHOD_AVAILABLE = True
except ImportError:
    MERMAID_DRAW_METHOD_AVAILABLE = False

def generate_graph_to_file(compiled_graph):
    """Main function to generate graph files"""
    try:
        print(_safe_text("Generating Mermaid diagram..."))
        
        graph_drawable = compiled_graph.get_graph()
        
        # Generate Mermaid text
        try:
            mermaid_text = graph_drawable.draw_mermaid()
            print(_safe_text("Generated Mermaid text from graph"))
        except Exception as e:
            print(_safe_text(f"Error generating Mermaid: {e}"))
            print(_safe_text("Attempting to create simplified diagram..."))
            mermaid_text = _create_simple_mermaid_fallback(compiled_graph)
            if not mermaid_text:
                return False
        
        # Fix syntax issues
        fixed_mermaid_text = _fix_mermaid_syntax(mermaid_text)
        
        # Validate syntax
        is_valid, validation_msg = _validate_mermaid_syntax(fixed_mermaid_text)
        print(_safe_text(f"Mermaid validation: {validation_msg}"))
        
        # Save Mermaid file
        with open("graph.mmd", "w", encoding="utf-8") as f:
            f.write(fixed_mermaid_text)
        print(_safe_text("Saved graph.mmd"))
        
        # Also save the original for debugging
        with open("graph_original.mmd", "w", encoding="utf-8") as f:
            f.write(mermaid_text)
        print(_safe_text("Saved graph_original.mmd (for debugging)"))
        
        if not is_valid:
            print(_safe_text(f"WARNING: Mermaid syntax may have issues: {validation_msg}"))
            print(_safe_text("Attempting to create simplified version..."))
            
            simple_mermaid = _create_simple_mermaid_fallback(compiled_graph)
            if simple_mermaid:
                with open("graph_simple.mmd", "w", encoding="utf-8") as f:
                    f.write(simple_mermaid)
                print(_safe_text("Created simplified diagram: graph_simple.mmd"))
                fixed_mermaid_text = simple_mermaid
        
        # Try to generate PNG using multiple methods
        png_success = False
        
        # Method 1: Pyppeteer (local rendering - most reliable)
        if MERMAID_DRAW_METHOD_AVAILABLE and not png_success:
            try:
                print(_safe_text("\nAttempting PNG generation with Pyppeteer..."))
                import nest_asyncio
                nest_asyncio.apply()

                # Generate PNG using Pyppeteer
                graph_drawable.draw_mermaid_png(
                    draw_method=MermaidDrawMethod.PYPPETEER,
                    output_file_path="graph.png"
                )

                # Check if file was created and is valid
                if os.path.exists("graph.png"):
                    size = os.path.getsize("graph.png")
                    if size > 1000:
                        print(_safe_text(f"SUCCESS: PNG generated with Pyppeteer ({size:,} bytes)"))
                        png_success = True
                    else:
                        print(_safe_text(f"Pyppeteer created file but size too small: {size} bytes"))
                else:
                    print(_safe_text("Pyppeteer did not create PNG file"))

            except ImportError:
                print(_safe_text("Pyppeteer not installed. Install: pip install pyppeteer nest-asyncio"))
            except Exception as e:
                print(_safe_text(f"Pyppeteer failed: {e}"))
        
        # Method 2: Mermaid CLI
        if not png_success:
            print(_safe_text("\nAttempting PNG generation with Mermaid CLI..."))
            cli_success = try_mermaid_cli("graph.mmd", "graph.png")
            if cli_success:
                png_success = True
                print(_safe_text("SUCCESS: PNG generated with Mermaid CLI"))
        
        # Method 3: Try with simplified diagram if available
        if not png_success and os.path.exists("graph_simple.mmd"):
            print(_safe_text("\nTrying with simplified diagram..."))
            cli_success = try_mermaid_cli("graph_simple.mmd", "graph.png")
            if cli_success:
                png_success = True
                print(_safe_text("SUCCESS: PNG generated from simplified diagram"))
        
        if not png_success:
            print(_safe_text("\n" + "="*50))
            print(_safe_text("PNG generation was not successful"))
            print(_safe_text("="*50))
            print(_safe_text("\nYou can convert the .mmd file to PNG using:"))
            print(_safe_text("\n1. Online: https://mermaid.live"))
            print(_safe_text("   - Open the website"))
            print(_safe_text("   - Copy contents from graph.mmd"))
            print(_safe_text("   - Paste and download PNG"))
            print(_safe_text("\n2. Install Pyppeteer:"))
            print(_safe_text("   pip install pyppeteer nest-asyncio"))
            print(_safe_text("\n3. Install Mermaid CLI:"))
            print(_safe_text("   npm install -g @mermaid-js/mermaid-cli"))
            print(_safe_text("   mmdc -i graph.mmd -o graph.png"))
        
        return True
        
    except Exception as e:
        print(_safe_text(f"Error in generate_graph_to_file: {e}"))
        import traceback
        traceback.print_exc()
        return False

def try_mermaid_cli(input_file, output_file):
    """Try to use Mermaid CLI to generate PNG"""
    try:
        # Try different ways to call Mermaid CLI
        commands = [
            # Method 1: Direct mmdc command (if in PATH)
            ['mmdc', '-i', input_file, '-o', output_file, '-b', 'white', '--width', '1200', '--height', '800'],
            # Method 2: Node.js script (if installed globally)
            ['node', 'C:\\Users\\zeesh\\AppData\\Roaming\\npm\\mmdc', '-i', input_file, '-o', output_file, '-b', 'white', '--width', '1200', '--height', '800'],
            # Method 3: PowerShell script (Windows)
            ['powershell', '-Command', f'& \"C:\\Users\\zeesh\\AppData\\Roaming\\npm\\mmdc.ps1\" -i \"{input_file}\" -o \"{output_file}\" -b white --width 1200 --height 800'],
            # Method 4: npx (if available)
            ['npx', '@mermaid-js/mermaid-cli', '-i', input_file, '-o', output_file, '-b', 'white', '--width', '1200', '--height', '800'],
        ]

        for i, cmd in enumerate(commands):
            try:
                print(_safe_text(f"  Trying method {i+1}: {' '.join(cmd[:3])}..."))
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=45
                )
                if os.path.exists(output_file) and os.path.getsize(output_file) > 1000:
                    print(_safe_text(f"  SUCCESS: Generated {os.path.getsize(output_file)} byte PNG"))
                    return True
                else:
                    print(_safe_text(f"  Method {i+1} completed but no valid PNG file created"))
            except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
                print(_safe_text(f"  Method {i+1} failed: {type(e).__name__}"))
                continue
            except Exception as e:
                print(_safe_text(f"  Method {i+1} error: {e}"))
                continue

        print(_safe_text("  All Mermaid CLI methods failed"))
        return False
    except Exception as e:
        print(_safe_text(f"  Mermaid CLI function error: {e}"))
        return False

if __name__ == "__main__":
    try:
        print(_safe_text("="*50))
        print(_safe_text("LangGraph Diagram Generator"))
        print(_safe_text("="*50 + "\n"))
        
        from Graph_Flow.main_graph import create_main_graph
        
        print(_safe_text("Loading graph..."))
        compiled_graph = create_main_graph()
        
        if hasattr(compiled_graph, 'graph'):
            compiled_graph = compiled_graph.graph
        
        print(_safe_text("Graph loaded successfully\n"))
        
        success = generate_graph_to_file(compiled_graph)
        
        if success:
            print(_safe_text("\n" + "="*50))
            print(_safe_text("Graph Generation Complete"))
            print(_safe_text("="*50))
            
            files = {
                "graph.png": "PNG Image (if generated)",
                "graph.mmd": "Mermaid Diagram (fixed)",
                "graph_original.mmd": "Original Mermaid (for debugging)",
                "graph_simple.mmd": "Simplified Mermaid (if created)"
            }
            
            print(_safe_text("\nGenerated files:"))
            for filename, description in files.items():
                if os.path.exists(filename):
                    size = os.path.getsize(filename)
                    abs_path = os.path.abspath(filename)
                    print(_safe_text(f"\n  {filename}"))
                    print(_safe_text(f"    {description}"))
                    print(_safe_text(f"    Size: {size:,} bytes"))
                    print(_safe_text(f"    Path: {abs_path}"))
            
            print(_safe_text("\n" + "="*50))
        else:
            print(_safe_text("\nFailed to generate graph files"))
            
    except Exception as e:
        print(_safe_text(f"\nError: {e}"))
        import traceback
        traceback.print_exc()