import sys
import os
import subprocess
from dotenv import load_dotenv

# Ensure console-safe output on Windows by stripping non-ASCII characters
def _safe_text(text):
    try:
        return str(text).encode('ascii', 'ignore').decode('ascii')
    except Exception:
        return str(text)

# Load environment variables from .env file
load_dotenv()

# Add the parent directory to the path so we can import from Graph_Flow
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Try to import IPython display for image rendering
try:
    from IPython.display import Image, display
    IPYTHON_AVAILABLE = True
except ImportError:
    IPYTHON_AVAILABLE = False

def generate_graph_with_ipython():
    """Use IPython display to generate and show graph image"""
    
    try:
        # Import and create the actual graph
        from Graph_Flow.main_graph import create_main_graph
        
        # FIXED: create_main_graph() returns CompiledStateGraph directly
        compiled_graph = create_main_graph()
        
        # Check if it's wrapped in an object or returned directly
        if hasattr(compiled_graph, 'graph'):
            compiled_graph = compiled_graph.graph
        
        print(_safe_text("Generating graph using IPython display..."))
        
        # Use IPython display to show the graph
        if IPYTHON_AVAILABLE:
            try:
                # FIXED: Call get_graph() directly on the compiled graph
                display(Image(compiled_graph.get_graph().draw_mermaid_png()))
                print(_safe_text("Graph displayed using IPython"))
                return True
            except Exception as e:
                print(_safe_text(f"IPython display failed: {e}"))
                print(_safe_text("Falling back to file generation..."))
                return generate_graph_to_file(compiled_graph)
        else:
            print(_safe_text("IPython not available, falling back to file generation..."))
            return generate_graph_to_file(compiled_graph)
            
    except Exception as e:
        print(_safe_text(f"Error generating graph with IPython: {e}"))
        import traceback
        print(_safe_text("Traceback:"))
        tb_lines = traceback.format_exc().split('\n')
        for line in tb_lines:
            print(_safe_text(line))
        return False

def generate_graph_to_file(compiled_graph):
    """Generate graph and save to file"""
    
    try:
        print(_safe_text("Generating graph and saving to file..."))
        
        # FIXED: Call get_graph() on the compiled graph, then draw methods
        graph_drawable = compiled_graph.get_graph()
        
        # Generate PNG and save to file
        png_data = graph_drawable.draw_mermaid_png()
        with open("graph.png", "wb") as f:
            f.write(png_data)
        print(_safe_text("PNG image saved as graph.png"))
        
        # Also generate Mermaid text
        mermaid_text = graph_drawable.draw_mermaid()
        with open("graph.mmd", "w", encoding="utf-8") as f:
            f.write(mermaid_text)
        print(_safe_text("Mermaid diagram saved as graph.mmd"))
        
        return True
        
    except Exception as e:
        print(_safe_text(f"Error generating graph to file: {e}"))
        import traceback
        print(_safe_text("Traceback:"))
        tb_lines = traceback.format_exc().split('\n')
        for line in tb_lines:
            print(_safe_text(line))
        print(_safe_text("Falling back to Mermaid text only..."))
        return generate_mermaid_fallback()

def generate_mermaid_fallback():
    """Fallback method to generate Mermaid text that can be converted to PNG"""
    
    try:
        # Import and create the actual graph
        from Graph_Flow.main_graph import create_main_graph
        
        # FIXED: create_main_graph() returns CompiledStateGraph directly
        compiled_graph = create_main_graph()
        
        # Check if it's wrapped in an object or returned directly
        if hasattr(compiled_graph, 'graph'):
            compiled_graph = compiled_graph.graph
        
        print(_safe_text("Generating Mermaid diagram text..."))
        
        # FIXED: Get the drawable graph representation
        graph_drawable = compiled_graph.get_graph()
        
        # Generate Mermaid text
        mermaid_text = graph_drawable.draw_mermaid()
        
        # Save Mermaid text
        with open("graph.mmd", "w", encoding="utf-8") as f:
            f.write(mermaid_text)
        print(_safe_text("Mermaid diagram saved as graph.mmd"))
        
        # Try to convert using Mermaid CLI if available
        try:
            print(_safe_text("Attempting to convert to PNG using Mermaid CLI..."))
            result = subprocess.run([
                'mmdc', '-i', 'graph.mmd', '-o', 'graph.png',
                '--width', '1200', '--height', '800',
                '--backgroundColor', 'white'
            ], capture_output=True, text=True, check=True)
            
            print(_safe_text("PNG image saved as graph.png"))
            return True
            
        except (subprocess.CalledProcessError, FileNotFoundError) as e:
            print(_safe_text("Mermaid CLI not available"))
            print(_safe_text("To convert to PNG:"))
            print(_safe_text("   1. Install Mermaid CLI: npm install -g @mermaid-js/mermaid-cli"))
            print(_safe_text("   2. Run: mmdc -i graph.mmd -o graph.png"))
            print(_safe_text("   3. Or view online at: https://mermaid.live"))
            return True
            
    except Exception as e:
        print(_safe_text(f"Error generating Mermaid fallback: {e}"))
        import traceback
        print(_safe_text("Traceback:"))
        tb_lines = traceback.format_exc().split('\n')
        for line in tb_lines:
            print(_safe_text(line))
        return False

if __name__ == "__main__":
    try:
        print(_safe_text("Starting graph generation..."))
        
        # Use IPython display method
        success = generate_graph_with_ipython()
        
        if success:
            print(_safe_text("\n" + "="*50))
            print(_safe_text("SUCCESS: Graph generation completed!"))
            print(_safe_text("="*50))
            print(_safe_text("Check the following files:"))
            if os.path.exists("graph.png"):
                abs_path = os.path.abspath("graph.png")
                print(_safe_text(f"  - graph.png"))
                print(_safe_text(f"    Location: {abs_path}"))
            if os.path.exists("graph.mmd"):
                abs_path = os.path.abspath("graph.mmd")
                print(_safe_text(f"  - graph.mmd"))
                print(_safe_text(f"    Location: {abs_path}"))
                print(_safe_text("    View at: https://mermaid.live"))
            print(_safe_text("="*50))
        else:
            print(_safe_text("\n" + "="*50))
            print(_safe_text("FAILED: Could not generate graph"))
            print(_safe_text("="*50))
            
    except Exception as e:
        print(_safe_text(f"\nError in main execution: {e}"))
        import traceback
        print(_safe_text("\nTraceback:"))
        tb_lines = traceback.format_exc().split('\n')
        for line in tb_lines:
            print(_safe_text(line))