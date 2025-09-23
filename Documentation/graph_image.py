import sys
import os
import subprocess
from dotenv import load_dotenv

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
        
        graph_instance = create_main_graph()
        graph = graph_instance.graph
        
        print("🔄 Generating graph using IPython display...")
        
        # Use IPython display to show the graph
        if IPYTHON_AVAILABLE:
            try:
                display(Image(graph.get_graph().draw_mermaid_png()))
                print("✅ Graph displayed using IPython")
                return True
            except Exception as e:
                print(f"⚠️ IPython display failed: {e}")
                print("🔄 Falling back to file generation...")
                return generate_graph_to_file(graph)
        else:
            print("⚠️ IPython not available, falling back to file generation...")
            return generate_graph_to_file(graph)
            
    except Exception as e:
        print(f"❌ Error generating graph with IPython: {e}")
        return False

def generate_graph_to_file(graph):
    """Generate graph and save to file"""
    
    try:
        print("🔄 Generating graph and saving to file...")
        
        # Generate PNG and save to file
        graph.get_graph().draw_mermaid_png(output_file_path="graph.png")
        print("✅ PNG image saved as graph.png")
        
        # Also generate Mermaid text
        mermaid_text = graph.get_graph().draw_mermaid()
        with open("graph.mmd", "w", encoding="utf-8") as f:
            f.write(mermaid_text)
        print("✅ Mermaid diagram saved as graph.mmd")
        
        return True
        
    except Exception as e:
        print(f"❌ Error generating graph to file: {e}")
        print("🔄 Falling back to Mermaid text only...")
        return generate_mermaid_fallback()

def generate_mermaid_fallback():
    """Fallback method to generate Mermaid text that can be converted to PNG"""
    
    try:
        # Import and create the actual graph
        from Graph_Flow.main_graph import create_main_graph
        
        graph_instance = create_main_graph()
        actual_graph = graph_instance.graph.get_graph()
        
        print("🔄 Generating Mermaid diagram text...")
        
        # Generate Mermaid text
        mermaid_text = actual_graph.draw_mermaid()
        
        # Save Mermaid text
        with open("graph.mmd", "w", encoding="utf-8") as f:
            f.write(mermaid_text)
        print("✅ Mermaid diagram saved as graph.mmd")
        
        # Try to convert using Mermaid CLI if available
        try:
            print("🔄 Attempting to convert to PNG using Mermaid CLI...")
            result = subprocess.run([
                'mmdc', '-i', 'graph.mmd', '-o', 'graph.png',
                '--width', '1200', '--height', '800',
                '--backgroundColor', 'white'
            ], capture_output=True, text=True, check=True)
            
            print("✅ PNG image saved as graph.png")
            return True
            
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("⚠️ Mermaid CLI not available")
            print("💡 To convert to PNG:")
            print("   1. Install Mermaid CLI: npm install -g @mermaid-js/mermaid-cli")
            print("   2. Run: mmdc -i graph.mmd -o graph.png")
            print("   3. Or view online at: https://mermaid.live")
            return True
            
    except Exception as e:
        print(f"❌ Error generating Mermaid fallback: {e}")
        return False

if __name__ == "__main__":
    try:
        print("🔄 Generating graph using IPython display...")
        
        # Use IPython display method
        success = generate_graph_with_ipython()
        
        if success:
            print("✅ Graph generation completed!")
        else:
            print("❌ Failed to generate graph")
            
    except Exception as e:
        print(f"❌ Error generating graph: {e}")
        import traceback
        traceback.print_exc() 