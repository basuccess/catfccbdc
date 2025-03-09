import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from threading import Thread
import subprocess
import os
import sys
import logging
from main import main as run_catfccbdc
from featurecount import main as run_featurecount

# Redirect logging to GUI text widget
class TextHandler(logging.Handler):
    def __init__(self, text_widget):
        logging.Handler.__init__(self)
        self.text_widget = text_widget

    def emit(self, record):
        msg = self.format(record)
        self.text_widget.insert(tk.END, msg + '\n')
        self.text_widget.see(tk.END)

def setup_logging_to_text(text_widget, log_file=None, log_level='INFO', log_parts=None):
    log_format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s'
    level = getattr(logging, log_level.upper(), logging.INFO)
    
    # Clear existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Text widget handler
    text_handler = TextHandler(text_widget)
    text_handler.setFormatter(logging.Formatter(log_format))
    logging.root.addHandler(text_handler)
    
    # File handler (optional)
    if log_file:
        file_handler = logging.FileHandler(log_file, mode='w')
        file_handler.setFormatter(logging.Formatter(log_format))
        logging.root.addHandler(file_handler)
    
    logging.root.setLevel(level)
    
    if log_parts:
        for part in log_parts:
            logging.getLogger(part.strip()).setLevel(level)
    logging.info("Logging configured for GUI.")

def run_processing(base_dir, states, log_file, log_level, log_parts, output_text):
    try:
        # Simulate command-line arguments
        sys.argv = [
            'catfccbdc_gui.py',
            '--base-dir', base_dir,
            '--state', *states.split(),
            '--log-file', log_file if log_file else 'debug.log',
            '--log-level', log_level,
            '--log-parts', *(log_parts.split() if log_parts else [])
        ]
        run_catfccbdc()
        output_text.insert(tk.END, f"\nProcessing completed for states: {states}\n")
    except Exception as e:
        logging.error(f"Error during processing: {str(e)}")
        messagebox.showerror("Error", f"Processing failed: {str(e)}")

def run_feature_count(base_dir, state, output_text):
    try:
        # Run featurecount.py as a subprocess to capture output
        cmd = [sys.executable, '-c', 
               f"from featurecount import main; main(['--base-dir', '{base_dir}', '--state', '{state}'])"]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()
        if process.returncode == 0:
            output_text.insert(tk.END, f"\n{stdout}")
        else:
            output_text.insert(tk.END, f"\nError: {stderr}")
    except Exception as e:
        logging.error(f"Error during feature count: {str(e)}")
        messagebox.showerror("Error", f"Feature count failed: {str(e)}")

class CatFCCBDCGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("CatFCCBDC - Broadband Data Processor")
        self.root.geometry("800x600")

        # Frame for inputs
        input_frame = ttk.Frame(root, padding="10")
        input_frame.pack(fill=tk.BOTH, expand=False)

        # Base Directory
        ttk.Label(input_frame, text="Base Directory:").grid(row=0, column=0, sticky="w", pady=5)
        self.base_dir_var = tk.StringVar(value=os.getcwd())
        base_dir_entry = ttk.Entry(input_frame, textvariable=self.base_dir_var, width=50)
        base_dir_entry.grid(row=0, column=1, sticky="ew", pady=5)
        ttk.Button(input_frame, text="Browse", command=self.browse_base_dir).grid(row=0, column=2, padx=5)

        # States
        ttk.Label(input_frame, text="States (e.g., TX AL):").grid(row=1, column=0, sticky="w", pady=5)
        self.states_var = tk.StringVar(value="TX")
        ttk.Entry(input_frame, textvariable=self.states_var, width=50).grid(row=1, column=1, sticky="ew", pady=5)

        # Log File
        ttk.Label(input_frame, text="Log File (optional):").grid(row=2, column=0, sticky="w", pady=5)
        self.log_file_var = tk.StringVar(value="debug.log")
        log_file_entry = ttk.Entry(input_frame, textvariable=self.log_file_var, width=50)
        log_file_entry.grid(row=2, column=1, sticky="ew", pady=5)
        ttk.Button(input_frame, text="Browse", command=self.browse_log_file).grid(row=2, column=2, padx=5)

        # Log Level
        ttk.Label(input_frame, text="Log Level:").grid(row=3, column=0, sticky="w", pady=5)
        self.log_level_var = tk.StringVar(value="INFO")
        ttk.Combobox(input_frame, textvariable=self.log_level_var, values=["DEBUG", "INFO", "WARNING", "ERROR"], state="readonly").grid(row=3, column=1, sticky="ew", pady=5)

        # Log Parts
        ttk.Label(input_frame, text="Log Parts (e.g., main tabblockmerge):").grid(row=4, column=0, sticky="w", pady=5)
        self.log_parts_var = tk.StringVar(value="main tabblockmerge")
        ttk.Entry(input_frame, textvariable=self.log_parts_var, width=50).grid(row=4, column=1, sticky="ew", pady=5)

        # Buttons
        button_frame = ttk.Frame(root, padding="10")
        button_frame.pack(fill=tk.BOTH, expand=False)
        ttk.Button(button_frame, text="Run Processing", command=self.start_processing).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Check Feature Count", command=self.start_feature_count).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Clear Output", command=lambda: self.output_text.delete(1.0, tk.END)).pack(side=tk.LEFT, padx=5)

        # Output Text
        self.output_text = tk.Text(root, height=20, width=80, wrap=tk.WORD)
        self.output_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        scrollbar = ttk.Scrollbar(root, command=self.output_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.output_text.config(yscrollcommand=scrollbar.set)

        # Setup logging
        setup_logging_to_text(self.output_text, self.log_file_var.get(), self.log_level_var.get(), self.log_parts_var.get().split())

    def browse_base_dir(self):
        directory = filedialog.askdirectory(initialdir=self.base_dir_var.get())
        if directory:
            self.base_dir_var.set(directory)

    def browse_log_file(self):
        file = filedialog.asksaveasfilename(defaultextension=".log", initialfile=self.log_file_var.get(), filetypes=[("Log files", "*.log"), ("All files", "*.*")])
        if file:
            self.log_file_var.set(file)

    def start_processing(self):
        self.output_text.insert(tk.END, "Starting CatFCCBDC processing...\n")
        thread = Thread(target=run_processing, args=(
            self.base_dir_var.get(),
            self.states_var.get(),
            self.log_file_var.get(),
            self.log_level_var.get(),
            self.log_parts_var.get(),
            self.output_text
        ))
        thread.start()

    def start_feature_count(self):
        self.output_text.insert(tk.END, f"Checking feature count for state {self.states_var.get().split()[0]}...\n")
        thread = Thread(target=run_feature_count, args=(
            self.base_dir_var.get(),
            self.states_var.get().split()[0],  # Take first state if multiple
            self.output_text
        ))
        thread.start()

if __name__ == "__main__":
    root = tk.Tk()
    app = CatFCCBDCGUI(root)
    root.mainloop()