
import sys
import logging
import datetime
from typing import Optional

# Attempt to import rich
try:
    from rich.console import Console
    from rich.panel import Panel
    from rich.text import Text
    from rich.logging import RichHandler
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
    from rich.theme import Theme
    
    custom_theme = Theme({
        "info": "white",
        "warning": "yellow",
        "error": "bold red",
        "success": "green",
        "banner": "bold white",
        "section": "bold blue"
    })
    
    console = Console(theme=custom_theme, force_terminal=True) # Force terminal for color, but check isatty for progress
    HAS_RICH = True
except ImportError:
    HAS_RICH = False
    console = None

# --- CONSTANTS ---
DIVIDER = "=" * 60

# --- LOGGING SETUP ---
def setup_logging(log_file_path: Optional[str] = None):
    """
    Sets up logging.
    """
    handlers = []
    
    # We rely on manual print_info/etc for console output to avoid duplication
    # when running under controller, so we don't add a StreamHandler here often.
    # But for standalone runs, we might need it.
    # actually, purely relying on the scripts calling print_info is safer for CLI tools.
    # We will ONLY add a file handler here if requested.
    
    if log_file_path:
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(message)s', 
        datefmt="[%X]",
        handlers=handlers,
        force=True
    )

# --- UI ELEMENT: BANNER ---
def print_banner(title: str, subtitle: str = ""):
    if HAS_RICH:
        console.print("")
        console.print(f"[banner]*** {title.upper()} ***[/]")
        if subtitle:
            console.print(f"[dim]    {subtitle}[/]")
        console.print("")
    else:
        print(f"\n{DIVIDER}")
        print(f"*** {title.upper()} ***")
        if subtitle:
            print(f"    {subtitle}")
        print(f"{DIVIDER}\n")

# --- UI ELEMENT: SECTION HEADER ---
def print_section(title: str):
    if HAS_RICH:
        console.print(f"\n[section]>>> {title.upper()}[/section]")
        console.print("[dim]" + "-" * 40 + "[/dim]")
    else:
        print(f"\n--- {title.upper()} ---")

# --- UI ELEMENT: STATUS MESSAGES ---
def print_success(message: str):
    if HAS_RICH:
        console.print(f"[success]✓ {message}[/]")
    else:
        print(f"[SUCCESS] {message}")

def print_error(message: str):
    if HAS_RICH:
        console.print(f"[error]✗ ERROR: {message}[/]")
    else:
        print(f"[ERROR] {message}")

def print_warning(message: str):
    if HAS_RICH:
        console.print(f"[warning]⚠ {message}[/]")
    else:
        print(f"[WARNING] {message}")

def print_info(message: str):
    if HAS_RICH:
        console.print(f"[info]  {message}[/]")
    else:
        print(f"  {message}")

# --- PROGRESS BAR HELPER ---
class DummyProgress:
    def __enter__(self): return self
    def __exit__(self, exc_type, exc_val, exc_tb): pass
    def add_task(self, *args, **kwargs): return 0
    def update(self, *args, **kwargs): pass

def create_progress():
    # Only return a real progress bar if we are in a TTY (interactive) 
    # AND rich is installed. 
    # When running under 00_Controller with captured pipes, isatty is False.
    if HAS_RICH and sys.stdout.isatty():
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=console
        )
    return DummyProgress()
