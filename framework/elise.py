# This will be the entry point for the two interfaces provided
import argparse
import os
from pathlib import Path
import subprocess
from threading import Thread
from time import sleep
import webview

# Framework dependencies
from webui.app import main as webui_main
from batch.submit import execute_simulation
from common.utils import get_executable, process_name, is_bundled

ELiSE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__)))

def run_gui():
    root_dir = Path(ELiSE_ROOT)
    if is_bundled():
        ws_path = root_dir.parent / process_name("ws_server")
    else:
        ws_path = root_dir / "webui" / process_name("ws_server")
    
    ws_cmd = get_executable(ws_path)
    ws_proc = subprocess.Popen(ws_cmd, env=os.environ.copy())
    
    # Start webui in a thread
    Thread(target=webui_main, args=("0.0.0.0", False), daemon=True).start()
    
    sleep(5)
    
    webview.create_window("ELiSE", "http://127.0.0.1:8050")
    webview.start()
    
    ws_proc.terminate()
    ws_proc.wait()

def run_webui():
    webui_main()

def run_cmdline(cmdargs):
    execute_simulation(cmdargs)

def main():
    parser = argparse.ArgumentParser(prog="elise", description="The entry point of ELiSE framework")

    # WebUI
    parser.add_argument("--webui", action="store_true", help="Launch the WebUI")

    # Commandline
    supported_providers = ["openmpi", "intelmpi", "mp"]
    parser.add_argument("-f", "--schematic-files", action="append", help="Provide a schematic file name")
    parser.add_argument("-p", "--provider", choices=supported_providers, default="mp", help="Define the provider for parallelizing tasks")
    parser.add_argument("--export_reports", default="", type=str, help="Provde a directory to export reports for each scheduler")
    
    args = parser.parse_args()

    if args.webui:
        run_webui()
    elif not args.schematic_files:
        run_gui()
    else:
        cmdargs = [f"-f {file}" for file in args.schematic_files]
        cmdargs.extend(["-p", args.provider])
        if args.export_reports:
            cmdargs.extend(["--export_reports", args.export_reports])
        run_cmdline(cmdargs)


if __name__ == "__main__":
    main()