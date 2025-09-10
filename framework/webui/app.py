from dash_extensions.enrich import DashProxy, Input, Output, dcc, html, callback
import dash_bootstrap_components as dbc
# from flask import Flask, request, jsonify
import argparse
from multiprocessing import freeze_support, Process
import os
from pathlib import Path
import subprocess
import sys
from time import sleep
from uuid import uuid4


# Start from ELiSE framework root
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))
from common import utils as cutils
from webui.layouts.main import main_layout
from webui.utils.action_utils import action_item_name, action_items_names

ELiSE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Store session information
app_session_store = dcc.Store(
        id="app-session-store",
        storage_type="session",
        data=dict(
            sid=str(uuid4())
            )
        )

actions_data = {
    action_item_name(name):{"inputs": [], "schedulers": []}
    for name in action_items_names
}

# Store schematic data
app_schematic_store = dcc.Store(
        id="app-sim-schematic",
        storage_type="session",
        data=dict(
            name="",
            description="",
            inputs=dict(),
            schedulers=dict(),
            actions=actions_data
        )
)

# Store simulation progress
app_progress_store = dcc.Store(
    id="app-progress-store",
    storage_type="memory",
    data=dict(
        progress=0
    ) 
)

app_results_store = dcc.Store(
    id="app-results-store",
    storage_type="memory",
    data=list()
)

# Defining the application
app = DashProxy(__name__,
                compress=True,
                meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
                external_stylesheets=["assets/css/bootstrap.min.css", "assets/css/bootstrap-icons.css", "assets/css/schematic_components.css"])

# Application configuration
# app.config.suppress_callback_exceptions = True

# Defining the layout
app.layout = dbc.Container([

    app_session_store,
    app_schematic_store,
    app_progress_store,
    app_results_store,
    main_layout

], fluid=True, class_name="mh-100", style={"height": "100vh"}, id="layout")

app.title = "ELiSE"
app.config.update_title = None

def main(ip_addr="0.0.0.0", launch_ws_server=True):
    # ws_server_exec_path = Path(ELiSE_ROOT) / "webui" / "ws_server.py"
    # ws_server_process = subprocess.Popen(["python", str(ws_server_exec_path)])
    if launch_ws_server:
        root_dir = Path(ELiSE_ROOT)
        if cutils.is_bundled():
            ws_path = root_dir.parent / cutils.process_name("ws_server")
        else:
            ws_path = root_dir / "webui" / cutils.process_name("ws_server")
        
        ws_cmd = cutils.get_executable(ws_path)
        ws_server_proc = subprocess.Popen(ws_cmd, env=os.environ.copy())

    # Start application
    gui_debug = cutils.envvar_bool_val("ELiSE_GUI_DEBUG")
    try:
        app.run(host=ip_addr, port=8050, debug=gui_debug)
    finally:
        if launch_ws_server:
            ws_server_proc.terminate()
            ws_server_proc.wait()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Starts the WebUI and by default the websocket server")
    parser.add_argument("--ip_addr", type=str, default="0.0.0.0")
    parser.add_argument("--launch_ws_server", default=False, action="store_true")
    args = parser.parse_args()
    main(args.ip_addr, args.launch_ws_server)