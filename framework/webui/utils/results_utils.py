# Python dependencies
import json
import plotly.graph_objects as go
from plotly.io import from_json
import os
from datetime import timedelta
import sys

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../..")
))

# Dash dependencies
from dash_extensions.WebSocket import WebSocket
import dash_bootstrap_components as dbc
from dash_extensions.enrich import Input, Output, State, callback, html, ALL, clientside_callback, MATCH, callback_context, dcc, dash_table
from dash.exceptions import PreventUpdate

# WebUI dependencies
from webui.utils.common_utils import get_session_dir

def import_results(path: str):
    # If it exists and is a file return the contents
    # else return None
    contents = None
    if os.path.exists(path) and os.path.isfile(path):
        with open(path, "r") as fd:
            contents = fd.read()
    return contents
            
def fork_results(inputs_ids, schedulers_ids, action_args, path: str):
    
    res = dict()

    # If one of them is an empty list
    inputs = action_args["inputs"]
    schedulers = action_args["schedulers"]
    if not inputs or not schedulers:
        return res

    # If the directory does not exist
    if not os.path.isdir(path):
        return res
    
    files = os.listdir(path)

    for inp_idx in inputs:
        webui_input_id = inputs_ids[inp_idx-1]
        input_key = f"input-{inp_idx-1}"
        res[input_key] = dict()
        for sched_idx in schedulers:
            webui_scheduler_id = schedulers_ids[sched_idx - 1]
            file_prefix = f"input_{inp_idx-1}_scheduler_{sched_idx-1}."

            filename = ""
            for file in files:
                if file_prefix in file:
                    filename = file
                    break
            
            if not filename:
                pass
            
            res[webui_input_id][webui_scheduler_id] = import_results(f"{path}/{filename}")

        res[webui_input_id] = {key: res[webui_input_id][key] for key in sorted(res[webui_input_id])}
    
    return res

def get_experiment_results(schematic_data, session_data):
    # Get results directory
    session_dir = get_session_dir(session_data)
    results_dir = f"{session_dir}/results"

    # Define basic experiment structure
    results = dict()
    results["schematic-data"] = schematic_data # Use it to replay an experiment
    results["time-reports"] = import_results(f"{results_dir}/time_reports.csv")

    def conditional_action(action: str, out_dir: str):
        if action in schematic_data["actions"]:
            inputs_ids = list(schematic_data["inputs"].keys())
            schedulers_ids = list(schematic_data["schedulers"].keys())
            fork_res = fork_results(inputs_ids, schedulers_ids, schematic_data["actions"][action], f"{results_dir}/{out_dir}")
            if fork_res:
                results[action] = fork_res

    conditional_action("get-workloads", "workloads")
    conditional_action("get-gantt-diagrams", "gantt")
    conditional_action("get-waiting-queue-diagrams", "waiting_queue")
    conditional_action("get-jobs-throughput-diagrams", "jobs_throughput")
    conditional_action("get-unused-cores-diagrams", "unused_cores")
    conditional_action("get-animated-clusters", "animated_cluster")
    
    return results

def get_action_name(action: str):
    match action:
        case "time-reports":
            return "Report"
        case "get-workloads":
            return "Workloads"
        case "get-gantt-diagrams":
            return "Gantt Diagrams"
        case "get-waiting-queue-diagrams":
            return "Waiting Queue Diagrams"
        case "get-jobs-throughput-diagrams":
            return "Jobs Throughput Diagrams"
        case "get-unused-cores-diagrams":
            return "Unused Cores Diagrams"
        case "get-animated-clusters":
            return "Animated Clusters"

def get_action_label(action: str, exp_id: int):
    action_name = get_action_name(action)
    action_btn_icon = html.I(" ", className="bi bi-folder2-open", id={"type": "experiment-action-icon", "experiment": exp_id, "action": action})
    if action == "time-reports":
        action_btn_icon = html.I(" ", className="bi bi-table")
    return [action_btn_icon, action_name]

def get_input_name(input_id: str):
    return input_id.replace("input-", "Input ")

def get_input_label(exp_id: int, action: str, input_id: str, is_general: bool):
    input_name = get_input_name(input_id)
    input_btn_icon = html.I(" ", className="bi bi-folder2-open", id={"type": "experiment-action-input-icon", "experiment": exp_id, "action": action, "input": input_id})
    if is_general:
        input_btn_icon = html.I(" ", className="bi bi-graph-up")
    
    return [input_btn_icon, input_name]

def get_scheduler_name(sched_id: str, schematic_data: dict):
    scheduler_name = schematic_data["schedulers"][sched_id]["name"]
    return f"{sched_id.replace('scheduler-', '')}. {scheduler_name}"

def get_scheduler_label(exp_id: int, action: str, input_id: str, sched_id: str, schematic_data: dict):
    sched_index = list(schematic_data["schedulers"].keys()).index(sched_id)
    sched_name = schematic_data["schedulers"][sched_id]["name"]
    sched_btn_icon = html.I(" ", className="bi bi-graph-up")
    if action == "get-workloads":
        sched_btn_icon = html.I(" ", className="bi bi-table")
    return [sched_btn_icon, sched_name, html.Sub(f"({sched_index})", id={"type": "hover-sub", "experiment": exp_id, "action": action, "input": input_id, "scheduler": sched_id}, style={"display": "none"})]


def is_general_diagram(action: str):
    match action:
        case "get-gantt-diagrams":
            return False
        case "get-workloads":
            return False
        case "get-animated-clusters":
            return False
        case _:
            return True

def create_table(input):
    data = input.split("\n")
    table_data = [] 
    head = data[0].strip().split(",")
    for line in data[1:]:
        values = line.strip().split(",")
        table_data.append(dict(zip(head, values)))
    return head, table_data


@callback(
    Output("results-component-items", "children"),
    Input("app-results-store", "data"),
    prevent_initial_call=True
)
def create_results_tree(results_data):
    results_component_children = []
    for exp_id, exp in enumerate(results_data):
        schematic_data_replay = results_data[exp_id]["schematic-data"]

        exp_btn = dbc.Button(html.H6(f"Experiment {exp_id}"), id={"experiment": exp_id}, size="sm", outline=True, style={"textAlign": "left", "border": "none"})
        exp_children = []

        for action, action_val in exp.items():

            # Ignore if it is about schematic data
            if action == "schematic-data":
                continue

            action_btn = dbc.Button(get_action_label(action, exp_id), id={"experiment": exp_id, "action": action}, size="sm", outline=True, style={"textAlign": "left", "border": "none"})
            # exp_children.append(action_btn)

            if type(action_val) == dict:
                action_children = []
                for input_id, input_val in action_val.items():
                    general_diagram = is_general_diagram(action)
                    # input_btn = dbc.Button([html.I(" ", className="bi bi-folder2-open"), get_input_name(input_id)], id={"experiment": exp_id, "action": action, "input": input_id, "general": general_diagram}, size="sm", outline=True, style={"textAlign": "left", "border": "none"})
                    input_btn = dbc.Button(get_input_label(exp_id, action, input_id, general_diagram), id={"experiment": exp_id, "action": action, "input": input_id, "general": general_diagram}, size="sm", outline=True, style={"textAlign": "left", "border": "none"})
                    
                    # Check if we want a general diagram or a per scheduler diagram
                    if not general_diagram:
                    
                        input_children = []
                        for sched_id in input_val.keys():
                            sched_btn = dbc.Button(get_scheduler_label(exp_id, action, input_id, sched_id, schematic_data_replay), 
                                                   id={"experiment": exp_id, "action": action, "input": input_id, "scheduler": sched_id}, 
                                                   size="sm", 
                                                   outline=True, 
                                                   style={"textAlign": "left", "border": "none", "flex": 1, "width": "100%"}) 
                            popover = dbc.Popover(
                                id={"type": "hover", "experiment": exp_id, "action": action, "input": input_id, "scheduler": sched_id},
                                trigger="hover",
                                target={"experiment": exp_id, "action": action, "input": input_id, "scheduler": sched_id},
                                className="d-none"
                            )
                            input_children.append(html.Div([sched_btn, popover], style={"flex": 1}))
                        
                        input_collapse = dbc.Collapse(dbc.Stack(input_children), id={"type": "experiment-action-input-collapse", "experiment": exp_id, "action": action, "input": input_id}, style={"paddingLeft": "10%"}, is_open=True)

                        action_children.append(dbc.Stack([input_btn, input_collapse]))
                    
                    else:
                        action_children.append(input_btn)
                    
                action_collapse = dbc.Collapse(dbc.Stack(action_children), id={"type": "experiment-action-collapse", "experiment": exp_id, "action": action}, style={"paddingLeft": "10%"}, is_open=True)
                
                exp_children.append(dbc.Stack([action_btn, action_collapse]))

            else:
                exp_children.append(action_btn)
        
        exp_collapse = dbc.Collapse(dbc.Stack(exp_children), id={"type": "experiment-collapse", "experiment": exp_id}, style={"paddingLeft": "10%"})
        results_component_children.append(dbc.Stack([exp_btn, exp_collapse]))
        
    return results_component_children             

@callback(
    Output({"type": "experiment-collapse", "experiment": MATCH}, "is_open"),
    Input({"experiment": MATCH}, "n_clicks"),
    State({"type": "experiment-collapse", "experiment": MATCH}, "is_open"),
    prevent_initial_call=True
)
def experiment_collapses(n_clicks, is_open):
    return not is_open

@callback(
    Output({"type": "experiment-action-collapse", "experiment": MATCH, "action": MATCH}, "is_open"),
    Output({"type": "experiment-action-icon", "experiment": MATCH, "action": MATCH}, "className"),
    Input({"experiment": MATCH, "action": MATCH}, "n_clicks"),
    State({"type": "experiment-action-collapse", "experiment": MATCH, "action": MATCH}, "is_open"),
    prevent_initial_call=True
)
def experiment_action_collapses(n_clicks, is_open):
    class_name = "bi bi-folder2" if is_open else "bi bi-folder2-open"
    return not is_open, class_name

@callback(
    Output({"type": "experiment-action-input-collapse", "experiment": MATCH, "action": MATCH, "input": MATCH}, "is_open"),
    Output({"type": "experiment-action-input-icon", "experiment": MATCH, "action": MATCH, "input": MATCH}, "className"),
    Input({"experiment": MATCH, "action": MATCH, "input": MATCH, "general": False}, "n_clicks"),
    State({"type": "experiment-action-input-collapse", "experiment": MATCH, "action": MATCH, "input": MATCH}, "is_open"),
    prevent_initial_call=True
)
def experiment_input_collapses(n_clicks, is_open):
    class_name = "bi bi-folder2" if is_open else "bi bi-folder2-open"
    return not is_open, class_name

@callback(
    Output("main-canvas", "children", allow_duplicate=True),
    Input({"experiment": ALL, "action": "time-reports"}, "n_clicks"),
    State("app-results-store", "data"),
    prevent_initial_call=True
)
def draw_report_table(n_clicks, results_data):
    if not any(n_clicks):
        raise PreventUpdate
    
    triggered_id = callback_context.triggered_id
    exp_id = int(triggered_id["experiment"])
    time_reports = results_data[exp_id]["time-reports"]
    
    # Create table data and header
    head, table_data = create_table(time_reports)

    # Create columns
    columns = list()
    for head_col in head:
        columns.append({"name": head_col, "id": head_col})
    
    return dash_table.DataTable(
        data=table_data, 
        columns=columns,
        style_table={"height": "100vh", "overflowY": "scroll", "whiteSpace": "pre-line"},
        style_header={"backgroundColor": "#111111", "color": "white", "whiteSpace": "normal", "height": "auto", "fontWeight": "bold", "textAlign": "center", "position": "sticky", "top": 0},
        style_cell={"backgroundColor": "#141414", "color": "#bbbbbb","textAlign": "center"},
        style_data_conditional=[
            {
                "if": {"column_id": "Simulation ID"},
                "color": "orange"
            }
        ]
    )

@callback(
    Output("main-canvas", "children", allow_duplicate=True),
    Input({"experiment": ALL, "action": ALL, "input": ALL, "general": True}, "n_clicks"),
    State("app-results-store", "data"),
    State("app-sim-schematic", "data"),
    prevent_initial_call=True
)
def draw_canvas_general_diagram(n_clicks, results_data, schematic_data):
    if not any(n_clicks):
        raise PreventUpdate

    triggered_id = callback_context.triggered_id
    exp_id = triggered_id["experiment"]
    action = triggered_id["action"]
    input_id = triggered_id["input"]

    schematic_data_replay = results_data[exp_id]["schematic-data"]
    result = results_data[exp_id][action][input_id]
    data = []

    max_x = -1
    for sched_id, sched_val in result.items():
        sched_data = from_json(json.loads(sched_val)).data[0]
        if max(sched_data.x) > max_x:
            max_x = max(sched_data.x)
        sched_index = list(schematic_data_replay["schedulers"]).index(sched_id)
        sched_name = schematic_data_replay["schedulers"][sched_id]["name"]
        sched_data.update({"name": f"{sched_name}<sub>({sched_index})</sub>"})
        data.append(sched_data)
    
    fig = go.Figure(data=data)

    match action:
        case "get-waiting-queue-diagrams":
            layout = {
                "title": f"<b>Number of jobs in waiting queues</b><br>{get_input_name(input_id)}",
                "title_x": 0.5,
                "xaxis": {"title": "<b>Time</b>"},
                "yaxis": {"title": "<b>Number of waiting jobs</b>"},
            }
        case "get-jobs-throughput-diagrams":
            layout = {
                "title": f"<b>Number of finished jobs per scheduler</b><br>{get_input_name(input_id)}",
                "title_x": 0.5,
                "xaxis": {"title": "<b>Time</b>"},
                "yaxis": {"title": "<b>Number of finished jobs</b>"},
            }
        case "get-unused-cores-diagrams":
            layout = {
                "title": f"<b>Cluster utilization per scheduler</b><br>{get_input_name(input_id)}",
                "title_x": 0.5,
                "xaxis": {"title": "<b>Time</b>"},
                "yaxis": {"title": "<b>Number of unused cores</b>"},
            }
        case _:
            layout = {}
    
    fig.update_layout(layout)

    xaxis_tickvals = [i * (max_x / 10) for i in range(0, 11)]
    xaxis_ticktext = [str(timedelta(seconds=i)).split('.')[0] for i in xaxis_tickvals]
    fig.update_xaxes(range=[0, max_x])
    
    fig["layout"]["xaxis"]["tickvals"] = xaxis_tickvals
    fig["layout"]["xaxis"]["ticktext"] = xaxis_ticktext

    fig["layout"]["template"] = "plotly_dark"
    return dcc.Graph(figure=fig, style={"height": "100vh"})
    

@callback(
    Output({"type": "hover-sub", "experiment": MATCH, "action": MATCH, "input": MATCH, "scheduler": MATCH}, "style"),
    Input({"type": "hover", "experiment": MATCH, "action": MATCH, "input": MATCH, "scheduler": MATCH}, "is_open"),
    prevent_initial_call=True
)
def hover_icon_display(is_open):
    return {"display": "inline-block"} if is_open else {"display": "none"}
    

@callback(
    Output("main-canvas", "children", allow_duplicate=True),
    Input({"experiment": ALL, "action": ALL, "input": ALL, "scheduler": ALL}, "n_clicks"),
    State("app-results-store", "data"),
    State("app-sim-schematic", "data"),
    prevent_initial_call=True
)
def draw_canvas_scheduler(n_clicks, results_data, schematic_data):
    
    if not any(n_clicks):
        raise PreventUpdate

    triggered_id = callback_context.triggered_id
    exp_id = triggered_id["experiment"]
    action = triggered_id["action"]
    input_id = triggered_id["input"]
    sched_id = triggered_id["scheduler"]
    
    schematic_data_replay = results_data[exp_id]["schematic-data"]
    result = results_data[exp_id][action][input_id][sched_id]
    
    match action:
        case "get-workloads":
            
            head, table_data = create_table(result)
            for row in table_data:
                for key, val in row.items():
                    if "time" in key.lower() and val != "":
                        row[key] = float(val)
            
            # Create tooltips from the other schedulers
            others = dict()
            for other_sched_id in schematic_data_replay["schedulers"].keys():
                if other_sched_id != sched_id:
                    try:
                        _, other_tdata = create_table(results_data[exp_id][action][input_id][other_sched_id])
                        others[other_sched_id] = other_tdata
                    except:
                        pass
            
            tooltip = []
            for i, row in enumerate(table_data):
                row_dict = dict()
                for key in row.keys():
                    values = []
                    for other_sched_id, other_tdata in others.items():
                        values.append(f"{schematic_data_replay['schedulers'][other_sched_id]['name']}: {other_tdata[i][key]}")
                    row_dict[key] = "".join(values)
                tooltip.append(row_dict)
            
            
            def create_columns():
                columns = []
                for head_col in head:
                    if "time" in head_col.lower():
                        columns.append({"name": head_col, "id": head_col, "type": "numeric", "format": {"specifier": ".3f"}})
                    else:
                        columns.append({"name": head_col, "id": head_col})
                return columns
            
            return dash_table.DataTable(data=table_data, 
                                        columns=create_columns(), 
                                        style_table={"height": "100vh", "overflowY": "scroll", "whiteSpace": "pre-line"},
                                        style_header={"backgroundColor": "#111111", "color": "white", "whiteSpace": "normal", "height": "auto", "fontWeight": "bold", "textAlign": "center", "position": "sticky", "top": 0},
                                        style_cell={"backgroundColor": "#141414", "color": "#bbbbbb","textAlign": "center"},
                                        style_data_conditional=[
                                            {
                                                "if": {"column_id": "Job Number"},
                                                "color": "orange"
                                            }
                                        ],
                                        # style_data={"backgroundColor": "black", "color": "white"},
                                        tooltip_data=tooltip,
                                        tooltip_delay=1000,
                                        tooltip_duration=None)
        
        case _:
            # For Plotly graphs
            data = json.loads(result)
            fig = from_json(data)
            fig["layout"]["template"] = "plotly_dark"
            fig["layout"]["plot_bgcolor"] = "#f2f2f2"
            fig.update_xaxes(linecolor="white", mirror=True, gridcolor="lightgray", gridwidth=2)
            fig.update_yaxes(linecolor="white", mirror=True, gridcolor="lightgray", gridwidth=2)
            return dcc.Graph(figure=fig, style={"height": "100vh"})