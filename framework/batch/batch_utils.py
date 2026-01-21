from copy import copy
from enum import Enum
from json import loads as json_loads
from yaml import safe_load
from pathlib import Path
from pickle import load as pickle_load
import importlib.util
import inspect
import os
import sys
from typing import Any
from uuid import uuid4

# Introduce path to realsim
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "../")
))

# Introduce path to api.loader
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
))

# Get logger
from common.utils import define_logger
logger = define_logger()

from common.hierarchy import import_class_hierarchy
# Get generator hierarchy and distribution hierarchies
gen_distr_hierarchy = import_class_hierarchy(os.path.abspath(os.path.join(os.path.dirname(__file__), "../realsim/generators")))

# Get scheduler hierarchy
scheduler_hierarchy = import_class_hierarchy(os.path.abspath(os.path.join(os.path.dirname(__file__), "../realsim/scheduler")))

# LoadManager
from api.loader import LoadManager

# Database
from realsim.jobs.utils import deepcopy_list
from realsim.database import Database

# Cluster
from realsim.cluster.cluster import Cluster

# Generators
from realsim.generators.AGenerator import AbstractGenerator

# Distributions
from realsim.generators.distribution.idistribution import IDistribution

# Schedulers
from realsim.scheduler.scheduler import Scheduler

# Logger
from realsim.logger.logger import Logger

# ComputeEngine
from realsim.compengine import ComputeEngine

def import_module(path):
    mod_name = os.path.basename(path).replace(".py", "")
    spec = importlib.util.spec_from_file_location(mod_name, path)
    gen_mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = gen_mod
    spec.loader.exec_module(gen_mod)
    return spec.name


def translate_action(action: str, translate: bool = False):
    translated_actions = {
        "get-workloads": "get_workload",
        "get-gantt-diagrams": "get_gantt_representation",
        "get-waiting-queue-diagrams": "get_waiting_queue_graph",
        "get-jobs-throughput-diagrams": "get_jobs_throughput",
        "get-unused-cores-diagrams": "get_unused_cores_graph",
        "get-animated-clusters": "get_animated_cluster"
    }
    if translate:
        return translated_actions[action]
    else:
        return action

def opt_is_number(val: str):
    return val.isnumeric()

def opt_is_bool(val: str):
    values = ["true", "false", "yes", "no"]
    if val.lower() in values:
        return True

def correct_opt_val(val: str):
    if opt_is_number(val):
        return float(val)
    elif opt_is_bool(val):
        return bool(val)
    else:
        return val


class SchematicAnalysis:

    def __init__(self, schematic_path: str, webui: bool = False):

        # If it is called from WebUI the actions will be translated
        self.__webui = webui
        
        # Load the configuration file
        logger.debug(f"Opening project file: {schematic_path}")
        self.schematic_read(schematic_path)

    def schematic_read(self, schematic_path: str) -> None:
        """Read a schematic file and configure the batch creator.

        Args:
            schematic_path (str): Path to schematic file
        
        Returns:
            None
        """
        with open(schematic_path, "r") as fd:

            self.config = safe_load(fd)
            
            sanity_entries = ["inputs", "schedulers", "actions"]

            if list(filter(lambda x: x not in self.config, sanity_entries)):
                raise RuntimeError("The configuration file is not properly designed")

            self.__schematic_name = self.config.get("name", str(uuid4()))
            if self.__schematic_name == "":
                self.__schematic_name = str(uuid4())
            self.__schematic_inputs = self.config["inputs"]
            self.__schematic_schedulers = self.config["schedulers"]
            self.__schematic_actions = self.config["actions"] if "actions" in self.config else dict()

    def get_sim_configs_num(self) -> int:
        logger.debug("Calculating the total number of simulation configurations")
        inputs_num = 0
        for input in self.__schematic_inputs:
            inputs_num += 1 if "repeat" not in input else int(input["repeat"])

        return inputs_num * len(self.__schematic_schedulers)
    
    @property
    def batch_id(self):
        return self.__schematic_name
    
    def analyze_inputs(self) -> None:

        logger.debug("Begin analyzing and dividing inputs in schematic file.")

        self.__inputs = list()

        for input in self.__schematic_inputs:
            repeat = 1
            if "repeat" in input:
                repeat = int(input["repeat"])
            self.__inputs.extend([input for _ in range(repeat)])
    
    def analyze_schedulers(self) -> None:

        logger.debug("Begin analyzing schedulers in schematic file.")
        self.__schedulers = self.__schematic_schedulers
    
    def analyze_actions(self) -> None:
        """
        The structure of __actions
        actions = {
            input0 = {
                scheduler0 = [],
                scheduler1 = [],
                ..
                schedulerM = []
            },
            ..
            inputN = {
                scheduler0 = []
                scheduler1 = []
                ..
                schedulerM = []
            }
        }
        The structure of __extra_features is a list of (arg: str, val: T) tuples
        __extra_features = [(arg0, val0), (arg1, val1), ...]
        """

        logger.debug("Begin analyzing the postprocessing actions in the schematic file.")

        # Define __actions
        self.__actions = dict()
        for input_index in range(len(self.__inputs)):
            input_dict = dict()
            for sched_index in range(len(self.__schedulers)):
                input_dict.update({sched_index: []})
            self.__actions.update({input_index: input_dict})

        # Define __extra_features
        self.__extra_features: list[tuple] = list()

        for action in self.__schematic_actions:
            action_inputs = self.__schematic_actions[action]["inputs"]
            action_schedulers = self.__schematic_actions[action]["schedulers"]

            action_extra_features = [(arg, val) 
                                     for arg, val in self.__schematic_actions[action].items() 
                                     if arg not in ["inputs", "schedulers"]]

            # Simple implementation is to overwrite an argument with the latest
            # value provided in the project file
            self.__extra_features.extend(action_extra_features)

            if action_inputs == "all":
                for input_dict in self.__actions.values():
                    if action_schedulers == "all":
                        for sched_dict in input_dict.values():
                            sched_dict.append(translate_action(action, self.__webui))
                    else:
                        for sched_index in action_schedulers:
                            input_dict[sched_index].append(translate_action(action, self.__webui))
            else:
                for input_index in action_inputs:
                    if action_schedulers == "all":
                        for sched_dict in self.__actions[input_index].values():
                            sched_dict.append(translate_action(action, self.__webui))
                    else:
                        for sched_index in action_schedulers:
                            self.__actions[input_index][sched_index].append(translate_action(action, self.__webui))

    @property
    def batch(self) -> list:

        self.analyze_inputs()
        self.analyze_schedulers()
        self.analyze_actions()

        __batch = list()

        sim_idx = 0
        for inp_idx, inp in enumerate(self.__inputs):
            for sched_idx, scheduler in enumerate(self.__schedulers):
                actions = self.__actions[inp_idx][sched_idx]

                __batch.append((self.batch_id, sim_idx, inp_idx, sched_idx, inp, scheduler, actions, self.__extra_features))

                sim_idx += 1
        
        return __batch
                


class SimConfigCreator:

    def __init__(self, batch: list):

        self.batch_id,\
        self.sim_idx,\
        self.inp_idx,\
        self.sched_idx,\
        self.inp_cfg,\
        self.sched_cfg,\
        self.act_cfg,\
        self.extra_features = batch

        # Ready to use generators implementing the AbstractGenerator interface
        self.__impl_generators = {}

        # Ready to use generators implementing the IDistribution interface
        self.__impl_distributions = {}

        for gen_distr_key, gen_distr_val in gen_distr_hierarchy.items():
            # Don't import abstract classes
            if gen_distr_val['abstract']:
                continue

            # It is a module defining a distribution
            if "IDistribution" in gen_distr_val['bases']:
                distr_name = gen_distr_val['name']
                if not distr_name:
                    distr_name = gen_distr_val
                self.__impl_distributions[distr_name] = gen_distr_val['obj']
            # It is a module defining a generator
            else:
                gen_name = gen_distr_val['name']
                if not gen_name:
                    gen_name = gen_distr_val
                self.__impl_generators[gen_name] = gen_distr_val['obj']

        # Ready to use schedulers implementing the Scheduler interface
        self.__impl_schedulers = {}
        for sched_key, sched_val in scheduler_hierarchy.items():
            scheduler_name = sched_val["name"]
            if not scheduler_name:
                scheduler_name = sched_key
            self.__impl_schedulers[scheduler_name] = sched_val["obj"]
        
        # If using MPI store modules that should be exported to other MPI procs
        self.mods_export = list()

    def __get_class_from_file(self, file: str, root_cls: object, export_module: bool = False) -> object:
        # Import generator module
        spec_name = import_module(file)
        module = sys.modules[spec_name]
        # Get the generator class from the module
        classes = inspect.getmembers(module, inspect.isclass)
        # It must be a concrete class implementing the AbstractGenerator interface
        classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], root_cls), classes))

        # If there are multiple then inform the user that the first will be used
        if len(classes) > 1:
            logger.debug(f"Multiple definitions were found. Using the first definition: {classes[0][0]}")

        _, cls = classes[0]

        if export_module:
            # Export module for MPI procs
            self.mods_export.append(file)
        
        return cls

    def __get_class_from_module(
        self, 
        cls_info: str|Path,
        root_cls: object, 
        loaded_classes: dict[str, object]) -> object:

        # If it is a python file we should import the module, get the class 
        # and export the module for the distributed workers(ranks)
        cls_info = str(cls_info)
        if cls_info.endswith(".py") and os.path.exists(cls_info):
            return self.__get_class_from_file(file=cls_info, root_cls=root_cls, export_module=True)
        elif cls_info in loaded_classes:
            return loaded_classes[cls_info]
        else:
            raise RuntimeError(f"The given class name or path: {cls_info}, doesn't exist.")


    def __input_read_from_source(self, input: dict) -> LoadManager:

        # Create a LoadManager based on the options given
        machine = input.get("loads-machine", "")
        suite = input.get("loads-suite", "")
        lm = LoadManager(machine=machine, suite=suite)

        # A LoadManager instance can be created using
        if "path" in input:
            logger.debug("Reading from a directory of logs.")
            # A path to a directory with the real logs
            path = input["path"]
            lm.init_loads(runs_dir=path)
        elif "load-manager" in input:
            logger.debug("Reading from a pickled LoadManager.")
            # A pickled LoadManager instance (or json WIP)
            with open(input["load-manager"], "rb") as fd:
                lm = pickle_load(fd)
        elif "db" in input:
            logger.debug("Reading from a database.")
            # A mongo database url
            lm.import_from_db(host=input["db"], dbname="storehouse")
        elif "json" in input:
            logger.debug("Reading from a JSON file.")
            lm.import_from_json(input["json"])
        else:
            logger.debug("No valid source was given.")
            raise RuntimeError("Couldn't provide a way to create a LoadManager")
        
        return lm
    
    def __input_generate_heatmap(self, input: dict, lm: LoadManager) -> dict:

        # Create a heatmap from the LoadManager instance or use a user-defined
        # if a path is provided
        if "heatmap" in input:
            path = input["heatmap"]
            logger.debug(f"Reading heatmap from file: {path}")
            if os.path.exists(path):
                with open(input["heatmap"], "r") as fd:
                    heatmap = json_loads(fd.read())
            else:
                logger.debug(f"Heatmap file: {path} doesn't exist. Generating heatmap from LoadManager.")
                heatmap = lm.export_heatmap()
        else:
            logger.debug("No heatmap file was given. Generating heatmap from the LoadManager.")
            heatmap = lm.export_heatmap()
        
        return heatmap


    def process_input_config(self) -> tuple:

        logger.debug("Begin processing the input configuration.")

        # Read from raw data
        logger.debug("Begin reading input data.")
        try:
            lm = self.__input_read_from_source(self.inp_cfg)
        except Exception as e:
            logger.exception(e)
            lm = None
        logger.debug("Finished reading input data.")

        # Generate heatmap
        logger.debug("Begin generating the heatmap.")
        heatmap = self.__input_generate_heatmap(self.inp_cfg, lm) #TODO: check if no LoadManager is given/created
        logger.debug("Finished generating the heatmap")

        logger.debug(f"Finished calculating the heatmap: {heatmap}")

        # Create cluster
        nodes = int(self.inp_cfg["cluster"]["nodes"])
        socket_conf = tuple(self.inp_cfg["cluster"]["socket-conf"])
        logger.debug(f"Capturing cluster configuration. Nodes: {nodes}, Socket Configuration: {socket_conf}.")

        # Create the workload
        logger.debug("Begin creating the workload.")
        if "generator" in self.inp_cfg:

            logger.debug("A generator was provided.")

            generator = self.inp_cfg["generator"]
            gen_type = generator["type"]
            gen_arg = generator["arg"]

            try:
                gen_cls = self.__get_class_from_module(cls_info=gen_type, root_cls=AbstractGenerator, loaded_classes=self.__impl_generators)
            except Exception as e:
                logger.exception(e.with_traceback(None))
            
            gen_inst = gen_cls(load_manager=lm)

            logger.debug(f"Got the generator: {gen_inst.name}. Creating the initial workload.")

            gen_input = gen_inst.generate_jobs_set(gen_arg)

            logger.debug(f"Finished generating the workload.")

            # Check if a transformer distribution is provided by the user
            if "distribution" in generator:

                logger.debug("A distribution was provided.")
            
                distribution = generator["distribution"]
                distr_type = distribution["type"]
                distr_arg = distribution["arg"]

                # If a path is provided for the distribution transformer
                try:
                    distr_cls = self.__get_class_from_module(cls_info=distr_type, root_cls=IDistribution, loaded_classes=self.__impl_distributions)
                except Exception as e:
                    logger.exception(e.with_traceback())

                distr_inst = distr_cls()
                distr_inst.apply_distribution(gen_input, time_step=distr_arg)

                logger.debug(f"A distribution was applied to the input: {distr_inst.name}.")
            
            else:
                logger.debug("A distribution was not applied to the initial workload.")
            
            logger.debug("Finished creating the workload.")

            logger.debug("Returning the workload, heatmap and cluster configuration.")
            return gen_input, heatmap, nodes, socket_conf

        else:
            raise RuntimeError("A generator to create the workload was not provided.")

    def process_scheduler_config(self) -> tuple:

        logger.debug(f"Begin processing the scheduler configuration: {self.sched_cfg}.")

        base_scheduler = self.sched_cfg["base"]            

        if os.path.exists(base_scheduler) and ".py" in base_scheduler:
            spec_name = import_module(base_scheduler)
            sched_mod = sys.modules[spec_name]
            classes = inspect.getmembers(sched_mod, inspect.isclass)
            classes = list(filter(lambda it: not inspect.isabstract(it[1]) and issubclass(it[1], Scheduler), classes))
            # If there are multiple then inform the user that the first will be used
            if len(classes) > 1:
                logger.info(f"Multiple scheduler definitions were found. Using the first definition: {classes[0][0]}")

            _, sched_cls = classes[0]

            # To export modules for MPI procs
            print(base_scheduler)
            self.mods_export.append(base_scheduler)
        else:
            try:
                sched_cls = self.__impl_schedulers[base_scheduler]
            except:
                raise RuntimeError(f"Scheduler of type {base_scheduler} does not exist")
        
        # Get scheduler options
        sched_opts = [(opt, val) for opt, val in self.sched_cfg.items() if opt != "base"]

        logger.debug(f"Finished processing the scheduler configuration.")
        return sched_cls, sched_opts

    @property
    def simconfig(self) -> list:

        workload, heatmap, nodes, socket_conf = self.process_input_config()

        # Create a database instance
        database = Database(deepcopy_list(workload), heatmap)
        database.setup()

        # Create a cluster instance
        cluster = Cluster(nodes, socket_conf)

        sched_cls, sched_opts = self.process_scheduler_config()
        # Create a scheduler instance
        scheduler = sched_cls()
        # Apply options to scheduler instance
        for opt, val in sched_opts:
            scheduler.__dict__[opt] = val

        # Create a logger instance
        evt_logger = Logger(debug=False)

        # Create a compute engine instance
        compengine = ComputeEngine(database, cluster, scheduler, evt_logger)
        compengine.setup_preloaded_jobs()

        return [self.batch_id, 
                self.sim_idx, 
                self.inp_idx, 
                self.sched_idx, 
                database, 
                cluster, 
                scheduler, 
                evt_logger, 
                compengine, 
                self.act_cfg, 
                self.extra_features]
