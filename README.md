![ELiSE logo](./assets/promo/dark-horizontal-elise-logo-github.png)

![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54)
![Anaconda](https://img.shields.io/badge/Anaconda-%2344A833.svg?style=for-the-badge&logo=anaconda&logoColor=white)
[![Discord](https://img.shields.io/badge/Discord-%235865F2.svg?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/cABwcWhBSx)

# **Efficient Lightweight Scheduling Estimator (ELiSE)**

## Description

ELiSE is a framework for fast prototyping and evaluation of scheduling and co-scheduling algorithms in HPC systems. 
The main focus is to be a simple, fast, small and easily extensible tool. ELiSE is also flexible providing a graphical interface,
a web interface and a command line interface.

## Demo

https://github.com/user-attachments/assets/ffcaafbe-0f6c-450b-959f-066b9f82170f

## 📚 ELiSE Material

* 🎞️ Presentation
[📂 View the Slides](./material/ELiSE_%20A%20tool%20to%20support%20algorithmic%20design%20for%20HPC%20co-scheduling.pdf)


* 📝 JSSPP 2025
[📂 View the Paper](./material/JSSPP_2025_paper_11.pdf)


## 💬 Join Us on Discord
[![Join us on Discord](https://img.shields.io/badge/Join%20us%20on%20Discord-5865F2?style=for-the-badge&logo=discord&logoColor=white)](https://discord.gg/cABwcWhBSx)

Stay connected with the ELiSE:
- Join our community!
- Get notified about new commits, bug fixes, and releases
- Ask questions and get support from the community


## Usage

### GUI
**Using Python (Windows and Linux)**
```bash
conda activate elise
cd framework
python elise.py
```

**Distributable in Linux**
```bash
cd /path/to/installed/elise
./elise
```

**Distributable in Windows**

You can create a shortcut to elise.exe. Otherwise from a Command Prompt or PowerShell:

```powershell
cd \path\to\installed\elise
.\elise.exe
```

### WebUI

**Using Python (Windows and Linux)**
```bash
conda activate elise
cd framework
python elise.py --webui
```

**Distributable in Linux**
```bash
cd /path/to/installed/elise
./elise --webui
```

**Distributable in Windows**

```powershell
cd \path\to\installed\elise
.\elise.exe --webui
```

### Terminal (Batch mode)

A schematic file needs to be provided as input in order to execute ELiSE in batch mode

**Using Python (Windows and Linux)**
```bash
conda activate elise
cd framework
python elise.py -f schematic.yml -p [mp,openmpi,intelmpi] --export_reports dir
```

**Distributable in Linux**
```bash
cd /path/to/installed/elise
./elise -f schematic.yml -p [mp,openmpi,intelmpi] --export_reports dir
```

**Distributable in Windows**
```powershell
cd \path\to\installed\elise
.\elise -f schematic.yml -p [mp,openmpi,intelmpi] --export_reports dir
```
* `--export_reports <dir>` Provide a directory to export reports for each scheduler


### Distributed Execution

ELiSE distributes based on the inputs and schedulers configured for an experiment. The providers for parallelization are:
1. Python's multiprocessing library (single host)
2. Open MPI, tested on Linux (multi-host)
3. Intel MPI, tested on Windows (multi-host)

A table detailing the operating systems and MPI versions tested will be included in future updates. 
Preliminary tests suggest that with some minor adjustments, achieving compatibility should be straightforward.

## Development

In order to start developing a new algorithm or a core feature for ELiSE, the conda environment should be built.
Two yaml files are provided:
- **for Linux:** env_linx64.yml
- **for Windows:** env_win64.yml

### Setting up the environment using conda

**For Linux:**
```bash
# Install the necessary dependencies
conda env create -f env_linx64.yml

# Starting the environment
conda activate elise
```

**For Windows:**
```powershell
# Install the necessary dependencies
conda env create -f env_win64.yml

# Starting the environment
conda activate elise
```

#### Developing the interfaces

Although there are three interfaces for ELiSE, only two matter. The batch mode and WebUI.
The source code for batch mode is located in **framework/batch**, while the WebUI source code can be found in **framework/webui**.

The WebUI serves as a graphical interface for the batch mode of ELiSE, ultimately invoking it to run experiments. 
Additionally, the GUI acts as a wrapper around the WebUI and operates on the localhost.


#### Library

The main components of ELiSE consist of the loosely connected packages **api/** and**framework/realsim**:
- **api:** This package is designed for loading and editing raw data, which will be utilized to create a workload for simulation runs.
- **realsim:** This library defines key elements of a simulation, including the Compute Engine, jobs, database, cluster, scheduling algorithms, and plotting features.
