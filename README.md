# LTH OMOP ETL

This project aims to transform data from multiple EHR data sources into the OMOP common data standard (_bronze layer_) using SQLMesh.

## Table of Contents

- [LTH OMOP ETL](#lth-omop-etl)
  - [Table of Contents](#table-of-contents)
  - [Installation](#installation)
    - [Cloning the Repository](#cloning-the-repository)
    - [Installing uv](#installing-uv)
    - [Setting up Virtual Environment](#setting-up-virtual-environment)
  - [Using VS Code Tasks](#using-vs-code-tasks)
  - [SQLMesh UI](#sqlmesh-ui)
  - [Resources](#resources)


## Installation

### Cloning the Repository

To clone this repository, run the following command:

```sh
git clone https://github.com/LTHTR-DST/dbt_omop.git
cd dbt_omop
```

### Installing uv

To manage virtual environments, we use `uv`.
You can install `uv` by following the [official instructions](https://docs.astral.sh/uv/getting-started/installation/).
On Windows, run the following command in Powershell to install `uv`.

```sh
winget install --id=astral-sh.uv  -e
```

Logout and login again to start using it.
For more information, refer to the [uv documentation](https://docs.astral.sh/uv/).

### Setting up Virtual Environment

1. To set up a virtual environment and install all the dependencies specified in `uv.lock` or `pyproject.toml`, run the following command.
This also installs the correct version of Python (which is specified in `.python-version`)

```sh
uv sync
```

2. Activate the virtual environment:

```sh
.venv/Scripts/activate
```
## Using VS Code Tasks

We have configured VS Code tasks to streamline the development process. To use these tasks:

1. Open the Command Palette (`Ctrl+Shift+P` ).
2. Type `Tasks: Run Task` and select it.
3. Choose the desired task from the list.

These tasks are defined in the `.vscode/tasks.json` file.

## SQLMesh UI

We recommend using SQLMesh UI embedded in VS Code in `plan` mode for rapid development. There are just 2 steps to this.

1. Either run `sqlmesh ui --mode plan` within a terminal in VS Code or run the `SQLMesh UI Plan` task as described in the previous section.
2. Open the Command Palette (`Ctrl+Shift+P`), run `Simple Browser` and enter `http://127.0.0.1:8000` when promted for the URL.

## Resources

- [uv Documentation](https://pypi.org/project/uv/)
- [SQLMesh Documentation](https://sqlmesh.readthedocs.io/)
