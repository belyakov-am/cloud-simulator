# Setup
This file contains information about all requirements for running code
locally and instructions for installing them.

---
**NOTE**

The simulator was developed and tested under Ubuntu 20.04 with Python 3.9.5.

---

## OS Packages
The simulator requires several OS-level packages. The list and commands for
installation can be found below.

- python3-dev
- graphviz-dev
- pip3
- pipenv

```shell
sudo apt-get install python3-dev graphviz-dev python3-pip
sudo pip3 install pipenv
```

## Python Packages
All required python packages are encapsulated into Pipfile. The command below
should be executed in root repository directory.

```shell
pipenv install
```
