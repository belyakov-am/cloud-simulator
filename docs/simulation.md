# Simulations
This file contains information about running simulations locally and 
configuration parameters.

## Code Execution
For running a simulation one should activate pipenv shell and run simulation
file from source directory.

```shell
pipenv shell
cd src
PYTHONPATH=$PYTHONPATH:$HOME/path-to-repository/src/ python simulation/main.py
```

## Configuration Parameters
Basically, there are two places for code configuration. The first one is 
[simulation code itself](../src/simulation/main.py). At the beginning of the 
`main` function one can choose a load type, configure the list of possible
recipes for workflow generation and choose, which schedulers to test.

The second place is [config](../src/simulation/config.py). There are several
parameters to configure and all explanations are presented in the comments. 
