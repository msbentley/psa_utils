# psa_utils
A python package for working with the ESA Planetary Science Archive. The following sub-modules are available:
### download
Provides functions to help with downloading public data products using either PDAP or TAP

### packager
Provides a class to package PDS4 products for delivery to the PSA

### geogen
Provides useful utilities for working with the geogen geometry generator package

### tap
A wrapper of the astropy tap class with some convenience functions and useful queries

### pdap
A minimal wrapper of the PDAP API using the requests library

### common
Common functions used across the package

### internal
Anything contained here is designed for PSA internal use.


## Dependencies

The following dependencies must be met:
- python >=3.6
- matplotlib
- numpy
- astropy
- pandas
- pyyaml
- requests

## Installation

### pip

```pip install psa_utils```

should do the job, although creating a dedicated environment is recommended (see below).

### conda

First, clone this repository. If you are using conda, the dependencies can be installed in a new environment using the provided environment file:

```conda env create -f environment.yml```

The newly created environment can be activated with:

```conda activate psa_utils```

Otherwise, please make sure the dependencies are installed with your system package manager, or a tool like `pip`. Use of a conda environment or virtualenv is recommended!

The package can then be installed with:

```python setup.py install```


## URL

The URL for the PSA can be specified when instantiating the tap. class. If none is given, a default URL is used, which corresponds to the default operational server.

## Authentication

Access to BOA needs authentication. This is controlled by a config file which can be pointed to by the `config_file` parameter when instantiating the Must class, for example:

```python
boa = psa_utils.BOA_tap(config_file='/path/to/a/config_file.yml')
```

If nothing is specified, a file `mustlink.yml` is looked for in paths pointed to by the environment variables `APPDATA`, `XDG_CONFIG_HOME` or in the `.config` folder in the user's home directory.

The configuration file should be in YAML format and contain the username and password as follows:

```yaml
user:
    login: "userone"
    password: "blah"
```

## Example

The Jupyter notebook included with this repository shows an example of how to use the code. Note that not all API functions are wrapped by this library, but only those that are commonly used. To view the notebook, click FIXME!!!!!
