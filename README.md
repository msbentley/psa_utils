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

The URL for the PSA can be specified when instantiating the tap class. If none is given, a default URL is used, which corresponds to the default operational server.



## Example

The Jupyter notebook included with this repository shows examples of each function and module. Click [here](https://nbviewer.jupyter.org/github/msbentley/psa_utils/blob/main/psa_utils_demo.ipynb) to access the notebook!
