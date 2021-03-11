from setuptools import setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(name='psa_utils',
    version='0.1',
    author='Mark S. Bentley',
    author_email='mark@lunartech.org',
    description='Utilities to work with the the Planetary Science Archive',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/msbentley/psa_utils",
    download_url = 'https://github.com/msbentley/psa_utils/archive/0.1.tar.gz',
    install_requires=['pandas','pyyaml','astropy','numpy','matplotlib','requests', 'astroquery', 'pds4_utils'],
    python_requires='>=3.6',
    keywords = ['PSA','planetary science','archive','pds4'],
    packages=['psa_utils'],
    zip_safe=False)
