from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="funtime",
    version="0.1",
    author="Kevin Hill",
    author_email="kevin@funguana.com",
    description="A timeseries library to make your workflow easier",
    long_description=long_description,
    long_description_content_type="text/markdown",
    py_modules=["funtime"],
    install_requires=['scipy', 'numpy', 'pandas', 'click', 'arctic', 'dask', 'dask[dataframe]', 'python-decouple', 'maya'], 
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
    
)