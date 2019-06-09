import os
import codecs
import sys
from shutil import rmtree
from setuptools import setup, find_packages, Command


here = os.path.abspath(os.path.dirname(__file__))


with open("README.md", "r") as fh:
    long_description = fh.read()

class UploadCommand(Command):
    """Support setup.py publish."""

    description = "Build and publish the package."
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold."""
        print("\033[1m{0}\033[0m".format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status("Removing previous builds…")
            rmtree(os.path.join(here, "dist"))
        except FileNotFoundError:
            pass
        self.status("Building Source distribution…")
        os.system("{0} setup.py sdist bdist_wheel".format(sys.executable))
        self.status("Uploading the package to PyPi via Twine…")
        os.system("sudo twine upload dist/*")
        sys.exit()



setup(
    name="funtime",
    version="0.4.7",
    author="Kevin Hill",
    author_email="kevin@funguana.com",
    description="A timeseries library to make your workflow easier",
    long_description=long_description,
    long_description_content_type="text/markdown",
    py_modules=["funtime"],
    install_requires=['scipy', 'numpy', 'pandas', 'click', 'arctic', 
        'toolz', 'dask', 'cloudpickle', 'dask[complete]', 
        'dask[dataframe]', 'python-decouple', 'maya', 'crayons'
    ], 
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    cmdclass={"upload": UploadCommand},
    
)
# I accept loss, pain, conflict, misunderstandings, failure and death while also embracing everything life has to offer. 
# Maybe by embracing the bad I'm accepting the good in life too.
# I'll slash the enemy with intent to kill knowing that it's to live. 
# I'll deeply know that conveying things honestly, even when that could  produce great negatives is apart of the great experience of life. 
# I accept that not failing is not taking all of life in. 
# I accept that avoiding hatred and hurtful feelings is not allowing myself to be human.
# I accept the hard shit.
# I accept the rejection.
# I accept being humbled to hell.
# I accept harsh conditions.
# I accept ostracization.
# I accept fear.
# By accepting these I say yes to life. To love. To enjoyment.
