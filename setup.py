import setuptools

with open("README.rst", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pycaz",
    version="0.0.1",
    author="Javier M. Mellid",
    author_email="jmunhoz@igalia.com",
    description="pycaz package",
    long_description=long_description,
    url="https://www.igalia.com",
    packages=setuptools.find_packages(),
    install_requires = ['dateutils'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
