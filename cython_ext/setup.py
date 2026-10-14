from Cython.Build import cythonize
from setuptools import setup

setup(ext_modules=cythonize("sparkhttp_cy.pyx", language_level=3))
