from setuptools import setup

setup(
   name='store_data',
   version='1.0',
   description='',
   author='Melvin Casanave',
   author_email='',
   packages=['store_data'],  #same as name
   install_requires=['polars[all]'],  #  external packages as dependencies
)
