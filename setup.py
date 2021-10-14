#!/usr/bin/python
"""
setup in flux
"""
from setuptools import setup, find_packages

# with open("README0.md", "r") as fh:
#    long_description = fh.read()
long_description = 'this is a package of pyspark dataframe transformations, ' \
                   'constraints checks and schema and column manipulations'
    
setup(
    name="pystitchr",
    version="0.2.snapshot",
    description='Stitchr Python API',
    long_description=long_description,# long_description,
    long_description_content_type="text/markdown",
    author='Nabil Hachem',
    author_email='nabilihachem@gmail.com',
    url='https://github.com/nhachem/stitchr-python/tree/master',
    # packages=find_packages(),
    packages=['pystitchr',
              'pystitchr.base'
              ],
    include_package_data=True,
    # package_dir={
    #    'stitchr_extensions.jars': 'deps/jars',
    # },
    # package_data={
    #    'stitchr_extensions.jars': ['*.jar'],
        # "": ['*.jar'],
    #    },
    # scripts=scripts, #['stitchr_extensions'],
    license='http://www.apache.org/licenses/LICENSE-2.0',
    install_requires=[],
        extras_require={
            'base': [],
            'extensions': []
        },
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Typing :: Typed'],
)
