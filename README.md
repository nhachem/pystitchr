 # PyStitchr: (Data)Stitchr (Python Version)

## What is (Data)Stitchr? ###

DataStitchr or Stitchr is a tool that helps move and transform data along all phases of a data processing pipeline.
It can be deployed at any place in the pipeline to help in the data loading, transformation and extraction process. 

The objective is to develop an open, extensible  and "simple-to-use" tool for data engineers.
The original tol is scala-based and is found at [(Data)Stitchr](https://github.com/nhachem/stitchr)

We initiated a new version PyStitchr that is intended to be pure python/pyspark based and started with basic pipeline processing capabilities on single datasets. The objective is to develop a library wrapping pyspark's capabilities and API and use this library to enable simple as well as elaborate single dataset transformations. 
In subsequent phases we would extend and port the full scala-based Stitchr functionality to Python/pyspark.

Stitchr (and PyStitchr) is built as an application based and using [Apache Spark](https://spark.apache.org/ "Spark").

## What Features do we currently support
You can build the basic docs by running `./bash/gen_docs.sh` from the top directory. 

A copy is generated for now and kept under the `html` directory
see docs under the html directory

## How to setup and demo the tool? ###

* Build

    First clone the repo
    ``` git clone https://github.com/nhachem/stitchr-python.git``` under for example  `....repo/`. The ```STITCHR_ROOT``` is ```...repo/stitchr``` and  is the root of the code under stitchr. All environment variables for  stitchr are defined in ``` $STITCHR_ROOT/bash/stitchr_env.sh ```

    First time build
    ```
    source bash/stitchr_env.sh
   ```
    then run
    ```
    python setup.py bdist_wheel
    ```
    This step is needed once for a new build environment. You can then deploy the dist/*.whl, for example to install locally
```python -m pip install dist/stitchr-0.1-py2.py3-none-any.whl```

  and to uninstall
```pip uninstall stitchr```
  
* Configuration

    `source $STITCHR_ROOT/bash/stitchr_env.sh` 
  
* Dependencies
   
        needs pyspark 3+. Although the system has been tested to work on Spark 2.2.3.

* How to run the demo

```
source bash/pystitchr_env.sh
cd scripts
python demoPipelineRun.py
or 
python demoPipelineRunFromFile.py
```


## Contribution guidelines ###

* Known Issues


* Pending Features and Fixes
    
    * Porting from the scala code to python
* General (WIP)
    * Writing unit tests
    * Code review
    * Other guidelines?!
    
### Send requests/comments  to ###
    
Repo owner/admin: Nabil Hachem (nabilihachem@gmail.com)

## Trademarks

Apache®, Apache Spark are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
See guidance on use of Apache Spark trademarks. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

Copyright ©2019 The Apache Software Foundation. All rights reserved.
