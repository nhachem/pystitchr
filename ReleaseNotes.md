 # PyStitchr: (Data)Stitchr (Python Version)

## Release 1

### Major changes 
* refactoring run_pipeline and changed the way transforms are specified. See the new version of demoPipelineRun
    * invocation now accepts **params as well as parameter list as a dict
  ``` 
  df_p = (
            df_p.transform(lambda df: method_to_call(df, params))
            if isinstance(params, list)
            else df_p.transform(method_to_call, **params)
        )
  ```
### Shortcomings
* need more unit tests to cover the df_transform dynamic_module
* documentations of API is very light

### Known Issues
* tests are bundled in the wheel

### Send requests/comments  to ###
    
Repo owner/admin: Nabil Hachem (nabilihachem@gmail.com)

## Trademarks

Apache®, Apache Spark are either registered trademarks or trademarks of the Apache Software Foundation in the United States and/or other countries.
See guidance on use of Apache Spark trademarks. All other marks mentioned may be trademarks or registered trademarks of their respective owners.

Copyright ©2019 The Apache Software Foundation. All rights reserved.
