export PROJECT_NAME=pystitchr

## set per your  environment
export USER_ROOT=$HOME
export USER_PERSIST_ROOT=$USER_ROOT/data
## run it from the root code directory
export ROOT_DIR=`pwd`
export VERSION=0.1-SNAPSHOT
## PYTHON SOURCES assumes that the root is root of src code
export PYTHONPATH=$PYTHONPATH:$ROOT_DIR:$ROOT_DIR/pystitchr
# export PYTHONPATH=$PYTHONPATH:`pwd`
export JUPYTER_PATH=$PYTHONPATH:$JUPYTER_PATH
