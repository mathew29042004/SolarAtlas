# Source this: keeps every tool and cache inside the project, no telemetry.
export SA_HOME="$(cd "$(dirname "${BASH_SOURCE[0]:-${(%):-%x}}")" && pwd)"
export PATH="$SA_HOME/.tools/bin:$SA_HOME/.venv/bin:$PATH"
export UV_CACHE_DIR="$SA_HOME/.cache/uv"
export UV_PYTHON_INSTALL_DIR="$SA_HOME/.tools/python"
export PIP_CACHE_DIR="$SA_HOME/.cache/pip"
export XDG_CACHE_HOME="$SA_HOME/.cache"
export IVY_HOME="$SA_HOME/.cache/ivy"
export SPARK_LOCAL_DIRS="$SA_HOME/.cache/spark"
export DO_NOT_TRACK=1 UV_NO_TELEMETRY=1 PYSPARK_PIN_THREAD=true
if [ -d "$SA_HOME/.tools/jdk/Contents/Home" ]; then
  export JAVA_HOME="$SA_HOME/.tools/jdk/Contents/Home"; export PATH="$JAVA_HOME/bin:$PATH"
fi
