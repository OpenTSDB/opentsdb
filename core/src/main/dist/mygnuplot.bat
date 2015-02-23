set -e
stdout=$1
shift
stderr=$1
shift
gnuplot %1 2>&1
