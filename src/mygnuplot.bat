set -e
stdout=$1
shift
stderr=$1
shift
gnuplot.exe %1 2>&1
