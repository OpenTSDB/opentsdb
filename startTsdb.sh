

tsdtmp=${TMPDIR-'/tmp'}/tsd

mkdir -p "$tsdtmp" 

./build/tsdb tsd --port=4242 --staticroot=build/staticroot --cachedir="$tsdtmp"


