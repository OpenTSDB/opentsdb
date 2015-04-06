
import os
OPENTSDB_SRC="/home/ospreyteam/OD_DEV/ods/3rdparty/opentsdb/src/"
f = file(os.path.join(OPENTSDB_SRC, "hbase-splitsfile.txt"), "w")		

indices = range(1, 11)
indices.extend(range(11, 111, 5))
indices.extend(range(111, 210, 10))
indices.extend(range(211, 2210, 200))


split_n = 2
for x in indices:
    #f.write('%0*x%s\n' % (6, x, "0"*20))
    #f.write('%0*x\n' % (6, x))
    #f.write('0x%0*x\n' % (8, x))
    hex_string = '%0*x' % (6, x)
    formatted_string = ""
    for i in range(0, len(hex_string), 2):
        a_slice = hex_string[i:i+2] 
        if a_slice: formatted_string += "\\x" + a_slice
    f.write(formatted_string+"\n")
f.close()

    
