import json
path = input("Donde se guardara: ")
finalPath = path + "/data.json"
cmd = input("Compile CMD: ")
source = input("Run SRC: ")
outputData = input("Output: ")
data = {
   'compile_cmd':cmd,
   'run_cmd':source,
   'output':outputData
   }
with open (finalPath, 'a') as outfile:
    json.dump(data, outfile)

