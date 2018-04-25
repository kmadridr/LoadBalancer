import os, json, sys

##Ejecutar un programa basandose en los parametros enviados en un archivo JSON
def run(param):
    ##Construir el comando a ejecutar
    prog = param["run_cmd"]
    comp = param["compile_cmd"]
    output = param["output"]
    if comp != '':
        os.system(comp)
    if output != '':
        prog += '>>' + output
    else:
        prog += '>> output.txt'
    #Ejecutar el comando
    os.system(prog)

def getParam(paramFile):
    data = open(paramFile, "r")
    return json.load(data)

def main():
    ##El unico argumento que deberia enviarse (ademas del programa) es el JSON
    run(getParam(sys.argv[1]))

##Si se ejecuta enviando el archivo JSON como parametro, correr el programa
if len(sys.argv) > 1:
    main()
