import threading, socket, os, sys

balancerIp = sys.argv[1]
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((balancerIp,8080))

def listen():
    while True:
        msg = s.recv(1024).decode('ascii')
        #Los mensajes del balancer al shell son dos posibles: status (regresa el status del programa) y 
        #get (el comando para regresar un trabajo)
        msg = msg.split(' ')
        if msg[0] == 'finish':
            #Ejecutar el comando para recuperar el output del archivo
            getOutput(msg[1])
        elif msg[0] == 'status':
            if msg[2] == '-1':
                print('Job',msg[1],'not found')
            elif msg[2] == '-2':
                print('Job',msg[1],"hasn't been assigned a node")
            else:
                print('Job',msg[1],'currently assigned to node',msg[2])

    
def write():
    while True:
        msg = input('> ')
        if msg.split(' ')[0] == 'submit':
            #primero se debe enviar el proyecto
            sendJob(msg.split(' ')[1])
            s.send(msg.encode('ascii'))
        elif msg.split(' ')[0] == 'check':
            s.send(msg.encode('ascii'))
        elif msg.split(' ')[0] == 'kill':
            s.send(msg.encode('ascii'))
            print('Kill signal sent')
        else:
            print('Unkwown command')

def sendJob(path):
    #cmd = 'ssh pi@' + balancerIp + " 'mkdir -p loadbalancer/jobs/" + path + "'; " + 'scp -r ' + path + '/* pi@' + balancerIp + ':loadbalancer/jobs/' + path
    cmd = "mkdir -p //home/ludanortmun/repos/loadbalancer/jobs/" + path + "; cp -r " + path + "/* //home/ludanortmun/repos/loadbalancer/jobs/" + path
    print(cmd)
    os.system(cmd)

def getOutput(path):
    #Copiar el trabajo del balancer al cliente
    cmd = "cp -r //home/ludanortmun/repos/loadbalancer/jobs/" + path + "/* " + path + "; rm -r //home/ludanortmun/repos/loadbalancer/jobs/" + path
    print(cmd)
    os.system(cmd)
    
t = threading.Thread(target=listen)
t.start()
write()