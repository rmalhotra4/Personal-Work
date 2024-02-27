#!/usr/bin/env python
import paramiko
import os 
import getpass
import argparse
import sys
import socket 

def usage():
    print("------Sample Usage------\n\npython firewallcheck.py -s 'Host file with a list of src hosts' -d 'Host file with a list of dst hosts' -p ports")

if len(sys.argv) == 1:
    usage()
    sys.exit()
else:   
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--srcfile", help = "Provide host file with a list of source hosts/IPs", type=open, required=True)
    parser.add_argument("-d", "--dstfile", help = "Provide host file with a list of destination hosts/IPs", type=open, required=True)
    parser.add_argument("-p", "--ports", help = "Provide list of TCP ports", required=True)
    parser.add_argument("-c", "--compact", help="Use this argument to display results in parsable format", action='store_true')
    args = parser.parse_args() 

srclist = [line.strip() for line in args.srcfile.readlines()]
dstlist = [line.strip() for line in args.dstfile.readlines()]
portlist = args.ports.split()
sshport = 22
username = getpass.getuser()
password = getpass.getpass()
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

def get_status(output):
    lines = output.split('\n')
    if len(lines) > 1:
        return lines[-1].strip()  # Get the last line as the return code
    else:
        return "Unknown Status"

for srchost in srclist:
    for dsthost in dstlist:
        statuses = []  # Create a list to store statuses for each port
        try:
            for port in portlist:
                client.connect(srchost, sshport, username, password, banner_timeout=200)
                command = "timeout 5 bash -c '</dev/tcp/"+dsthost+"/"+port+"'; echo $?"
                stdin, stdout, stderr = client.exec_command(command, get_pty=True)
                output = str(stdout.read()).strip()
                status = get_status(output)
                if any(i in output for i in {"Connection refused"}):
                    status = "REFUSED"
                elif any(i in output for i in {"Name or service not known"}):
                    status = "Does not resolve"
                statuses.append((port, status))  # Append the status to the list
        except socket.gaierror:
            statuses.append(("N/A", f"{port} is not reachable"))
        except paramiko.AuthenticationException as error:
            statuses.append(("N/A", "Authentication Error. Please check username and password"))
        finally:
            client.close()
        
        if args.compact:
            for port, status in statuses:
                print(srchost + "#" + dsthost + "#" + port + "#" + status)
        else:
            for port, status in statuses:
                print(f"Source host {srchost} Dest host {dsthost} Port {port} - {status}")