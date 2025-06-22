#!/usr/bin/env python3

################################################################################
#                                                                              #
#  JSON to UDP Packet Sender for ROS2-bags                                     #
#                                                                              #
#  Copyright (c) 2024 Milosch Meriac <milosch@meriac.com>                      #
#                                                                              #
#  Licensed under the Apache License, Version 2.0 (the "License");             #
#  you may not use this file except in compliance with the License.            #
#  You may obtain a copy of the License at                                     #
#                                                                              #
#      http://www.apache.org/licenses/LICENSE-2.0                              #
#                                                                              #
#  Unless required by applicable law or agreed to in writing, software         #
#  distributed under the License is distributed on an "AS IS" BASIS,           #
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.    #
#  See the License for the specific language governing permissions and         #
#  limitations under the License.                                              #
#                                                                              #
#  This file was written with the assistance of Claude.ai                      #
#                                                                              #
################################################################################

import argparse
import json
import sys
import base64
import bz2
import os
import glob
import socket
from typing import Optional, Tuple
from pprint import pprint

def parse_ip_port(ipdest: str) -> Tuple[str, int]:
    """
    Parse IP address and port from string format 'IP:PORT'.
    
    Args:
        ipdest: IP destination in format 'IP:PORT'
        
    Returns:
        Tuple of (ip_address, port)
    """
    try:
        if ':' in ipdest:
            ip, port_str = ipdest.rsplit(':', 1)
            port = int(port_str)
        else:
            # Default port if not specified
            ip = ipdest
            port = 8080
            
        # Basic IP validation
        socket.inet_aton(ip)
        
        if not 1 <= port <= 65535:
            raise ValueError(f"Port {port} is out of valid range (1-65535)")
            
        return ip, port
    except (ValueError, socket.error) as e:
        print(f"Error: Invalid IP address or port format: {ipdest}", file=sys.stderr)
        print(f"Expected format: 'IP:PORT' (e.g., '127.0.0.1:8080')", file=sys.stderr)
        sys.exit(1)

def nested_obj_from_path(path, obj):

    # Turn string path into array
    path = path.strip('/').split('/')

    # Return for empty paths
    depth = len(path)-1
    if depth<0:
        return obj;

    # Create nested object
    result = {}
    current = result

    # Nest until hitting leaf level and place obj there
    for index, part in enumerate(path):
        current[part] = {} if index < depth else obj
        current = current[part]

    return result

def process_json_to_udp(input_file: str, ipdest: Optional[str] = None) -> None:
    """
    Process JSON file line by line and send each as UDP packet.

    Args:
        input_file: Path to input JSON file (can be .bz2 compressed)
        ipdest: IP destination in format 'IP:PORT' for UDP output
    """
    # Setup UDP socket if IP destination provided
    sock = None
    udp_dest = None
    
    if ipdest:
        ip, port = parse_ip_port(ipdest)
        udp_dest = (ip, port)
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            print(f"Sending UDP packets to {ip}:{port}", file=sys.stderr)
        except socket.error as e:
            print(f"Error creating UDP socket: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        # Open input file with bz2 compression if needed
        if input_file.endswith('.bz2'):
            f = bz2.open(input_file, 'rt', encoding='utf-8')
        else:
            f = open(input_file, 'r', encoding='utf-8')
        
        with f:
            line_number = 0
            packets_sent = 0
            
            for line in f:
                line_number += 1
                line = line.strip()
                if not line:
                    continue

                try:
                    # Parse JSON object
                    json_obj = json.loads(line)
                    json_obj['timestamp'] /= 1e9
                    json_obj['data'] = nested_obj_from_path(json_obj['topic'],json_obj['data'])
                    
                    # Send via UDP or print to stdout
                    if sock and udp_dest:
                        # Convert back to compact JSON for transmission
                        sock.sendto(json.dumps(json_obj).encode('utf-8'), udp_dest)
                        packets_sent += 1

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON on line {line_number}: {e}", file=sys.stderr)
                    print(f"Content: {line[:100]}...", file=sys.stderr)
                    continue
                except socket.error as e:
                    print(f"Error sending UDP packet on line {line_number}: {e}", file=sys.stderr)
                    continue
                except Exception as e:
                    print(f"Unexpected error on line {line_number}: {e}", file=sys.stderr)
                    continue

    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except IOError as e:
        print(f"Error reading input file: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if sock:
            sock.close()
    
    if ipdest and packets_sent > 0:
        print(f"\nTotal UDP packets sent: {packets_sent}", file=sys.stderr)
    elif line_number == 0:
        print("Warning: No valid JSON objects found in input file.", file=sys.stderr)


def main() -> None:
    """Main entry point for the JSON to UDP packet sender."""
    parser = argparse.ArgumentParser(
        description='Send JSON objects (one per line) as UDP packets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Add command line arguments
    parser.add_argument('-v', '--view', metavar='LOGFILE', type=str, required=True, 
                        help='Input JSON log file to view (mandatory, supports .bz2)')
    parser.add_argument('-i', '--ipdest', type=str, required=False, default='127.0.0.1:9870', 
                        help='Output JSON over UDP network instead of stdout. '
                             'Send UDP packets to the specified <IPDEST>. '
                             'Specify the port with a \':\' and a number following '
                             'the IP address (example: \'127.0.0.1:8080\').')

    args = parser.parse_args()

    # Process the JSON to UDP
    process_json_to_udp(args.view, args.ipdest)


if __name__ == '__main__':
    main()