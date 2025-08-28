#!/usr/bin/env python3

################################################################################
#                                                                              #
#  JSON to WebSocket Sender for ROS2-bags                                      #
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
import asyncio
import websockets
from websockets.exceptions import WebSocketException
from typing import Optional
from urllib.parse import urlparse

def validate_websocket_url(ws_url: str) -> str:
    """
    Validate and normalize WebSocket URL.

    Args:
        ws_url: WebSocket URL (e.g., 'ws://localhost:9871' or 'wss://example.com/path')

    Returns:
        Validated WebSocket URL
    """
    # Add default ws:// scheme if not present
    if not ws_url.startswith(('ws://', 'wss://')):
        ws_url = f'ws://{ws_url}'

    try:
        parsed = urlparse(ws_url)
        if parsed.scheme not in ['ws', 'wss']:
            raise ValueError(f"Invalid WebSocket scheme: {parsed.scheme}")
        if not parsed.netloc:
            raise ValueError("Missing host in WebSocket URL")
        return ws_url
    except Exception as e:
        print(f"Error: Invalid WebSocket URL format: {ws_url}", file=sys.stderr)
        print(f"Expected format: 'ws://host:port/path' or 'wss://host:port/path'", file=sys.stderr)
        print(f"Error details: {e}", file=sys.stderr)
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

async def process_json_to_websocket(input_file: str, ws_url: Optional[str] = None) -> None:
    """
    Process JSON file line by line and send each message via WebSocket.

    Args:
        input_file: Path to input JSON file (can be .bz2 compressed)
        ws_url: WebSocket URL for output (e.g., 'ws://localhost:8080')
    """
    websocket_conn = None
    packets_sent = 0
    line_number = 0

    try:
        # Connect to WebSocket if URL provided
        if ws_url:
            ws_url = validate_websocket_url(ws_url)
            try:
                websocket_conn = await websockets.connect(ws_url)
                print(f"Connected to WebSocket at {ws_url}", file=sys.stderr)
            except Exception as e:
                print(f"Error connecting to WebSocket: {e}", file=sys.stderr)
                sys.exit(1)

        # Open input file with bz2 compression if needed
        if input_file.endswith('.bz2'):
            f = bz2.open(input_file, 'rt', encoding='utf-8')
        else:
            f = open(input_file, 'r', encoding='utf-8')

        with f:
            for line in f:
                line_number += 1
                line = line.strip()
                if not line:
                    continue

                try:
                    # Parse JSON object
                    json_obj = json.loads(line)

                    # Check whether its ROS or raw JSON
                    if all(key in json_obj for key in ['timestamp','data']):
                        json_obj['timestamp'] /= 1e9
                        json_obj['data'] = nested_obj_from_path(json_obj['topic'],json_obj['data'])

                    # Send via WebSocket or print to stdout
                    if websocket_conn:
                        # Convert back to compact JSON for transmission
                        await websocket_conn.send(json.dumps(json_obj))
                        packets_sent += 1
                    else:
                        # Print to stdout if no WebSocket connection
                        print(json.dumps(json_obj))

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON on line {line_number}: {e}", file=sys.stderr)
                    print(f"Content: {line[:100]}...", file=sys.stderr)
                    continue
                except websockets.exceptions.WebSocketException as e:
                    print(f"Error sending WebSocket message on line {line_number}: {e}", file=sys.stderr)
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
        if websocket_conn:
            await websocket_conn.close()

    if ws_url and packets_sent > 0:
        print(f"\nTotal WebSocket messages sent: {packets_sent}", file=sys.stderr)
    elif line_number == 0:
        print("Warning: No valid JSON objects found in input file.", file=sys.stderr)


def main() -> None:
    """Main entry point for the JSON to WebSocket sender."""
    parser = argparse.ArgumentParser(
        description='Send JSON objects (one per line) via WebSocket',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Add command line arguments
    parser.add_argument('-v', '--view', metavar='LOGFILE', type=str, required=True,
                        help='Input JSON log file to view (mandatory, supports .bz2)')
    parser.add_argument('-w', '--websocket', type=str, required=False, default='ws://localhost:9871',
                        help='WebSocket URL for sending JSON messages. '
                             'Format: ws://host:port/path or wss://host:port/path '
                             '(example: \'ws://localhost:9871\' or \'wss://example.com/data\')')

    args = parser.parse_args()

    # Run the async function
    asyncio.run(process_json_to_websocket(args.view, args.websocket))


if __name__ == '__main__':
    main()