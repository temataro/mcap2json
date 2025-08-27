# MCAP to JSON Converter Suite

A comprehensive toolkit for converting, processing, and streaming ROS2 MCAP bag files to JSON format for analysis, visualization, and real-time data streaming.

## Overview

This project provides tools to:
- **Convert MCAP files to JSON**: Extract ROS2 messages from MCAP bag files into line-delimited JSON format
- **Stream data via WebSocket**: Send JSON-formatted ROS2 data to visualization tools like PlotJuggler
- **Process compressed data**: Handle `.bz2` compressed files seamlessly
- **Decode ROS2 messages**: Parse CDR-encoded messages and IDL schemas

## Project Structure

```
mcap2json/
├── mcap2json/           # Core MCAP to JSON converter
│   ├── workspace/       # Working directory with conversion scripts
│   ├── Containerfile    # Container definition for deployment
│   ├── Makefile        # Build automation
│   └── README.md       # Converter documentation
├── plotjuggler/        # WebSocket streaming for PlotJuggler
│   ├── play_plotjuggler.py  # WebSocket streaming script
│   ├── requirements.txt     # Python dependencies
│   ├── media/          # Screenshots and documentation images
│   └── README.md       # PlotJuggler integration guide
└── README.md           # This file
```

## Components

### 1. MCAP to JSON Converter (`mcap2json/`)

The core converter that processes ROS2 MCAP bag files and outputs JSON. Features include:
- Topic filtering and listing
- IDL schema extraction
- CDR message decoding
- Batch processing of directories
- Compressed output support

[📖 Read the MCAP Converter Documentation](mcap2json/README.md)

**Quick Example:**
```bash
cd mcap2json/workspace
python mcap2json.py -m rosbag.mcap -o output.json.bz2
```

### 2. PlotJuggler WebSocket Streamer (`plotjuggler/`)

A real-time streaming tool that sends JSON data via WebSocket for visualization in PlotJuggler:
- Asynchronous WebSocket communication
- ROS2 timestamp conversion
- Topic-based data organization
- Support for secure WebSocket (WSS)

[📖 Read the PlotJuggler Integration Guide](plotjuggler/README.md)

**Quick Example:**
```bash
cd plotjuggler
./play_plotjuggler.py -v data.json -w ws://localhost:9871
```

## License

Licensed under the Apache License, Version 2.0. See the source file for full license text.
