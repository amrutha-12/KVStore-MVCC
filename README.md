# KVStore-MVCC

## Overview

**KVStore-MVCC**  is a key-value store implementation with Multi-Version Concurrency Control (MVCC), written in Go. It enables thread-safe concurrent access and maintains multiple versions of stored values.

## Features

-   Multi-Version Concurrency Control (MVCC)
    
-   Thread-safe operations (read/write/delete)
    
-   In-memory storage

## Prerequisites

-   **Go 1.23**  (or newer)
    
-   Git (for cloning the repository)
    

## Setting Up Go 1.23

### Linux (Ubuntu/Debian)

1.  **Download the Go 1.23 tarball**:
    
    bash
    
    `wget https://go.dev/dl/go1.23.0.linux-amd64.tar.gz` 
    
2.  **Extract and install**:
    
    bash
    
    `sudo  rm -rf /usr/local/go &&  sudo  tar -C /usr/local -xzf go1.23.0.linux-amd64.tar.gz` 
    
3.  **Add Go to PATH**:  
    Add this line to  `~/.profile`  or  `~/.bashrc`:
    
    bash
    
    `export  PATH=$PATH:/usr/local/go/bin` 
    
    Then apply changes:
    
    bash
    
    `source ~/.profile` 
    
4.  **Verify installation**:
    
    bash
    
    `go version # Should output "go1.23.0 linux/amd64"` 
    

### macOS (Apple chip)

1.  **Download the Go 1.23 installer**:
    
    bash
    
    `curl -OL https://go.dev/dl/go1.23.0.darwin-amd64.pkg` 
    
2.  **Install via GUI**  (double-click the  `.pkg`  file) or CLI:
    
    bash
    
    `sudo installer -pkg go1.23.0.darwin-amd64.pkg -target /` 
    
3.  **Add Go to PATH**:  
    Add to  `~/.zshrc`  or  `~/.bash_profile`:
    
    bash
    
    `export  PATH=$PATH:/usr/local/go/bin` 
    
    Apply changes:
    
    bash
    
    `source ~/.zshrc` 
    
4.  **Verify installation**:
    
    bash
    
    `go version # Should output "go1.23.0 darwin/amd64"`

## Building and Running

1.  **Clone the repository**:
    
    bash
    
    `git clone https://github.com/amrutha-12/KVStore-MVCC.git cd KVStore-MVCC` 
    
2.  **Build the project**:
    
    bash
    
    `go build -o kvstore ./cmd/main.go` 
    
3.  **Run the executable**:
    
    bash
    
    `./kvstore` 
    
4.  **Run tests**:
    
    bash
    
    `go test -v ./...`

## Troubleshooting

-   **"go: command not found"**: Ensure Go is in your  `PATH`.
    
-   **Permission errors**: Use  `sudo`  for system-wide installations.
    
-   **Version conflicts**: Remove old Go installations before updating.
