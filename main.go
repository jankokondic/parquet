package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"root/gen-go/parquet"

	"github.com/apache/thrift/lib/go/thrift"
)

func ReadParquetFooter(path string) (*parquet.FileMetaData, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}
	defer f.Close()

	// Stat to get size
	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := stat.Size()

	// Footer = [metadata][4 bytes len][4 bytes "PAR1"]
	// Read last 8 bytes
	tail := make([]byte, 8)
	_, err = f.ReadAt(tail, fileSize-8)
	if err != nil {
		return nil, fmt.Errorf("failed reading footer length: %w", err)
	}

	footerLen := int64(binary.LittleEndian.Uint32(tail[:4]))
	magic := string(tail[4:8])
	if magic != "PAR1" {
		return nil, fmt.Errorf("invalid parquet magic bytes: %s", magic)
	}

	// Load footer metadata bytes
	footerStart := fileSize - 8 - footerLen
	footer := make([]byte, footerLen)

	_, err = f.ReadAt(footer, footerStart)
	if err != nil {
		return nil, fmt.Errorf("failed reading footer: %w", err)
	}

	// Decode using Thrift Compact Protocol
	mem := thrift.NewTMemoryBuffer()
	_, err = mem.Write(footer)
	if err != nil {
		return nil, fmt.Errorf("failed to write to TMemoryBuffer: %w", err)
	}

	conf := &thrift.TConfiguration{}
	protocol := thrift.NewTCompactProtocolFactoryConf(conf).GetProtocol(mem)

	meta := parquet.NewFileMetaData()
	err = meta.Read(context.Background(), protocol)
	if err != nil {
		return nil, fmt.Errorf("failed to decode thrift footer: %w", err)
	}

	return meta, nil
}

func main() {
	ReadParquetFooter("./flights-1m.parquet")
}
