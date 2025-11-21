package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"root/gen-go/parquet"

	"github.com/apache/thrift/lib/go/thrift"
)

func ReadParquetFooter(f *os.File) (*parquet.FileMetaData, error) {
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

func ReadPageHeader(f *os.File, offset int64) (*parquet.PageHeader, error) {
	// Seek to the beginning of the page header
	_, err := f.Seek(offset, 0)
	if err != nil {
		return nil, fmt.Errorf("seek failed: %w", err)
	}

	// Create a reader that wraps the file
	transport := thrift.NewStreamTransportR(f)
	protocol := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(transport)

	header := parquet.NewPageHeader()
	err = header.Read(context.Background(), protocol)
	if err != nil {
		return nil, fmt.Errorf("failed reading page header: %w", err)
	}

	return header, nil
}

func main() {
	f, err := os.Open("./flights-1m.parquet")
	if err != nil {
		fmt.Printf("failed to open parquet file: %e", err)
	}
	defer f.Close()

	meta, err := ReadParquetFooter(f)
	if err != nil {
		fmt.Printf("failed to read parquet file: %e", err)
	}

	//stats
	for i, rg := range meta.RowGroups {
		for j, col := range rg.Columns {
			dpo := col.MetaData.DataPageOffset
			fmt.Printf("\nRowGroup %d, Column %d:\n", i, j)
			fmt.Println("  DataPageOffset:", dpo)

			header, err := ReadPageHeader(f, dpo)
			if err != nil {
				fmt.Printf("  FAILED reading page header: %v\n", err)
				continue
			}

			fmt.Printf("  Page type: %v\n", header.Type)
			// fmt.Printf("  Uncompressed size: %v\n", header.Uncompressed_page_size)
			// fmt.Printf("  Compressed size: %v\n", header.Compressed_page_size)
		}
	}
}
