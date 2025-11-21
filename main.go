package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"root/gen-go/parquet"

	"github.com/apache/thrift/lib/go/thrift"
)

// Čita footer i dekodira FileMetaData
func ReadParquetFooter(f *os.File) (*parquet.FileMetaData, error) {
	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}
	fileSize := stat.Size()

	// Parquet footer = [metadata][4 bytes len][4 bytes magic "PAR1"]
	tail := make([]byte, 8)
	_, err = f.ReadAt(tail, fileSize-8)
	if err != nil {
		return nil, fmt.Errorf("failed reading footer length: %w", err)
	}

	footerLen := int64(binary.LittleEndian.Uint32(tail[:4]))
	magic := string(tail[4:8])
	if magic != "PAR1" {
		return nil, fmt.Errorf("invalid magic bytes: %s", magic)
	}

	footerStart := fileSize - 8 - footerLen
	footer := make([]byte, footerLen)
	_, err = f.ReadAt(footer, footerStart)
	if err != nil {
		return nil, fmt.Errorf("failed reading footer: %w", err)
	}

	mem := thrift.NewTMemoryBuffer()
	_, err = mem.Write(footer)
	if err != nil {
		return nil, fmt.Errorf("failed to write footer to memory buffer: %w", err)
	}

	protocol := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(mem)
	meta := parquet.NewFileMetaData()
	if err := meta.Read(context.Background(), protocol); err != nil {
		return nil, fmt.Errorf("failed to decode thrift footer: %w", err)
	}

	return meta, nil
}

func ReadPageHeaderAndDataOffset(f *os.File, offset int64) (*parquet.PageHeader, int64, error) {
	// Postavi file offset na početak page-a
	_, err := f.Seek(offset, 0)
	if err != nil {
		return nil, 0, fmt.Errorf("seek failed: %w", err)
	}

	// Transport direktno nad fajlom
	transport := thrift.NewStreamTransportR(f)
	protocol := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(transport)

	header := parquet.NewPageHeader()
	if err := header.Read(context.Background(), protocol); err != nil {
		return nil, 0, fmt.Errorf("failed reading page header: %w", err)
	}

	// Transport je direktno nad fajlom → tell(f) = offset page data
	pageDataOffset, err := f.Seek(0, 1) // vrati trenutni offset
	if err != nil {
		return nil, 0, fmt.Errorf("failed getting file offset: %w", err)
	}

	return header, pageDataOffset, nil
}

func main() {
	f, err := os.Open("./flights-1m.parquet")
	if err != nil {
		panic(fmt.Errorf("failed to open parquet file: %w", err))
	}
	defer f.Close()

	meta, err := ReadParquetFooter(f)
	if err != nil {
		panic(fmt.Errorf("failed to read parquet footer: %w", err))
	}

	for _, rg := range meta.RowGroups {
		for _, col := range rg.Columns {
			offset := col.MetaData.DataPageOffset
			end := col.FileOffset + col.MetaData.TotalCompressedSize

			for offset < end {
				header, pageDataOffset, err := ReadPageHeaderAndDataOffset(f, offset)
				if err != nil {
					fmt.Println("Failed reading page header:", err)
					break
				}

				fmt.Printf("Page at offset %d, type=%v, compressed=%d, uncompressed=%d\n",
					pageDataOffset, header.Type, header.CompressedPageSize, header.UncompressedPageSize)

				// Čitaj page data direktno
				pageData := make([]byte, header.CompressedPageSize)
				_, err = f.ReadAt(pageData, pageDataOffset)
				if err != nil {
					fmt.Println("Failed reading page data:", err)
					break
				}

				//dekompresovati sad treba te podatke

				// Sledeći page offset
				offset = pageDataOffset + int64(header.CompressedPageSize)
			}
		}
	}
}
