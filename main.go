package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
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

// ReadPageHeaderAndDataOffset čita PageHeader sa datog offseta i vraća:
// - header: dekodirani PageHeader
// - pageDataOffset: offset na početak kompresovanih podataka stranice
func ReadPageHeaderAndDataOffset(f *os.File, offset int64) (*parquet.PageHeader, int64, error) {
	const maxHeaderSize = 64 * 1024 // 64 KB, dovoljno za PageHeader

	headerBuf := make([]byte, maxHeaderSize)
	n, err := f.ReadAt(headerBuf, offset)
	if err != nil && err != io.EOF {
		return nil, 0, fmt.Errorf("failed reading potential page header bytes: %w", err)
	}
	if n == 0 {
		return nil, 0, fmt.Errorf("no bytes read for page header at offset %d", offset)
	}

	mem := thrift.NewTMemoryBufferLen(n)
	_, err = mem.Write(headerBuf[:n])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to write header bytes to memory buffer: %w", err)
	}

	beforeLen := mem.Len()

	protocol := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(mem)
	header := parquet.NewPageHeader()
	if err := header.Read(context.Background(), protocol); err != nil {
		return nil, 0, fmt.Errorf("failed reading page header: %w", err)
	}

	afterLen := mem.Len()
	consumed := int64(beforeLen - afterLen)
	if consumed <= 0 {
		return nil, 0, fmt.Errorf("invalid consumed header length %d at offset %d", consumed, offset)
	}

	pageDataOffset := offset + consumed
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

	for rgIdx, rg := range meta.RowGroups {
		fmt.Println("RowGroup:", rgIdx)

		for colIdx, col := range rg.Columns {
			md := col.MetaData
			if md == nil {
				fmt.Println("  Column", colIdx, "has nil MetaData, skipping")
				continue
			}

			decoder := md.Codec
			dataPageOffset := md.DataPageOffset
			numValuesTotal := md.NumValues

			fmt.Println("  Column:", colIdx,
				"decoder:", decoder,
				"dataPageOffset:", dataPageOffset,
				"numValues:", numValuesTotal)

			offset := dataPageOffset
			var valuesRead int64
			pageIdx := 0

			// Glavna razlika: petlja ide dok ne pročitaš sve vrijednosti (NumValues),
			// a NE dok offset < dataPageOffset + totalCompressedSize.
			for valuesRead < numValuesTotal {
				header, pageDataOffset, err := ReadPageHeaderAndDataOffset(f, offset)
				if err != nil {
					fmt.Println("    Failed reading page header at", offset, ":", err)
					break
				}

				fmt.Printf("    Page %d: headerOffset=%d, dataOffset=%d, type=%v, compressed=%d, uncompressed=%d\n",
					pageIdx, offset, pageDataOffset, header.Type, header.CompressedPageSize, header.UncompressedPageSize)

				// Ažuriraj brojač vrijednosti samo za data page-ove
				switch header.Type {
				case parquet.PageType_DATA_PAGE:
					if header.DataPageHeader == nil {
						fmt.Println("      DATA_PAGE but DataPageHeader is nil")
						return
					}
					valuesRead += int64(header.DataPageHeader.NumValues)

				case parquet.PageType_DATA_PAGE_V2:
					if header.DataPageHeaderV2 == nil {
						fmt.Println("      DATA_PAGE_V2 but DataPageHeaderV2 is nil")
						return
					}
					valuesRead += int64(header.DataPageHeaderV2.NumValues)

				default:
					// DICTIONARY_PAGE, INDEX_PAGE itd. – ovdje ne mijenjamo valuesRead
				}

				// (Ako želiš čitati page data, ovdje bi išlo:)
				// pageData := make([]byte, header.CompressedPageSize)
				// _, err = f.ReadAt(pageData, pageDataOffset)
				// if err != nil {
				//     fmt.Println("    Failed reading page data:", err)
				//     break
				// }

				// Sljedeći header dolazi odmah nakon kompresovanih podataka trenutne stranice
				offset = pageDataOffset + int64(header.CompressedPageSize)
				pageIdx++
			}

			fmt.Printf("  Column %d finished: valuesRead=%d / %d\n\n",
				colIdx, valuesRead, numValuesTotal)
		}
	}
}
