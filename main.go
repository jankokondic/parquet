package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"time"

	"root/gen-go/parquet"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/golang/snappy"
)

//
// 1. countingReader + countingTransport + footer + page header
//

// countingReader čita direktno iz fajla preko ReadAt i broji koliko je bajtova pročitano.
type countingReader struct {
	f        *os.File
	offset   int64 // početak header-a u fajlu
	consumed int64 // koliko smo bajtova pročitali preko ovog reader-a
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.f.ReadAt(p, r.offset+r.consumed)
	r.consumed += int64(n)
	return n, err
}

// countingTransport je minimalna implementacija thrift.TTransport
// koja wrapa naš countingReader. NEMA dodatnog buffera.
type countingTransport struct {
	r *countingReader
}

func (t *countingTransport) Read(p []byte) (int, error) {
	return t.r.Read(p)
}

func (t *countingTransport) Write(p []byte) (int, error) {
	return 0, thrift.NewTTransportException(thrift.UNKNOWN_TRANSPORT_EXCEPTION, "write not supported")
}

func (t *countingTransport) Close() error {
	return nil
}

func (t *countingTransport) Flush(ctx context.Context) error {
	return nil
}

func (t *countingTransport) Open() error {
	return nil
}

func (t *countingTransport) IsOpen() bool {
	return true
}

func (t *countingTransport) RemainingBytes() uint64 {
	return ^uint64(0)
}

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
	cr := &countingReader{
		f:      f,
		offset: offset,
	}

	ct := &countingTransport{r: cr}
	protocol := thrift.NewTCompactProtocolFactoryConf(&thrift.TConfiguration{}).GetProtocol(ct)

	header := parquet.NewPageHeader()
	if err := header.Read(context.Background(), protocol); err != nil {
		return nil, 0, fmt.Errorf("failed reading page header: %w", err)
	}

	pageDataOffset := offset + cr.consumed
	return header, pageDataOffset, nil
}

//
// 2. Helperi: dekompresija, bitsNeeded, RLE/bit-packed
//

// decompressPage dekompresuje payload stranice na osnovu codec-a.
func decompressPage(f *os.File, codec parquet.CompressionCodec, header *parquet.PageHeader, pageDataOffset int64) ([]byte, error) {
	compSize := int64(header.CompressedPageSize)
	pageData := make([]byte, compSize)
	_, err := f.ReadAt(pageData, pageDataOffset)
	if err != nil {
		return nil, fmt.Errorf("failed reading page data: %w", err)
	}

	switch codec {
	case parquet.CompressionCodec_UNCOMPRESSED:
		return pageData, nil
	case parquet.CompressionCodec_SNAPPY:
		decoded, err := snappy.Decode(nil, pageData)
		if err != nil {
			return nil, fmt.Errorf("failed to snappy-decompress page data: %w", err)
		}
		return decoded, nil
	default:
		return nil, fmt.Errorf("unsupported compression codec: %v", codec)
	}
}

// bitsNeeded vraća broj bitova potreban da se predstavi maxLevel (0..maxLevel).
func bitsNeeded(maxLevel int) int {
	if maxLevel <= 0 {
		return 0
	}
	b := 0
	for (1 << b) <= maxLevel {
		b++
	}
	return b
}

// readUVarIntFromSlice čita unsigned varint iz []byte (Parquet RLE/BitPacked header).
func readUVarIntFromSlice(data []byte, pos *int) (uint64, error) {
	var (
		x uint64
		s uint
	)
	for {
		if *pos >= len(data) {
			return 0, fmt.Errorf("varint: buffer too small")
		}
		b := data[*pos]
		*pos++
		if b < 0x80 {
			if s >= 64 {
				return 0, fmt.Errorf("varint: overflow")
			}
			x |= uint64(b) << s
			return x, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
		if s >= 64 {
			return 0, fmt.Errorf("varint: overflow")
		}
	}
}

// decodeRLEBitPackedHybridWithUsed dekodira RLE/BitPacked hibrid i vraća vrijednosti + koliko bajtova je potrošeno iz data.
func decodeRLEBitPackedHybridWithUsed(data []byte, bitWidth int, numValues int) ([]int32, int, error) {
	if bitWidth <= 0 || bitWidth > 32 {
		return nil, 0, fmt.Errorf("invalid bitWidth: %d", bitWidth)
	}
	mask := uint32((1 << bitWidth) - 1)

	pos := 0
	out := make([]int32, 0, numValues)

	for len(out) < numValues {
		if pos >= len(data) {
			return nil, 0, fmt.Errorf("ran out of data in RLE/BitPacked stream")
		}
		header, err := readUVarIntFromSlice(data, &pos)
		if err != nil {
			return nil, 0, err
		}

		if header&1 == 0 {
			// RLE run: (header >> 1) = runLen
			runLen := int(header >> 1)
			byteWidth := (bitWidth + 7) / 8
			if pos+byteWidth > len(data) {
				return nil, 0, fmt.Errorf("not enough bytes for RLE value")
			}
			var v uint32
			for i := 0; i < byteWidth; i++ {
				v |= uint32(data[pos+i]) << (8 * i)
			}
			pos += byteWidth
			v &= mask
			for i := 0; i < runLen && len(out) < numValues; i++ {
				out = append(out, int32(v))
			}
		} else {
			// Bit-packed run: (header >> 1) = numGroups, each group = 8 values
			numGroups := int(header >> 1)
			totalVals := numGroups * 8
			bitsTotal := totalVals * bitWidth
			bytesNeeded := (bitsTotal + 7) / 8
			if pos+bytesNeeded > len(data) {
				return nil, 0, fmt.Errorf("not enough bytes for bit-packed values")
			}
			runData := data[pos : pos+bytesNeeded]
			pos += bytesNeeded

			bitPos := 0
			for i := 0; i < totalVals && len(out) < numValues; i++ {
				var v uint32
				bitsRead := 0
				for bitsRead < bitWidth {
					byteIndex := bitPos / 8
					bitOffset := bitPos % 8
					if byteIndex >= len(runData) {
						return nil, 0, fmt.Errorf("out of range in bit-packed run")
					}
					cur := runData[byteIndex]
					available := 8 - bitOffset
					need := bitWidth - bitsRead
					take := available
					if need < take {
						take = need
					}
					part := (cur >> bitOffset) & byte((1<<take)-1)
					v |= uint32(part) << bitsRead
					bitsRead += take
					bitPos += take
				}
				v &= mask
				out = append(out, int32(v))
			}
		}
	}

	if len(out) > numValues {
		out = out[:numValues]
	}
	return out, pos, nil
}

// decodeRLEBitPackedHybrid – stari helper (bez used), ako ti ikad zatreba
func decodeRLEBitPackedHybrid(data []byte, bitWidth int, numValues int) ([]int32, error) {
	vals, _, err := decodeRLEBitPackedHybridWithUsed(data, bitWidth, numValues)
	return vals, err
}

// decodeLevels dekodira def/rep levele; vraća slice i broj potrošenih bajtova iz buf-a.
func decodeLevels(buf []byte, maxLevel int, numVals int) ([]int32, int, error) {
	if maxLevel == 0 || numVals == 0 {
		return nil, 0, nil
	}
	bitWidth := bitsNeeded(maxLevel)
	if bitWidth == 0 {
		return nil, 0, nil
	}
	levels, used, err := decodeRLEBitPackedHybridWithUsed(buf, bitWidth, numVals)
	if err != nil {
		return nil, 0, err
	}
	return levels, used, nil
}

//
// 3. maxDefLevel / maxRepLevel za kolonu (pretpostavka: flat schema)
//

func maxLevelsForColumn(meta *parquet.FileMetaData, path []string) (maxDef, maxRep int) {
	if meta == nil || meta.Schema == nil || len(path) == 0 {
		return 0, 0
	}
	leafName := path[len(path)-1]

	for i, se := range meta.Schema {
		if i == 0 {
			continue // root
		}
		if se.Name == leafName {
			if se.RepetitionType == nil {
				return 0, 0
			}
			switch *se.RepetitionType {
			case parquet.FieldRepetitionType_REQUIRED:
				return 0, 0
			case parquet.FieldRepetitionType_OPTIONAL:
				return 1, 0
			case parquet.FieldRepetitionType_REPEATED:
				return 1, 1
			default:
				return 0, 0
			}
		}
	}
	return 0, 0
}

//
// 4. INT32: dictionary page + data page (PLAIN + DICTIONARY) sa def-levelima
//

// DICTIONARY_PAGE za INT32 (payload je PLAIN int32, i kad encoding=PLAIN_DICTIONARY)
func decodeDictionaryPageInt32(f *os.File, codec parquet.CompressionCodec, header *parquet.PageHeader, pageDataOffset int64) ([]int32, error) {
	if header.Type != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("not a dictionary page: %v", header.Type)
	}
	if header.DictionaryPageHeader == nil {
		return nil, fmt.Errorf("DictionaryPageHeader is nil")
	}

	enc := header.DictionaryPageHeader.Encoding
	if enc != parquet.Encoding_PLAIN &&
		enc != parquet.Encoding_PLAIN_DICTIONARY &&
		enc != parquet.Encoding_RLE_DICTIONARY {
		return nil, fmt.Errorf("unsupported dictionary encoding for INT32: %v", enc)
	}

	numVals := int(header.DictionaryPageHeader.NumValues)
	if numVals == 0 {
		return []int32{}, nil
	}

	uncompressed, err := decompressPage(f, codec, header, pageDataOffset)
	if err != nil {
		return nil, err
	}
	if len(uncompressed) < numVals*4 {
		return nil, fmt.Errorf("dictionary payload too small: %d bytes for %d int32", len(uncompressed), numVals)
	}

	dict := make([]int32, numVals)
	for i := 0; i < numVals; i++ {
		o := i * 4
		dict[i] = int32(binary.LittleEndian.Uint32(uncompressed[o : o+4]))
	}
	return dict, nil
}

// decodeInt32PlainValuesFrom: PLAIN int32 data, uzimajući u obzir def-levels (NULL-ove).
func decodeInt32PlainValuesFrom(buf []byte, defLevels []int32, maxDef int, numRows int) ([]int32, int, error) {
	values := make([]int32, numRows)
	pos := 0

	if maxDef == 0 {
		need := numRows * 4
		if len(buf) < need {
			return nil, 0, fmt.Errorf("uncompressed size %d too small for %d int32 values", len(buf), numRows)
		}
		for i := 0; i < numRows; i++ {
			values[i] = int32(binary.LittleEndian.Uint32(buf[pos : pos+4]))
			pos += 4
		}
		return values, pos, nil
	}

	for i := 0; i < numRows; i++ {
		if defLevels[i] == int32(maxDef) {
			if pos+4 > len(buf) {
				return nil, 0, fmt.Errorf("not enough bytes for int32 value (pos=%d, len=%d)", pos, len(buf))
			}
			values[i] = int32(binary.LittleEndian.Uint32(buf[pos : pos+4]))
			pos += 4
		} else {
			// NULL -> stavi 0 ili neki sentinel
			values[i] = 0
		}
	}
	return values, pos, nil
}

// decodeInt32DictionaryValuesFrom: dictionary encoded INT32, sa def-levelima.
func decodeInt32DictionaryValuesFrom(buf []byte, dict []int32, defLevels []int32, maxDef int, numRows int) ([]int32, int, error) {
	if len(buf) == 0 {
		return nil, 0, fmt.Errorf("empty buffer for dictionary-encoded data")
	}

	numPhysical := numRows
	if maxDef > 0 && defLevels != nil {
		c := 0
		for _, d := range defLevels {
			if d == int32(maxDef) {
				c++
			}
		}
		numPhysical = c
	}

	bitWidth := int(buf[0])
	indexStream := buf[1:]

	indices, usedIdx, err := decodeRLEBitPackedHybridWithUsed(indexStream, bitWidth, numPhysical)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode dictionary indices: %w", err)
	}

	values := make([]int32, numRows)
	idxPos := 0

	for i := 0; i < numRows; i++ {
		if maxDef == 0 || (defLevels != nil && defLevels[i] == int32(maxDef)) {
			if idxPos >= len(indices) {
				return nil, 0, fmt.Errorf("ran out of indices (idxPos=%d, len=%d)", idxPos, len(indices))
			}
			idx := indices[idxPos]
			idxPos++
			if idx < 0 || int(idx) >= len(dict) {
				return nil, 0, fmt.Errorf("dictionary index out of range: %d (dict size %d)", idx, len(dict))
			}
			values[i] = dict[idx]
		} else {
			// NULL
			values[i] = 0
		}
	}

	return values, 1 + usedIdx, nil
}

// ReadInt32ColumnChunkInto: dekodira cijeli INT32 column chunk u dati buffer.
func ReadInt32ColumnChunkInto(meta *parquet.FileMetaData, f *os.File, col *parquet.ColumnChunk, dest []int32) error {
	md := col.MetaData
	if md == nil {
		return fmt.Errorf("column has nil MetaData")
	}
	if md.Type != parquet.Type_INT32 {
		return fmt.Errorf("column type is %v, expected INT32", md.Type)
	}

	codec := md.Codec
	numValuesTotal := int(md.NumValues)
	if len(dest) < numValuesTotal {
		return fmt.Errorf("dest too small: have %d, need %d", len(dest), numValuesTotal)
	}

	maxDef, maxRep := maxLevelsForColumn(meta, md.PathInSchema)

	var dictOffset int64
	if md.DictionaryPageOffset != nil {
		dictOffset = *md.DictionaryPageOffset
	}
	dataPageOffset := md.DataPageOffset

	offset := dataPageOffset
	if dictOffset != 0 && dictOffset < offset {
		offset = dictOffset
	}

	var (
		dict       []int32
		valuesRead int
		writePos   int
		pageIdx    int
	)

	_ = maxRep // za sada ne koristimo rep-level (nema nested/repeated)

	for valuesRead < numValuesTotal {
		header, pageDataOffset, err := ReadPageHeaderAndDataOffset(f, offset)
		if err != nil {
			return fmt.Errorf("reading page header at %d failed: %w", offset, err)
		}

		switch header.Type {
		case parquet.PageType_DICTIONARY_PAGE:
			dict, err = decodeDictionaryPageInt32(f, codec, header, pageDataOffset)
			if err != nil {
				return fmt.Errorf("failed to decode dictionary page: %w", err)
			}

		case parquet.PageType_DATA_PAGE:
			if header.DataPageHeader == nil {
				return fmt.Errorf("DATA_PAGE but DataPageHeader is nil")
			}

			numRows := int(header.DataPageHeader.NumValues)
			uncompressed, err := decompressPage(f, codec, header, pageDataOffset)
			if err != nil {
				return fmt.Errorf("failed to decompress INT32 page: %w", err)
			}
			buf := uncompressed

			// rep-leveli ako treba (ovdje ignorisani, ali trošimo bajtove ako maxRep>0)
			if maxRep > 0 {
				_, usedRep, err := decodeLevels(buf, maxRep, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode repetition levels: %w", err)
				}
				buf = buf[usedRep:]
			}

			// def-leveli
			var defLevels []int32
			if maxDef > 0 {
				var usedDef int
				defLevels, usedDef, err = decodeLevels(buf, maxDef, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode definition levels: %w", err)
				}
				buf = buf[usedDef:]
			}

			enc := header.DataPageHeader.Encoding
			var pageVals []int32
			var usedVals int

			switch enc {
			case parquet.Encoding_PLAIN:
				pageVals, usedVals, err = decodeInt32PlainValuesFrom(buf, defLevels, maxDef, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode PLAIN INT32 page: %w", err)
				}
				_ = usedVals // ne trebaju nam dalje

			case parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_RLE_DICTIONARY:
				if len(dict) == 0 {
					return fmt.Errorf("dictionary-encoded page but dictionary is empty")
				}
				pageVals, usedVals, err = decodeInt32DictionaryValuesFrom(buf, dict, defLevels, maxDef, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode dictionary-encoded INT32 page: %w", err)
				}
				_ = usedVals

			default:
				return fmt.Errorf("unsupported INT32 page encoding: %v", enc)
			}

			if len(pageVals) != numRows {
				return fmt.Errorf("decoded %d values, header says %d", len(pageVals), numRows)
			}

			copy(dest[writePos:writePos+numRows], pageVals)
			writePos += numRows
			valuesRead += numRows

		case parquet.PageType_DATA_PAGE_V2:
			return fmt.Errorf("DATA_PAGE_V2 not supported yet for INT32")

		default:
			// INDEX_PAGE, BLOOM_FILTER itd. – preskačemo
		}

		offset = pageDataOffset + int64(header.CompressedPageSize)
		pageIdx++
		_ = pageIdx
	}

	if valuesRead != numValuesTotal {
		return fmt.Errorf("filled %d values, expected %d", valuesRead, numValuesTotal)
	}
	return nil
}

//
// 5. FLOAT: dictionary + PLAIN sa def-levelima
//

// DICTIONARY_PAGE za FLOAT (payload je PLAIN float32, čak i kad encoding=PLAIN_DICTIONARY)
func decodeDictionaryPageFloat(f *os.File, codec parquet.CompressionCodec, header *parquet.PageHeader, pageDataOffset int64) ([]float32, error) {
	if header.Type != parquet.PageType_DICTIONARY_PAGE {
		return nil, fmt.Errorf("not a dictionary page: %v", header.Type)
	}
	if header.DictionaryPageHeader == nil {
		return nil, fmt.Errorf("DictionaryPageHeader is nil")
	}

	enc := header.DictionaryPageHeader.Encoding
	if enc != parquet.Encoding_PLAIN &&
		enc != parquet.Encoding_PLAIN_DICTIONARY &&
		enc != parquet.Encoding_RLE_DICTIONARY {
		return nil, fmt.Errorf("unsupported dictionary encoding for FLOAT: %v", enc)
	}

	numVals := int(header.DictionaryPageHeader.NumValues)
	if numVals == 0 {
		return []float32{}, nil
	}

	uncompressed, err := decompressPage(f, codec, header, pageDataOffset)
	if err != nil {
		return nil, err
	}
	if len(uncompressed) < numVals*4 {
		return nil, fmt.Errorf("dictionary payload too small: %d bytes for %d float32", len(uncompressed), numVals)
	}

	dict := make([]float32, numVals)
	for i := 0; i < numVals; i++ {
		o := i * 4
		bits := binary.LittleEndian.Uint32(uncompressed[o : o+4])
		dict[i] = math.Float32frombits(bits)
	}
	return dict, nil
}

// decodeFloatPlainValuesFrom: PLAIN float32 data sa def-levelima.
func decodeFloatPlainValuesFrom(buf []byte, defLevels []int32, maxDef int, numRows int) ([]float32, int, error) {
	values := make([]float32, numRows)
	pos := 0

	if maxDef == 0 {
		need := numRows * 4
		if len(buf) < need {
			return nil, 0, fmt.Errorf("uncompressed size %d too small for %d float32 values", len(buf), numRows)
		}
		for i := 0; i < numRows; i++ {
			bits := binary.LittleEndian.Uint32(buf[pos : pos+4])
			values[i] = math.Float32frombits(bits)
			pos += 4
		}
		return values, pos, nil
	}

	for i := 0; i < numRows; i++ {
		if defLevels[i] == int32(maxDef) {
			if pos+4 > len(buf) {
				return nil, 0, fmt.Errorf("not enough bytes for float32 value (pos=%d, len=%d)", pos, len(buf))
			}
			bits := binary.LittleEndian.Uint32(buf[pos : pos+4])
			values[i] = math.Float32frombits(bits)
			pos += 4
		} else {
			// NULL → NaN kao sentinel
			values[i] = float32(math.NaN())
		}
	}
	return values, pos, nil
}

// decodeFloatDictionaryValuesFrom: dictionary encoded FLOAT sa def-levelima.
func decodeFloatDictionaryValuesFrom(buf []byte, dict []float32, defLevels []int32, maxDef int, numRows int) ([]float32, int, error) {
	if len(buf) == 0 {
		return nil, 0, fmt.Errorf("empty buffer for dictionary-encoded float data")
	}

	numPhysical := numRows
	if maxDef > 0 && defLevels != nil {
		c := 0
		for _, d := range defLevels {
			if d == int32(maxDef) {
				c++
			}
		}
		numPhysical = c
	}

	bitWidth := int(buf[0])
	indexStream := buf[1:]

	indices, usedIdx, err := decodeRLEBitPackedHybridWithUsed(indexStream, bitWidth, numPhysical)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode dictionary indices for FLOAT: %w", err)
	}

	values := make([]float32, numRows)
	idxPos := 0

	for i := 0; i < numRows; i++ {
		if maxDef == 0 || (defLevels != nil && defLevels[i] == int32(maxDef)) {
			if idxPos >= len(indices) {
				return nil, 0, fmt.Errorf("ran out of float indices (idxPos=%d, len=%d)", idxPos, len(indices))
			}
			idx := indices[idxPos]
			idxPos++
			if idx < 0 || int(idx) >= len(dict) {
				return nil, 0, fmt.Errorf("float dictionary index out of range: %d (dict size %d)", idx, len(dict))
			}
			values[i] = dict[idx]
		} else {
			// NULL → NaN
			values[i] = float32(math.NaN())
		}
	}

	return values, 1 + usedIdx, nil
}

func ReadFloatColumnChunkInto(meta *parquet.FileMetaData, f *os.File, col *parquet.ColumnChunk, dest []float32) error {
	md := col.MetaData
	if md == nil {
		return fmt.Errorf("column has nil MetaData")
	}
	if md.Type != parquet.Type_FLOAT {
		return fmt.Errorf("column type is %v, expected FLOAT", md.Type)
	}

	codec := md.Codec
	numValuesTotal := int(md.NumValues)
	if len(dest) < numValuesTotal {
		return fmt.Errorf("dest too small: have %d, need %d", len(dest), numValuesTotal)
	}

	maxDef, maxRep := maxLevelsForColumn(meta, md.PathInSchema)

	var dictOffset int64
	if md.DictionaryPageOffset != nil {
		dictOffset = *md.DictionaryPageOffset
	}
	dataPageOffset := md.DataPageOffset

	offset := dataPageOffset
	if dictOffset != 0 && dictOffset < offset {
		offset = dictOffset
	}

	var (
		dict       []float32
		valuesRead int
		writePos   int
		pageIdx    int
	)

	_ = maxRep // za sada ne koristimo rep-level

	for valuesRead < numValuesTotal {
		header, pageDataOffset, err := ReadPageHeaderAndDataOffset(f, offset)
		if err != nil {
			return fmt.Errorf("reading FLOAT page header at %d failed: %w", offset, err)
		}

		switch header.Type {
		case parquet.PageType_DICTIONARY_PAGE:
			dict, err = decodeDictionaryPageFloat(f, codec, header, pageDataOffset)
			if err != nil {
				return fmt.Errorf("failed to decode FLOAT dictionary page: %w", err)
			}

		case parquet.PageType_DATA_PAGE:
			if header.DataPageHeader == nil {
				return fmt.Errorf("FLOAT DATA_PAGE but DataPageHeader is nil")
			}

			numRows := int(header.DataPageHeader.NumValues)
			uncompressed, err := decompressPage(f, codec, header, pageDataOffset)
			if err != nil {
				return fmt.Errorf("failed to decompress FLOAT page: %w", err)
			}
			buf := uncompressed

			// rep-level (ignorisan, ali pojedemo bajtove ako maxRep>0)
			if maxRep > 0 {
				_, usedRep, err := decodeLevels(buf, maxRep, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode repetition levels (FLOAT): %w", err)
				}
				buf = buf[usedRep:]
			}

			// def-level
			var defLevels []int32
			if maxDef > 0 {
				var usedDef int
				defLevels, usedDef, err = decodeLevels(buf, maxDef, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode definition levels (FLOAT): %w", err)
				}
				buf = buf[usedDef:]
			}

			enc := header.DataPageHeader.Encoding
			var pageVals []float32
			var usedVals int

			switch enc {
			case parquet.Encoding_PLAIN:
				pageVals, usedVals, err = decodeFloatPlainValuesFrom(buf, defLevels, maxDef, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode FLOAT PLAIN page: %w", err)
				}
				_ = usedVals

			case parquet.Encoding_PLAIN_DICTIONARY, parquet.Encoding_RLE_DICTIONARY:
				if len(dict) == 0 {
					return fmt.Errorf("FLOAT dictionary-encoded page but dictionary is empty")
				}
				pageVals, usedVals, err = decodeFloatDictionaryValuesFrom(buf, dict, defLevels, maxDef, numRows)
				if err != nil {
					return fmt.Errorf("failed to decode FLOAT dictionary page: %w", err)
				}
				_ = usedVals

			default:
				return fmt.Errorf("unsupported FLOAT page encoding: %v", enc)
			}

			if len(pageVals) != numRows {
				return fmt.Errorf("decoded %d float values, header says %d", len(pageVals), numRows)
			}

			copy(dest[writePos:writePos+numRows], pageVals)
			writePos += numRows
			valuesRead += numRows

		case parquet.PageType_DATA_PAGE_V2:
			return fmt.Errorf("DATA_PAGE_V2 not supported yet for FLOAT")

		default:
			// INDEX_PAGE, BLOOM_FILTER, itd. – preskočimo
		}

		offset = pageDataOffset + int64(header.CompressedPageSize)
		pageIdx++
		_ = pageIdx
	}

	if valuesRead != numValuesTotal {
		return fmt.Errorf("filled %d values, expected %d", valuesRead, numValuesTotal)
	}

	return nil
}

//
// 6. main – dekodira sve kolone iz flights-1m.parquet
//

func main() {
	start := time.Now()

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

			fmt.Println("  Column:", colIdx,
				"path:", md.PathInSchema,
				"physicalType:", md.Type,
				"numValues:", md.NumValues,
				"codec:", md.Codec)

			switch md.Type {
			case parquet.Type_INT32:
				buf := make([]int32, md.NumValues)
				if err := ReadInt32ColumnChunkInto(meta, f, col, buf); err != nil {
					fmt.Println("    ERROR decoding INT32 column:", err)
					continue
				}

				limit := int(md.NumValues)
				if len(buf) < limit {
					limit = len(buf)
				}
				fmt.Printf("    INT32 first %d values:", limit)
				for i := 0; i < limit; i++ {
					fmt.Printf(" %d", buf[i])
				}
				fmt.Println()

			case parquet.Type_FLOAT:
				buf := make([]float32, md.NumValues)
				if err := ReadFloatColumnChunkInto(meta, f, col, buf); err != nil {
					fmt.Println("    ERROR decoding FLOAT column:", err)
					continue
				}
				limit := int(md.NumValues)
				if len(buf) < limit {
					limit = len(buf)
				}
				fmt.Printf("    FLOAT first %d values:", limit)
				for i := 0; i < limit; i++ {
					fmt.Printf(" %.2f", buf[i])
				}
				fmt.Println()

			default:
				fmt.Println("    (decoder not implemented for type:", md.Type, ")")
			}

			fmt.Println()
		}
	}

	fmt.Println(time.Since(start))
}
