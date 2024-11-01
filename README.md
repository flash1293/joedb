# 🐿️JoeDB

This notebook is a test implementation of 🐿️JoeDB - a really simple database for logs storage.

The idea is to build a log storage engine from scratch to learn how things work and how important which parts are. There are a ton of aspects for production-grade logs storage, so we need to start small.

## MVP scope

* Focus on storage size, not performance
* No distributed storage
* No sophisticated query languages
* No indices
* Store only JSON and be able to reconstruct from binary format
* Try to get the binary format as small as possible
* Support JSON objects with arbitrary nesting depth, but only string values and no arrays
* Build in a way that it's still possible later on to query without scanning everything

## Setup

* Columnar storage with run length encoding
* Trie per column for dictionary
* That's it - no further structures.

## Testing

Get all the structured csvs from https://github.com/logpai/loghub , convert them to JSON, store them with my algo, restore them, check whether the output is identical.

Here are better larger datasets https://docs.yscope.com/clp/main/user-guide/resources-datasets

Compare against: gzip on the JSON, gzip on the raw csv, parquet

Later on other opensource tools like Elasticsearch, Clickhouse, etc. (this won't be an apples-to-apples comparison, but oh my)

## Algorithm

To encode:
* Flatten the JSON object down
* In a hash map, init a trie and and array for each flattened key
* look up the value of the key - if it's not in the trie, add it and assign it an incrementing number
* In the array, append the incrementing number
* If a key doesn't exist in an object, put a zero
* Do this later - Once all objects are consumed like this, check how many different values are in each column to pick whether a byte or a longer representation for the coding
* Apply run length encoding of (dictionary-index, length) - dictionary index zero means no value
* Concatenate the tries, then concatenate the RLE-columns
* Store as file

To decode:
* Read the trees and RLE-columns back into memory
* Expand RLE
* Resolve all columns from trie
* Iterate all columns at once and piece together JSON objects


## Binary format

* Magic header: `0xf0 0x9f 0x90 0xbf 0xef 0xb8 0x8f 0x6a 0x6f 0x65 0x64 0x62` (`🐿️joedb`)
* Number of records in 8 bytes
* Hash map of tries:
  * 0-terminated utf8 string of the flattened field name
  * Trie (in depth-first search):
    * 4 bytes for the length of gzip compressed data
    * Gzip-compressed data:
      * Node (id is just increment, starting with 1):
        * 0-terminated utf8 string of the prefix
        * Number of children in 1 bytes
        * Children
      * One zero byte to mark end of each trie
  * One zero byte to mark end of all tries
  * For each entry of the hash map one column:
    * Single byte denoting the number of bytes required to represent the dictionary index
    * Single byte denoting the number of bytes required to represent the length
    * 4 bytes for the length of gzip compressed data
    * Gzip-compressed data:
      * For every RLE pair:
        * Dictionary index as how many bytes are necessary based on the size of the trie
        * Length

## Next steps

* Implement dedicated number and date columns with double delta encoding + RLE
* Implement a simple CLP scheme that uses number and date columns

## Better baseline to see how work the trie does

* Test another, much simpler format:
* A column index in the front that stores offsets to where the columns start
* Each column has an RLE-encoding scheme per entry:
  * 0-byte: no value
  * 1-byte: value just for the current record, read null-terminated string
  * 2-byte: multi-value, one byte for length, then read null-terminated string
  * 3-byte: multi-value, two bytes for length, then read null-terminated string

That might compress just as well - theoretically gzip/zstd can't look back as far as my trie, but I'm not sure whether that matters in practice.

## Looking beyond

The above works just fine, next possible steps (need to sleep over it):
* Implement a simple search / aggregation that only unpacks what's necessary
* Get ahold of all the loghub data
* Get ahold of some classic OTel logs from an Elastic instance
* Implement support for more data types - Make this part of the key name as a prefix (e.g. `Nmy.number.key`, `Smy.string.key`). Strings use a trie, numbers are just stored as is (could get smart here with stuff like delta encoding), Booleans could use a scheme where RLE and packed bitmaps are used (5 ternary values true/false/null in one byte), depending on the statistics of the column
* Implement support for null - this is easiest done by treating it as a separate column with a special type and then packing either as regular bitmap or as RLE like the boolean columns
* Implement date detection and a special column type for it that is doing delta encoding by default
* Implement support for arrays - make this part of the key name as well (e.g. `Smy.array.[__].nested.key`) - These columns are special because they just have one trie, but two columns (one using delta+RLE encoding for the record id and one for the value) - here no zeroes need to be added because the record id column contains the information what to skip. In case of multiple nested arrays like `Smy.array.[__].nested.array.[__].something_in_here`, there would be three columns - two with delta+RLE encoding for the record id and for the index of the outer array (which is like a sub-record in a sense), and then the regular value column.
* Think about how the fact can be exploited that the same record can't have the same key for multiple types at the same time. For example, the second column just completely ignores records that had a value in the first one and so on. Not sure how much it would change in practice though
* Implement a simple CLP scheme as well (should cut the file size down even further) - dynamically introduce sub-columns for all strings
* Store all key names in a trie itself at the beginning of the file, pointing to their tries via offset - the key name trie itself can also be gzipped. This allows to do field discovery without having to touch the actual tries. Also add an offset to the column data as well. This allows to only unpack the key name trie to check whether a field exists, if it does, it allows to progressively unpack the value trie to see whether a certain value actually exists, and only if it does the column data can be read progressively.
* Split up key trie, value tries and columns into different files and implement reading/writing directly from and to S3 in an efficient way
* Look into how to it can be avoided to decompress the whole value trie when searching for a specific value - right now this would be pretty costly for queries that match a certain value for a key. If it would be possible to get the index of this prefix in a cheap way, matches could be found pretty fast by scanning the column, looking for this value index. But I can't come up with a good way - this would be big though
* For numbers, we could probe a small amount of values to decide which encoding to use (delta encoding, double delta encoding, trie, direct values)
* Support dots in field names properly by escaping them somehow
* Change the trie representation to breadth search instead of depth first - then on top of this for the query-engine, implement early stopping: During progressively decompressing and decoding the trie, stop early if the query can't be satisfied any more

In general, this scheme is really good for cheap storage, and _some_ queries can be kept at an acceptable level of performance, but as soon as the whole record needs to be reconstructed, it will turn into a full scan and be super duper expensive. Really shows this tradeoff of search performance to storage size quite well. I need to turn this into a presentation!!!

Some thouhts:
It's not really avoidable to decode the full trie in this scheme - however, one can get smart by working with multiple segments clustered by the right keys to keep things cheap. Also, with CLP applied tries should stay _relatively_ small anyway.

For searching: If the segment is ordered descending in time (which is also a good default order of results), then search would work like this:
* Decode all tries
* Start progressively decoding and scanning the columns that are searched over and collect matches (this should probably happen in batches to exploit cache locality better)
* As soon as the number of desired matches is reached, decode the rest of the column up to the last match to reconstruct the full records