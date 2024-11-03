import datetime
import struct
from ppretty import ppretty
from .patternization import extract_pattern, rehydrate_message
import pyzstd
import humanize
import io
import hyperloglog


class TrieNode:
    def __init__(self):
        self.children = {}
        self.index = None  # Index assigned to this value

class Trie:
    def __init__(self):
        self.root = TrieNode()
        self.current_index = 1  # Start indexing from 1 (0 is reserved for "no value")

    def insert(self, word):
        node = self.root
        idx = 0

        while idx < len(word):
            for key in node.children:
                if word[idx:idx + len(key)] == key:
                    # Move to the next node if the key matches part of the word
                    node = node.children[key]
                    idx += len(key)
                    break
            else:
                break

        # Now, we need to either split an existing node or add a new one
        if idx == len(word):
            return node.index

        # Check if we need to split an existing node
        for key in list(node.children.keys()):
            common_len = self._common_prefix_length(word[idx:], key)
            if common_len > 0:
                # Split the node
                remaining_key = key[common_len:]
                remaining_word = word[idx + common_len:]

                # Create a new node for the remaining part of the original key
                new_node = TrieNode()
                new_node.children[remaining_key] = node.children[key]
                new_node.index = self.current_index  # Merged node doesn't have an index yet
                self.current_index += 1
                del node.children[key]

                # Insert the new node under the common prefix
                node.children[word[idx:idx + common_len]] = new_node

                # If there is a remaining word part, insert it too
                node = new_node
                if remaining_word:
                    new_node = TrieNode()
                    new_node.index = self.current_index
                    self.current_index += 1
                    node.children[remaining_word] = new_node
                    return new_node.index
                else:
                    node.index = self.current_index
                    self.current_index += 1
                    return node.index

        # If no common prefix, directly insert the rest of the word
        node.children[word[idx:]] = TrieNode()
        node.children[word[idx:]].index = self.current_index
        self.current_index += 1
        return node.children[word[idx:]].index

    def _common_prefix_length(self, str1, str2):
        """Helper function to get the length of the common prefix between two strings."""
        min_len = min(len(str1), len(str2))
        for i in range(min_len):
            if str1[i] != str2[i]:
                return i
        return min_len

    def rename_indices(self):
        """Rename the indices of the nodes in a depth-first manner."""
        rename_map = {}
        def dfs(node):
            if node.index is not None:
                # Renaming the index
                rename_map[node.index] = self.current_index
                node.index = self.current_index
                self.current_index += 1
            for child in node.children.values():
                dfs(child)

        self.current_index = 1  # Start indexing from 1
        dfs(self.root)

        return rename_map

    def merge_single_children(self, used_nodes=None):
        """Merge nodes that have a single child with their parent, excluding used nodes."""
        if used_nodes is None:
            used_nodes = set()

        def dfs(node):
            # Traverse all children
            for key, child in list(node.children.items()):
                while len(child.children) == 1 and child.index not in used_nodes:
                    # If the child has only one child and is not referenced, merge it
                    grandchild_key = list(child.children.keys())[0]
                    node.children[key + grandchild_key] = child.children[grandchild_key]
                    del node.children[key]
                    child = node.children[key + grandchild_key]
                    key = key + grandchild_key
                # Continue to traverse
                dfs(child)

        dfs(self.root)

COMPRESSION_LEVEL = 15

def flatten_json(data, parent_key='', sep='.'):
    """Flatten nested JSON objects."""
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def run_length_encode(data):
    """Run-length encoding for a list of values."""
    encoded = []
    if not data:
        return encoded
    prev_value = data[0]
    count = 1
    for value in data[1:]:
        if value == prev_value:
            count += 1
        else:
            encoded.append((prev_value, count))
            prev_value = value
            count = 1
    encoded.append((prev_value, count))
    return encoded


class JoeDB:
    MAGIC_HEADER = b'\xf0\x9f\x90\xbf\xef\xb8\x8f\x6a\x6f\x65\x64\x62'  # 🐿️joedb
    TYPE_STRING = 0x01  # Column type byte for string columns
    TYPE_NUMBER = 0x02  # Column type byte for number columns
    TYPE_TIMESTAMP = 0x03  # Column type byte for number columns


    def __init__(self, use_patternization=True):
        self.columns = {}
        self.column_types = {}
        self.cardinality = {}
        self.tries: dict[str, Trie] = {}
        self.record_count = 0
        self.use_patternization = use_patternization

    def insert(self, json_object):
        """Insert a JSON object into the database."""
        flat_data = flatten_json(json_object)
        keys = set(flat_data.keys())

        for key, value in flat_data.items():
            pattern, vars = extract_pattern(value, key) if self.use_patternization else (value, {})
            # actual columns are key plus the keys of the vars object
            local_keys = [key] + list(vars.keys())
            keys.update(local_keys)
            for local_key in local_keys:
                is_number_column = False if local_key not in self.column_types else self.column_types[local_key] == self.TYPE_NUMBER
                is_timestamp_column = False if local_key not in self.column_types else self.column_types[local_key] == self.TYPE_TIMESTAMP
                if local_key not in self.columns:
                    self.columns[local_key] = []
                    self.cardinality[local_key] = hyperloglog.HyperLogLog(0.01)
                    self.columns[local_key].extend([0] * self.record_count)
                    # a number column key always starts with var_ and ends with _number<int>
                    is_timestamp_column = local_key.startswith('var_') and local_key.endswith('_timestamp')
                    is_number_column = is_timestamp_column or local_key.startswith('var_') and local_key.endswith('_number')
                    if is_timestamp_column:
                        self.column_types[local_key] = self.TYPE_TIMESTAMP
                    elif is_number_column:
                        self.column_types[local_key] = self.TYPE_NUMBER
                    else:
                        self.column_types[local_key] = self.TYPE_STRING
                    if not is_number_column:
                        self.tries[local_key] = Trie()
                # Insert into trie and get the index
                local_value = pattern if local_key == key else vars[local_key]
                if not is_number_column and not is_timestamp_column:
                    # Use Trie for strings
                    index = self.tries[local_key].insert(local_value if isinstance(local_value, str) else str(local_value))
                elif is_timestamp_column:
                    # convert iso timestamp to unix timestamp
                    index = int(datetime.datetime.fromisoformat(local_value).timestamp())
                else:
                    # Directly store number for delta encoding
                    index = local_value
                self.columns[local_key].append(index)
                self.cardinality[local_key].add(local_value)

        for key in self.columns:
            if not key in keys:
                self.columns[key].append(0)

        self.record_count += 1

    def encode(self, file_path):
        """Encode the database into a binary format and store it."""
        self.write_counter = 0
        # =============== PRINT:THE:TREE =================
        # print(ppretty(self.tries, depth=20))
        print(ppretty(self.columns, depth=20))
        print(ppretty(self.column_types, depth=20))
        with open(file_path, 'wb') as f:
            # Write the magic header
            f.write(self.MAGIC_HEADER)

            # Write the number of records
            f.write(struct.pack('>Q', self.record_count))  # 8-byte unsigned int

            for key, trie in self.tries.items():
              trie.merge_single_children(self.columns[key])

            # =============== PRINT:THE:TREE =================
            # print(ppretty(self.tries, depth=20))

            rename_maps = {key: trie.rename_indices() for key, trie in self.tries.items()}

            # Order all columns by cardinality
            ordered_columns =  sorted(self.columns.keys(), key=lambda x: len(self.cardinality[x]))
            print("Ordered columns:", ordered_columns)


            # join all the columns back into records
            records = []
            for i in range(self.record_count):
                record = {}
                for key in ordered_columns:
                    record[key] = self.columns[key][i]
                records.append(record)
            
            # order the records by all the columns in order of cardinality
            records = sorted(records, key=lambda x: tuple(str(x[key]) for key in ordered_columns))

            # put the records back into the columns
            for key in self.columns:
                self.columns[key] = [record[key] for record in records]

            # Write the tries
            for key, column in self.columns.items():
                # Write column type
                f.write(struct.pack('B', self.column_types[key]))

                # Write column name with null terminator
                f.write(key.encode('utf-8') + b'\x00')

                if self.column_types[key] == self.TYPE_STRING:
                    compressed_data = io.BytesIO()
                    with pyzstd.ZstdFile(filename=compressed_data, mode='wb', level_or_option=COMPRESSION_LEVEL) as gz:
                        self._write_trie(gz, self.tries[key].root)
                        gz.write(b'\x00')
                    compressed_bytes = compressed_data.getvalue()
                    f.write(struct.pack('>I', len(compressed_bytes)))
                    f.write(compressed_bytes)

                    self.write_counter += compressed_data.tell()
                    print("Wrote", humanize.naturalsize(compressed_data.tell()), "bytes for trie", key)
            f.write(b'\x00')  # End of tries marker

            print("Wrote", humanize.naturalsize(self.write_counter), "bytes for tries")

            # Write the columns with RLE
            for key, column in self.columns.items():
                print("Writing column", key)
                self.write_counter = 0
                compressed_data = io.BytesIO()
                if self.column_types[key] == self.TYPE_STRING:
                    rename_map = rename_maps[key]
                    column = [rename_map.get(value, value) for value in column]
                    rle_encoded = run_length_encode(column)
                    # print(ppretty(rle_encoded, depth=10))
                    max_value = max(column)
                    max_length = max(rle_encoded, key=lambda x: x[1])[1]
                    value_byte_size = (max_value.bit_length() + 7) // 8
                    length_byte_size = (max_length.bit_length() + 7) // 8
                    print("Value byte size:", value_byte_size)
                    print("Length byte size:", length_byte_size)
                    f.write(struct.pack('B', value_byte_size))  # TODO: This can be calculated from the tree, remove
                    f.write(struct.pack('B', length_byte_size))  # Write byte size for RLE values

                    # Compress the RLE data using gzip
                    with pyzstd.ZstdFile(filename=compressed_data, mode='wb', level_or_option=COMPRESSION_LEVEL) as gz:
                        for value, length in rle_encoded:
                            gz.write(value.to_bytes(value_byte_size, byteorder='big'))
                            gz.write(length.to_bytes(length_byte_size, byteorder='big'))
                    # Get the compressed data and write its length
                    compressed_bytes = compressed_data.getvalue()
                    f.write(struct.pack('>I', len(compressed_bytes)))  # Write 4-byte compressed length
                    f.write(compressed_bytes)  # Write compressed data
                else:
                    # Delta + RLE encoding for number columns
                    delta_encoded = [int(column[0])]
                    for i in range(1, len(column)):
                        delta_encoded.append(int(column[i]) - int(column[i - 1]))

                    rle_encoded = run_length_encode(delta_encoded)
                    max_value = max(delta_encoded, key=abs)
                    value_byte_size = (max_value.bit_length() + 8) // 8

                    # calculate leading zeros per value
                    leading_zeros = [len(str(value)) - len(str(value).lstrip('0')) for value in column]

                    max_length = max(rle_encoded, key=lambda x: x[1])[1]
                    length_byte_size = (max_length.bit_length() + 7) // 8
                    print("Value byte size:", value_byte_size)
                    print("Length byte size:", length_byte_size)

                    f.write(struct.pack('B', value_byte_size))
                    f.write(struct.pack('B', length_byte_size))

                    with pyzstd.ZstdFile(filename=compressed_data, mode='wb', level_or_option=COMPRESSION_LEVEL) as gz:
                        for value, length in rle_encoded:
                            gz.write(value.to_bytes(value_byte_size, byteorder='big', signed=True))
                            gz.write(length.to_bytes(length_byte_size, byteorder='big'))
                            if self.column_types[key] == self.TYPE_NUMBER:
                                # TODO: This scheme doesn't work - leading zeros don't necessarily share the same runs as the values
                                leading_zero_count = leading_zeros.pop(0)
                                gz.write(leading_zero_count.to_bytes(1, byteorder='big'))
                    compressed_bytes = compressed_data.getvalue()
                    f.write(struct.pack('>I', len(compressed_bytes)))
                    f.write(compressed_bytes)
                print("Wrote", humanize.naturalsize(compressed_data.tell()), "bytes for column", key)

    def _write_trie(self, f, node):
        """Recursively write a trie to a file in depth-first order."""
        for char, child in node.children.items():
            f.write(char.encode('utf-8') + b'\x00')
            f.write(struct.pack('>B', len(child.children)))  # Number of children
            self._write_trie(f, child)

    def decode(self, file_path):
        """Decode a binary format back into the original JSON objects."""
        with open(file_path, 'rb') as f:
            # Read and check the magic header
            magic_header = f.read(len(self.MAGIC_HEADER))
            if magic_header != self.MAGIC_HEADER:
                raise ValueError("Invalid file format!")

            # Read number of records
            record_count = struct.unpack('>Q', f.read(8))[0]

            # Read tries
            self.tries = {}
            self.trie_value_maps: dict[str, dict[int, str]] = {}
            while True:
                column_type = f.read(1)
                if column_type == b'\x00':
                    break
                column_type = column_type[0]

                key = self._read_null_terminated_string(f)
                self.column_types[key] = column_type

                self.columns[key] = []
                if column_type == self.TYPE_STRING:
                    self.tries[key] = Trie()
                    compressed_length = struct.unpack('>I', f.read(4))[0]  # Read 4-byte compressed length
                    compressed_data = f.read(compressed_length)  # Read the compressed column data

                    with pyzstd.ZstdFile(filename=io.BytesIO(compressed_data), mode='rb') as gz:
                        self._read_trie(gz, self.tries[key].root, self.tries[key])

                    # Build hashmap for fast index lookup
                    self.trie_value_maps[key] = {}
                    self._build_trie_value_map(self.tries[key].root, '', self.trie_value_maps[key])

                # ============ PRINT:THE:TREE ================
                # print(ppretty(self.tries[key].root, depth=20))

            # Read columns
            for key in self.columns:
                if self.column_types[key] == self.TYPE_STRING:
                    value_byte_size = struct.unpack('B', f.read(1))[0]  # TODO: This can be calculated from the tree, remove
                    length_byte_size = struct.unpack('B', f.read(1))[0]
    #                column_data = []
    #                while len(column_data) < record_count:
    #                    value = int.from_bytes(f.read(value_byte_size), byteorder='big')
    #                    length = int.from_bytes(f.read(length_byte_size), byteorder='big')
    #                    column_data.extend([value] * length)
    #                self.columns[key] = column_data
                    compressed_length = struct.unpack('>I', f.read(4))[0]  # Read 4-byte compressed length
                    compressed_data = f.read(compressed_length)  # Read the compressed column data

                    with pyzstd.ZstdFile(filename=io.BytesIO(compressed_data), mode='rb') as gz:
                        column_data = []
                        while len(column_data) < record_count:
                            value = int.from_bytes(gz.read(value_byte_size), byteorder='big')
                            length = int.from_bytes(gz.read(length_byte_size), byteorder='big')
                            column_data.extend([value] * length)
                        self.columns[key] = column_data
                else:
                    # Read value/length byte sizes
                    value_byte_size = struct.unpack('B', f.read(1))[0]
                    length_byte_size = struct.unpack('B', f.read(1))[0]

                    compressed_length = struct.unpack('>I', f.read(4))[0]
                    compressed_data = f.read(compressed_length)

                    column_data = []
                    leading_zeros = []
                    with pyzstd.ZstdFile(filename=io.BytesIO(compressed_data), mode='rb') as gz:
                        while len(column_data) < record_count:
                            value = int.from_bytes(gz.read(value_byte_size), byteorder='big', signed=True)
                            length = int.from_bytes(gz.read(length_byte_size), byteorder='big')
                            column_data.extend([value] * length)
                            if self.column_types[key] == self.TYPE_NUMBER:
                                leading_zeros.extend([int.from_bytes(gz.read(1), byteorder='big')] * length)
                    

                    # Decode deltas
                    decoded_column = [column_data[0]]
                    # if column_data[0] == 0:
                        # remove one leading zeros, because it got encoded in the column
                        # leading_zeros[0] = leading_zeros[0] - 1
                    for i in range(1, len(column_data)):
                        new_val = decoded_column[-1] + column_data[i]
                        decoded_column.append(new_val)
                        # if new_val == 0:
                            # remove one leading zeros, because it got encoded in the column
                           # leading_zeros[i] = leading_zeros[i] - 1
                    # convert to stream and add leading zeros
                    if self.column_types[key] == self.TYPE_NUMBER:
                        self.columns[key] = [str(decoded_column[i]).zfill(leading_zeros[i] + len(str(decoded_column[i]))) for i in range(len(decoded_column))]
                    elif self.column_types[key] == self.TYPE_TIMESTAMP:
                        self.columns[key] = [datetime.datetime.fromtimestamp(decoded_column[i], datetime.UTC).isoformat() for i in range(len(decoded_column))]


           # Split out pattern columns (start with var_)
            pattern_columns = {key: self.columns[key] for key in self.columns if key.startswith('var_')}
            real_columns = {key: self.columns[key] for key in self.columns if not key.startswith('var_')}
            

            # Reconstruct JSON objects
            json_objects = []
            for i in range(record_count):
                json_object = {}
                # Get pattern values for the current record
                pattern_values = {key: self.trie_value_maps[key].get(column[i], None) if self.column_types[key] == self.TYPE_STRING else str(column[i]) for key, column in pattern_columns.items()}
                for key, column in real_columns.items():
                    value_index = column[i]
                    if value_index != 0:  # Zero means no value
                        value = self.trie_value_maps[key].get(value_index, None)
                        value = rehydrate_message(value, pattern_values) if value else value

                        # Resolve dots to nested objects
                        [*parts, key] = key.split('.')
                        json_object_to_set = json_object
                        for part in parts:
                            if part not in json_object_to_set:
                                json_object_to_set[part] = {}
                            json_object_to_set = json_object_to_set[part]
                        json_object_to_set[key] = value
                json_objects.append(json_object)
            return json_objects

    def _build_trie_value_map(self, node, prefix, value_map):
        """Recursively build a hashmap of trie values."""
        if node.index is not None:
            value_map[node.index] = prefix
        for char, child in node.children.items():
            self._build_trie_value_map(child, prefix + char, value_map)


    def _read_trie(self, f, node, trie):
        """Recursively read a trie from a file."""
        while True:
          hit_terminator = self._read_child_trie(f, node, trie)
          if hit_terminator:
            break

    def _read_child_trie(self, f, node, trie):
        char = self._read_null_terminated_string(f)
        if not char:
            return True
        child_count = struct.unpack('>B', f.read(1))[0]
        node.children[char] = TrieNode()
        node.children[char].index = trie.current_index
        trie.current_index += 1
        for _ in range(child_count):
          self._read_child_trie(f, node.children[char], trie)
        return False

    def _resolve_trie_value(self, node, index):
        """Resolve a value from the trie given its index."""
        if node.index == index:
            return ''
        for char, child in node.children.items():
            value = self._resolve_trie_value(child, index)
            if value is not None:
                return char + value
        return None

    def _read_null_terminated_string(self, f):
        """Read a null-terminated UTF-8 string from a file."""
        string_bytes = bytearray()
        while True:
            char = f.read(1)
            if char in {b'\x00', b''}:  # Check for null terminator or EOF
                break
            string_bytes.extend(char)
        return string_bytes.decode('utf-8', errors='replace')  # Decode at once
