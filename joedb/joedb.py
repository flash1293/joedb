import struct
from ppretty import ppretty
import pyzstd
import io


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

    def __init__(self):
        self.columns = {}
        self.tries = {}
        self.record_count = 0

    def insert(self, json_object):
        """Insert a JSON object into the database."""
        flat_data = flatten_json(json_object)
        keys = list(flat_data.keys())

        for key, value in flat_data.items():
            if key not in self.columns:
                self.columns[key] = []
                # append leading zeros in case there are records already
                self.columns[key].extend([0] * self.record_count)
                self.tries[key] = Trie()

            # Insert into trie and get the index
            index = self.tries[key].insert(value if isinstance(value, str) else str(value))
            self.columns[key].append(index)

        for key in self.columns:
            if not key in keys:
                self.columns[key].append(0)

        self.record_count += 1

    def encode(self, file_path):
        """Encode the database into a binary format and store it."""
        self.write_counter = 0
        # =============== PRINT:THE:TREE =================
        # print(ppretty(self.tries, depth=20))
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

            # Write the tries
            for key, trie in self.tries.items():
                f.write(key.encode('utf-8') + b'\x00')  # Null-terminated key
                compressed_data = io.BytesIO()
                with pyzstd.ZstdFile(filename=compressed_data, mode='wb') as gz:
                  self._write_trie(gz, trie.root)
                  gz.write(b'\x00')
                compressed_bytes = compressed_data.getvalue()
                f.write(struct.pack('>I', len(compressed_bytes)))  # Write 4-byte compressed length
                f.write(compressed_bytes)  # Write compressed data
                self.write_counter += compressed_data.tell()
                print("Wrote", compressed_data.tell(), "bytes for trie", key)
            f.write(b'\x00')  # End of tries marker

            print("Wrote", self.write_counter, "bytes for tries")

            # Write the columns with RLE
            for key, column in self.columns.items():
                self.write_counter = 0
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
                compressed_data = io.BytesIO()
                with pyzstd.ZstdFile(filename=compressed_data, mode='wb') as gz:
                    for value, length in rle_encoded:
                        gz.write(value.to_bytes(value_byte_size, byteorder='big'))
                        gz.write(length.to_bytes(length_byte_size, byteorder='big'))
                # Get the compressed data and write its length
                compressed_bytes = compressed_data.getvalue()
                f.write(struct.pack('>I', len(compressed_bytes)))  # Write 4-byte compressed length
                f.write(compressed_bytes)  # Write compressed data


#                for value, length in rle_encoded:
#                    f.write(value.to_bytes(value_byte_size, byteorder='big'))
#                    f.write(length.to_bytes(length_byte_size, byteorder='big'))
#                    self.write_counter += value_byte_size + length_byte_size
                print("Wrote", compressed_data.tell(), "bytes for column", key)

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
            self.trie_value_maps = {}
            while True:
                key = self._read_null_terminated_string(f)
                if not key:  # End of tries marker
                    break
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
            self.columns = {key: [] for key in self.tries}
            for key in self.tries:
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



            # Reconstruct JSON objects
            json_objects = []
            for i in range(record_count):
                json_object = {}
                for key, column in self.columns.items():
                    value_index = column[i]
                    if value_index != 0:  # Zero means no value
                        value = self.trie_value_maps[key].get(value_index, None)
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
