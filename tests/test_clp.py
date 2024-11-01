from joedb.clp import extract_pattern


def test_extract_simple_log_message():
    log_message = "2023-10-01 12:34:56 Process started with ID 123 and memory 0x1a2b3c"
    expected_pattern = "{var_timestamp0} Process started with ID {var_number0} and memory {var_hex0}"
    expected_variables = {
        "var__timestamp0": "2023-10-01 12:34:56",
        "var__number0": "123",
        "var__hex0": "0x1a2b3c"
    }
    
    pattern, variables = extract_pattern(log_message, '')
    assert pattern == expected_pattern, f"Expected pattern: {expected_pattern}, but got: {pattern}"
    assert variables == expected_variables, f"Expected variables: {expected_variables}, but got: {variables}"

def test_extract_log_message_with_ip_and_float():
    log_message = "Connection from 192.168.1.1 at 2023-10-01 12:34:56 with load 0.75"
    expected_pattern = "Connection from {var_ip0} at {var_timestamp0} with load {var_float0}"
    expected_variables = {
        "var__ip0": "192.168.1.1",
        "var__timestamp0": "2023-10-01 12:34:56",
        "var__float0": "0.75"
    }
    
    pattern, variables = extract_pattern(log_message, '')
    assert pattern == expected_pattern, f"Expected pattern: {expected_pattern}, but got: {pattern}"
    assert variables == expected_variables, f"Expected variables: {expected_variables}, but got: {variables}"

def test_extract_simple_string_with_number():
    log_message = "123 Hi"
    expected_pattern = "{var_number0} Hi"
    expected_variables = {
        "var__number0": "123"
    }
    
    pattern, variables = extract_pattern(log_message, '')
    assert pattern == expected_pattern, f"Expected pattern: {expected_pattern}, but got: {pattern}"
    assert variables == expected_variables, f"Expected variables: {expected_variables}, but got: {variables}"
