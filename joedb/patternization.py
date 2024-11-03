import re

def extract_pattern(log_message, root):
    # Extended regex patterns for timestamps, numbers, hex values, IPs, and floating-point numbers
    regex_patterns = {
        'timestamp': r'\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?\b',
        'number': r'\b\d+\b',
        'time': r'\b\d+s\b',
        'hex': r'\b(0x)?[0-9a-fA-F]+\b',
        'ip': r'\b(?:\d{1,3}\.){3}\d{1,3}\b'
    }
    
    variables = {}
    var_count = {'timestamp': 0, 'number': 0, 'hex': 0, 'ip': 0, 'time': 0}
    total_vars = 0  # Track total extracted variables

    # Step 1: Extract timestamps before tokenizing
    def replace_timestamp(match):
        nonlocal total_vars
        if total_vars >= 10:
            return match.group(0)  # Keep original text if variable limit reached
        
        var_name = f"var_{root}_{var_count['timestamp']}_timestamp"
        variables[var_name] = match.group(0)
        var_count['timestamp'] += 1
        total_vars += 1
        return f'{{{var_name}}}'

    log_message = re.sub(regex_patterns['timestamp'], replace_timestamp, log_message)
    
    # Step 2: Tokenize message based on remaining delimiters
    tokens = re.split(r'(\s+|[{}\[\](),;:\"\'=\-.])', log_message)
    
    pattern = []

    # Check each token for dynamic parts
    for token in tokens:
        if not token.strip():
            pattern.append(token)
            continue
        
        matched = False
        for var_type, regex in regex_patterns.items():
            if total_vars >= 10:
                continue
            
            if re.fullmatch(regex, token):
                var_name = f"var_{root}_{var_count[var_type]}_{var_type}"
                variables[var_name] = token
                pattern.append(f'{{{var_name}}}')
                var_count[var_type] += 1
                total_vars += 1
                matched = True
                break
        
        if not matched:
            pattern.append(token)

    pattern_str = ''.join(pattern)
    
    return pattern_str, variables


def rehydrate_message(pattern_str, variables):
    rehydrated_message = pattern_str
    for var_name, value in variables.items():
        rehydrated_message = rehydrated_message.replace(f'{{{var_name}}}', value) if value else rehydrated_message
    return rehydrated_message
