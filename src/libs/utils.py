import re


def getext(file_name):
    pattern = r'^(.*?)(\.[a-zA-Z0-9]+)$'
    match = re.search(pattern, file_name)
    if match:
        extension = match.group(1)
        file_name = match.group(2)
        return (file_name, extension)
    else:
        return None

