import os
import uuid

from utils import import_validator

UPLOAD_DIR = "/data/uploads"

def save_file(filename, content):
    name, ext = os.path.splitext(filename)
    if not ext:
        raise ValueError("File must have an extension")
    if ext.lower() not in ['.csv']:
        raise ValueError("Only CSV files are allowed")

    try:
        import_validator.validate(content)
    except Exception as e:
        raise ValueError(f"File validation failed: {str(e)}")

    new_filename = f"{name}_{uuid.uuid4().hex}{ext}"
    save_path = os.path.join(UPLOAD_DIR, new_filename)

    with open(save_path, "wb") as f:
        f.write(content)

    return save_path.replace("\\", "/")
