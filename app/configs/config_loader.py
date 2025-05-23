import os
import yaml

def load_config(config_path, project_base=None, use_hdfs=False):
    if project_base is None:
        project_base = os.path.dirname(os.path.dirname(os.path.abspath(config_path)))

    with open(config_path) as f:
        config = yaml.safe_load(f)

    for group in ['data', 'model']:
        for key in config.get(group, {}):
            config[group][key] = to_spark_path(config[group][key], project_base, use_hdfs=use_hdfs)

    return config


def to_spark_path(path, base_dir, use_hdfs=False, hdfs_host="hadoop-master", hdfs_port=9000):
    if path.startswith("hdfs://") or path.startswith("file://"):
        return path
    if os.path.isabs(path):
        return f"file://{path}" if not use_hdfs else f"hdfs://{hdfs_host}:{hdfs_port}{path}"

    full_path = os.path.normpath(os.path.join(base_dir, path))
    if use_hdfs:
        relative = os.path.relpath(full_path, base_dir)
        return f"hdfs://{hdfs_host}:{hdfs_port}/" + relative.replace("\\", "/")
    else:
        return f"file://{full_path}"

