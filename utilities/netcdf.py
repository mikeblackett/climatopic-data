import os
import xarray as xr


def list_files(directory, recursive=False, path=False):
    """Returns a list of all the .nc files in a directory"""
    assert os.path.isdir(directory)
    nc_files = []
    for dirpath, dirnames, filenames in os.walk(directory):
        if not recursive:
            dirnames.clear()
        for file in filenames:
            if os.path.splitext(file)[1] == ".nc":
                if path:
                    nc_files.append(os.path.join(dirpath, file))
                else:
                    nc_files.append(file)
    return nc_files
