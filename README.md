# Labox

[![PyPI - Version](https://img.shields.io/pypi/v/labox.svg)](https://pypi.org/project/labox)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A storage framework for heterogeneous data in Python.

## Todo

- decouple serialization dumper/loader logic and storage writer/reader logic
    - this will allow many to one relationships between dumpers/loaders and
        writers/readers
    - this would allow for multiple storage writers can specify that they use a common
        reader
    - for example, a file storage with multiple writers (e.g. different paths) ought
        to be able to use the same reader
