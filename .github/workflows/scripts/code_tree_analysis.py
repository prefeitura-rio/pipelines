# -*- coding: utf-8 -*-
import ast
from pathlib import Path
import sys
from typing import List, Union

import networkx as nx


def filename_to_python_module(filename: str) -> str:
    """
    Returns the Python module name from a filename.

    Example:

    - Filename:

    ```py
    path/to/file.py
    ```

    - Output:

    ```py
    'path.to.file'
    ```

    Args:
        filename (str): The filename to get the Python module name from.

    Returns:
        str: The Python module name.
    """
    # Get the file path in Python module format.
    file_path = Path(filename).with_suffix("").as_posix().replace("/", ".")

    return file_path


def python_module_to_filename(python_module: str) -> str:
    """
    Returns the filename from a Python module.

    Example:

    - Python module:

    ```py
    'path.to.file'
    ```

    - Output:

    ```py
    'path/to/file.py'
    ```

    Args:
        python_module (str): The Python module to get the filename from.

    Returns:
        str: The filename from the Python module.
    """
    # Get the file path in Python module format.
    file_path = Path(python_module).with_suffix("").as_posix().replace(".", "/")

    return f"{file_path}.py"


def get_dependencies(python_file: Union[str, Path]) -> List[str]:
    """
    Returns a list of dependencies from a Python file. The dependencies are
    defined as the import statements in the file. Their names on the output
    must be fully qualified.

    Example:

    - Python file:

    ```py
    from prefect import task
    from prefect.tasks.secrets import Secret
    from some_package import (
        func1, func2,
    )
    ```

    - Output:

    ```py
    ['prefect.task', 'prefect.tasks.secrets.Secret', 'some_package.func1', 'some_package.func2']
    ```

    Args:
        python_file (str): The Python file to get the dependencies from.

    Returns:
        list: A list of dependencies from the Python file.
    """
    # We need to get the contents of the Python file.
    with open(python_file, "r") as f:
        content = f.read()

    # Parse it into an AST.
    tree = ast.parse(content)

    # Then, iterate over the imports.
    dependencies = []
    for node in tree.body:
        if isinstance(node, ast.Import):
            for name in node.names:
                full_name = f"{name.name}"
                dependencies.append(full_name)
        elif isinstance(node, ast.ImportFrom):
            for name in node.names:
                full_name = f"{node.module}.{name.name}"
                dependencies.append(full_name)

    return dependencies


def get_declared(python_file: Union[str, Path]) -> List[str]:
    """
    Returns a list of declared variables, functions and classes
    in a Python file. The output must be fully qualified.

    Example:

    - Python file (path/to/file.py):

    ```py
    x = 1
    y = 2

    def func1():
        pass

    class Class1:
        pass
    ```

    - Output:

    ```py
    ['path.to.file.x', 'path.to.file.y', 'path.to.file.func1', 'path.to.file.Class1']
    ```

    Args:
        python_file (str): The Python file to get the declared variables from.

    Returns:
        list: A list of declared variables from the Python file.
    """
    # We need to get the contents of the Python file.
    with open(python_file, "r") as f:
        content = f.read()

    # Get file path in Python module format.
    file_path = filename_to_python_module(python_file)

    # Parse it into an AST.
    tree = ast.parse(content)

    # Then, iterate over the imports.
    declared = []
    for node in tree.body:
        # print(type(node))
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name):
                    declared.append(f"{file_path}.{target.id}")
        elif isinstance(node, ast.AugAssign):
            if isinstance(node.target, ast.Name):
                declared.append(f"{file_path}.{node.target.id}")
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name):
                declared.append(f"{file_path}.{node.target.id}")
        elif isinstance(node, ast.With):
            for item in node.items:
                if isinstance(item, ast.withitem):
                    if isinstance(item.optional_vars, ast.Name):
                        declared.append(f"{file_path}.{item.optional_vars.id}")
        elif isinstance(node, ast.FunctionDef):
            declared.append(f"{file_path}.{node.name}")
        elif isinstance(node, ast.AsyncFunctionDef):
            declared.append(f"{file_path}.{node.name}")
        elif isinstance(node, ast.ClassDef):
            declared.append(f"{file_path}.{node.name}")

    return declared


def list_all_python_files(directory: Union[str, Path]) -> List[Path]:
    """
    Returns a list of all Python files in a directory.

    Example:

    - Directory:

    ```py
    path/to/file1.py
    path/to/file2.py
    path/to/file3.py
    ```

    - Output:

    ```py
    ['path/to/file1.py', 'path/to/file2.py', 'path/to/file3.py']
    ```

    Args:
        directory (str): The directory to list the files from.

    Returns:
        list: A list of all Python files in the directory.
    """
    # Get the directory path.
    directory = Path(directory)

    # Get all files in the directory.
    files = directory.glob("**/*.py")

    # Filter out files that are not Python files.
    files = [f for f in files if f.suffix == ".py"]

    return [f.as_posix() for f in files]


def object_is_instance(fully_qualified_import: str, compare_to: type) -> bool:
    """
    Returns whether an object is an instance of a class.

    Args:
        fully_qualified_import (str): The fully qualified import to check.
        compare_to (type): The type to compare the import to.

    Returns:
        bool: Whether the object is an instance of the class.
    """
    # Get the module and class name.
    module, class_name = fully_qualified_import.rsplit(".", 1)

    # Import the module.
    module = __import__(module, fromlist=[class_name])

    # Get the object.
    object_ = getattr(module, class_name)

    # Check if the object is an instance of the class.
    return isinstance(object_, compare_to)


def assert_all_imports_are_declared(root_directory: str) -> None:
    """
    Asserts that all imports are declared somewhere.
    """
    # Get all Python files.
    files = [
        file_
        for file_ in list_all_python_files(root_directory)
        if "cookiecutter" not in file_
    ]

    # Get all declared stuff.
    declared = set()
    for file_ in files:
        file_declared = [
            item
            for item in get_declared(file_)
            if (item.startswith("pipelines") and not item.endswith("*"))
        ]
        declared.update(file_declared)

    # Get all dependencies.
    dependencies = set()
    for file_ in files:
        file_dependencies = [
            item
            for item in get_dependencies(file_)
            if (item.startswith("pipelines") and not item.endswith("*"))
        ]
        dependencies.update(file_dependencies)

    # Assert that all dependencies are declared.
    for dependency in dependencies:
        assert dependency in declared, f"{dependency} is not declared."


def build_dependency_graph(root_directory: str) -> nx.DiGraph:
    """
    Builds a dependency graph from a directory.

    Args:
        root_directory (str): The directory to build the graph from.

    Returns:
        nx.DiGraph: The dependency graph.
    """
    # Get all Python files.
    files = [
        file_
        for file_ in list_all_python_files(root_directory)
        if "cookiecutter" not in file_
    ]

    # Get dependencies by file.
    dependencies_by_file = {}
    for file_ in files:
        file_dependencies = set(
            [item for item in get_dependencies(file_) if item.startswith("pipelines")]
        )
        dependencies_by_file[file_] = file_dependencies

    # Get declared stuff by file.
    declared_by_file = {}
    for file_ in files:
        file_declared = set(get_declared(file_))
        declared_by_file[file_] = file_declared

    # Get all declared.
    all_declared = set()
    for file_ in files:
        file_declared = set(get_declared(file_))
        all_declared.update(file_declared)

    # Build the dependency graph.
    graph = nx.DiGraph()

    # First we add the dependencies. Each dependency neighbor is a file that
    # depends on it.
    for file_ in files:
        if file_ not in graph.nodes:
            graph.add_node(file_)
        for dependency in dependencies_by_file[file_]:
            if dependency.endswith("*"):
                for sub_dependency in all_declared:
                    if sub_dependency.startswith(dependency[:-1]):
                        if sub_dependency not in graph.nodes:
                            graph.add_node(sub_dependency)
                        graph.add_edge(sub_dependency, file_)
            else:
                if dependency not in graph.nodes:
                    graph.add_node(dependency)
                graph.add_edge(dependency, file_)

    # Then we add the declared stuff. Each file neighbor is a declared thing.
    for file_ in files:
        if file_ not in graph.nodes:
            graph.add_node(file_)
        for dependency in declared_by_file[file_]:
            if dependency not in graph.nodes:
                graph.add_node(dependency)
            graph.add_edge(file_, dependency)

    return graph


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <changed_files>")

    changed_files = sys.argv[1]
    print(f'Changed files: "{changed_files}"')
    # build_dependency_graph("pipelines/")
