## Release Notes - Version X.Y.Z (Replace X.Y.Z with the actual version number)

### New Features:
- Added a new `decorator refactor` task to improve the decorator system.
- Implemented an SDK file and message viewer for handling compressed, encrypted, and serialized messages.
- Added a new `passbyref` decorator for pass-by-reference functionality.
- Introduced a `clean up utility class` task to enhance code maintenance.
- Improved graph handling by addressing `xenon complexity issues`.
- Grouped similar types into subgraphs for better organization.
- Introduced support for wildcard configurations to enable more flexible setups.
- Added the ability to display payloads or bindings for better visibility.

### Bug Fixes:
- Resolved an issue in `hypergo_cli.py` that caused a `BrokenPipeError` when no input message was piped through stdin.

### Behavior Changes:
- Refactored the `hypergo/executor.py` file to improve code structure and readability.
- Changed the `execute` method in the `Executor` class to utilize the `Transform.pass_by_reference` decorator for pass-by-reference functionality.
- Updated the `compression` decorator in the `Transform` class to handle input and output compression based on the configuration.

### Miscellaneous:
- Performed code cleanup and made various improvements throughout the codebase for better maintainability.

**Note:** The above release notes are based on the inferred changes from the provided Git diff. The actual changes and their impact may vary. Please review the code and changes for a more detailed understanding of the release.

---

## Release Notes - Version X.Y.Z (Replace X.Y.Z with the actual version number)

### New Features

- Added **compression** decorator to the `hypergo/transform.py` module.
  - This decorator allows data compression before serialization to optimize storage and transmission.
  - Usage: `@Transform.compression("key")` applied to a function that returns a generator.
  - Data is compressed using LZMA and then encoded in Base64 for efficient storage and decoding.
  - When decompressing, the data is decoded from Base64 and then decompressed using LZMA.
  - Example usage:
    ```python
    @Transform.compression("body")
    @Transform.serialization
    def execute(self, input_envelope: MessageType) -> Generator[MessageType, None, None]:
        # Function code...
    ```

### Changes

- Updated the `hypergo/BACKLOG.md` file to mark tasks as completed in **PHASE 1**:
  - The **decorators** task is now marked as completed.
  - The sub-task **serialization** under **decorators** is also marked as completed.

### Bug Fixes

- None

### Improvements

- None

### Deprecated

- None

### Removed

- None

### Security

- None

### Internal

- None

---

## Release Notes

### Features

- Added `serialization` method to the `Transform` class in `hypergo/transform.py`. The `serialization` method takes a callable `func` and returns a generator that serializes and deserializes data using the `Utility.serialize` and `Utility.deserialize` functions. This allows for easy serialization of data for message passing.

### Bug Fixes

- None

### Possible Future Enhancements

1. **Improved Type Annotations**: Consider adding more precise type annotations to the methods and functions in the codebase, especially for the input parameters and return values. This can enhance code readability and enable better static type checking with tools like `mypy`.

2. **Unit Tests**: Introduce comprehensive unit tests to ensure the correctness of the code and improve its maintainability. Test the edge cases and various scenarios to catch potential bugs early and avoid regressions in future changes.

3. **Serialization for More Data Types**: Extend the serialization and deserialization capabilities of `Utility.serialize` and `Utility.deserialize` functions to support a wider range of data types. This can include support for custom classes and objects.

4. **Error Handling**: Enhance error handling in the codebase to provide informative error messages and handle potential exceptions more gracefully. This can improve the overall robustness and user experience.

### Additional Notes

- Updated `hypergo/utility.py` to improve serialization and deserialization functions. The `Utility.serialize` function now handles additional data types, including `None`, `bool`, `int`, `float`, and `str`. It also includes a new `traverse_datastructures` decorator to handle serialization of dictionaries, lists, and tuples recursively.
- The `Utility.deserialize` function has been improved to handle potential decoding and unpickling errors and gracefully return the input `serialized` data if an error occurs.

These release notes highlight the changes made in the codebase and provide suggestions for possible future enhancements. Incorporating these enhancements can lead to improved code quality, maintainability, and user experience.

---

## Release Notes - Version X.Y.Z

### New Features and Enhancements
- Added a new utility function `traverse_datastructures` in the `hypergo.utility` module. This function allows traversing through nested data structures such as dictionaries and lists and applies a transformation function on each element, enabling easier serialization and deserialization of complex data structures.

### Changes to Existing Functionality
- Modified the `hypergo.transform.Transform.serialization` method to use the new `traverse_datastructures` utility function for serialization, improving the serialization process for nested data structures.

### Bug Fixes
- Fixed an issue in the `hypergo.utility.deserialize` function, where binary data and bytes were not being deserialized correctly. The function now properly deserializes binary data and bytes.

### Testing
- Expanded the test suite in `tests.test_utility_serialization.py` to include comprehensive tests for various data types and nested data structures.
- Added a new test method `test_comprehensive_dict` in `tests.test_utility_serialization.py`, which tests the serialization and deserialization of a comprehensive dictionary containing various data types, nested dictionaries, lists, functions, classes, and instances.
- The test suite now achieves higher code coverage and ensures more robustness of the serialization and deserialization process.

### Miscellaneous
- Updated the `hypergo.utility.serialize` and `hypergo.utility.deserialize` methods to handle edge cases and improve the overall reliability of the serialization and deserialization process.

### Deprecations and Removals
- None.

### Internal Refactorings
- Internal optimizations and code refactorings for improved maintainability and performance.

### Known Issues
- None.

### Contributors
- Thanks to [Contributor Name](mailto:contributor@example.com) for their valuable contributions to this release.

### Note
- Please update your code to use the new `traverse_datastructures` utility function for serialization and deserialization of nested data structures to ensure compatibility with the latest version.

---

# Release Notes - Version X.Y.Z

## New Features

- Added `sdk unit tests` decorator which automates testing in the linter.
- Added more decorators for `validation`, `compression`, and `encryption`.

## Changes

- The `Executor` class in `hypergo/executor.py` now includes a new method `execute` with the `Transform.serialization` decorator, allowing for serialization of function objects.

## Dependencies

- Added `dill` as a new dependency.

Please note that this is just a summary of the changes made. For more detailed information, you can refer to the individual files and their corresponding commits.

# Release Notes

## Version X.Y.Z (Date)

### Bug Fixes

- Fixed a bug in `hypergo/config.py` that caused an issue with importing the `NotRequired` type.
- Fixed a bug in `hypergo/executor.py` where input bindings were not formatted correctly.

### Removed Code

- Removed the `Config` class from `hypergo/config.py`.
- Removed the `load_configs` function from `hypergo/graph.py`.

### Refactoring

- Refactored the `ConfigType` class in `hypergo/config.py` to use the `TypedDictType` from `hypergo/custom_types.py`.
- Refactored the `from_yaml` and `from_json` methods in `Config` class from `hypergo/config.py`.
- Refactored the `graph` function in `hypergo/graph.py` to load multiple configuration files.

### Miscellaneous

- Updated dependencies: `typing_extensions` to `NotRequired` in `hypergo/config.py`.
- Improved error handling in `hypergo/hypergo_cli.py` when executing commands.

### Removed Unused Imports

- Removed unused imports from `hypergo/config.py`.
- Removed unused imports from `hypergo/hypergo_cli.py`.
- Removed unused imports from `hypergo/hypergo_click.py`.

### Updated CLI Output

- Updated the CLI output in `hypergo/hypergo_cli.py` to show version information and prompt.

### Performance Improvements

- Improved performance in `hypergo/hypergo_cmd.py` by handling recognized commands efficiently.

### Documentation Updates

- Updated documentation in `hypergo/config.py` to reflect changes and add missing comments.
- Updated the `graph` function documentation in `hypergo/graph.py`.
- Updated documentation in `hypergo/message.py`.
- Updated the `consume` method documentation in `hypergo/stdio_connection.py`.

### Code Clean-Up

- Cleaned up unused code and commented-out code from several files.


---

# Release Notes

## Version 1.1.0

### HypergoCli Class Updates:
- Added the `stdio` method to the `HypergoCli` class to handle standard input/output.
- Removed the `format_date`, `get_version_path`, `load_config`, `use`, and `start` methods from the `HypergoCli` class.

### HypergoCmd Class Updates:
- Removed the handling of stdin input from the `HypergoCmd` class.

### hypergo_cli.py Changes:
- Removed the `yaml` import statement, as it is no longer used.
- Added the `json` import statement to support the loading of a JSON configuration.
- Moved the `format_date` and `get_version_path` functions inside the `intro` property getter.
- The `intro` property now displays the version and timestamp of the Hypergo tool.

### hypergo_click.py Changes:
- Renamed the `run` command to `stdio`.
- Removed the `init`, `use`, and `start` commands from the `hypergo_click.py` module.

### hypergo_cmd.py Changes:
- Removed the handling of stdin input from the `HypergoCmd` class.

Note: This diff seems to be a refactor of the Hypergo tool's CLI and input/output handling. The `stdio` method has been added to the `HypergoCli` class to handle standard input/output. Several methods, such as `format_date`, `get_version_path`, `load_config`, `use`, and `start`, have been removed or refactored to support the changes. Additionally, stdin input handling has been removed from various parts of the codebase.

Please note that this release includes internal changes and improvements for the Hypergo tool, making it more robust and efficient. Users of Hypergo can now interact with the tool using standard input/output, and the `HypergoCli` class has been streamlined for better maintainability.

---

### Release Notes - Version X.X.X

#### New Features
- Added a new command-line interface module: `hypergo/hypergo_cli.py`.
  - The `HypergoCli` class provides functionality for interacting with the hypergo system via the command line.
  - Includes commands such as `run`, `use`, `init`, `start`, and `graph` to perform various operations within the system.
- Introduced the `hypergo/hypergo_click.py` module.
  - This module contains a click-based command-line interface for the hypergo system.
  - New commands such as `shell`, `run`, `use`, `init`, `start`, and `graph` are available for easier interaction.

#### Bug Fixes
- Fixed an issue in the `hypergo/hypergo_cmd.py` module, where the `do_exit` method was not properly implemented. It now correctly exits the command-line interface.

#### Dependencies
- Added the following new dependencies in `setup.cfg`:
  - `click`: A Python package for creating command-line interfaces easily.
  - `ansicolors`: A package for adding colored output to the command-line interface.
  - `click-default-group`: A package for setting a default command in click-based command-line interfaces.
  - `graphviz`: A package for working with Graphviz graph visualization software.
  - `urllib3<2.0`: An updated version of urllib3 to address potential compatibility issues.

#### Miscellaneous
- Updated the `install_requires` section in `setup.py` to include the newly added dependencies.

---
Please note that this release may contain additional undocumented changes, improvements, or bug fixes.