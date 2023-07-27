import os

from hypergo.storage import Storage


class LocalStorage(Storage):
    def load(self, file_name: str) -> str:
        with open(file_name, "r", encoding="utf-8") as file:
            content: str = file.read()
        return content

    def save(self, file_name: str, content: str) -> None:
        def create_folders_for_file(file_path: str) -> None:
            directory: str = os.path.dirname(file_path)
            try:
                os.makedirs(directory)
            except OSError as error:
                if not os.path.isdir(directory):
                    raise error

        create_folders_for_file(file_name)

        with open(file_name, "w", encoding="utf-8") as file:
            file.write(content)
