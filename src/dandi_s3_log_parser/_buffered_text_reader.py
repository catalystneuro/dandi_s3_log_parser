import pathlib


class BufferedTextReader:
    def __init__(self, *, file_path: str | pathlib.Path, maximum_ram_usage_in_bytes: int = 10**9):
        """
        Lazily read a text file into RAM using buffers of a specified size.

        Parameters
        ----------
        file_path : string or pathlib.Path
            The path to the text file to be read.
        maximum_ram_usage_in_bytes : int, default: 1 GB
            The theoretical maximum amount of RAM (in bytes) to be used by the BufferedTextReader object.
        """
        self.file_path = file_path
        self.maximum_ram_usage_in_bytes = maximum_ram_usage_in_bytes

        # The actual amount of bytes to read per iteration is 3x less than theoretical maximum usage
        # due to decoding and handling
        self.buffer_size_in_bytes = int(maximum_ram_usage_in_bytes / 3)

        self.total_file_size = pathlib.Path(file_path).stat().st_size
        self.offset = 0

    def __iter__(self):
        return self

    def __next__(self) -> list[str]:
        """Retrieve the next buffer from the file, or raise StopIteration if the file is exhausted."""
        if self.offset >= self.total_file_size:
            raise StopIteration

        with open(file=self.file_path, mode="rb", buffering=0) as io:
            io.seek(self.offset)
            intermediate_bytes = io.read(self.buffer_size_in_bytes)
        decoded_intermediate_buffer = intermediate_bytes.decode()
        split_intermediate_buffer = decoded_intermediate_buffer.splitlines()

        # Check if we are at the end of the file
        if len(intermediate_bytes) < self.buffer_size_in_bytes:
            self.offset = self.total_file_size
            return split_intermediate_buffer

        buffer = split_intermediate_buffer[:-1]
        last_line = split_intermediate_buffer[-1]

        if len(buffer) == 0 and last_line != "":
            raise ValueError(
                f"BufferedTextReader encountered a line at offset {self.offset} that exceeds the buffer "
                "size! Try increasing the `buffer_size_in_bytes` to account for this line."
            )

        # The last line split by the intermediate buffer may or may not be incomplete
        if decoded_intermediate_buffer.endswith("\n"):
            # By chance, this iteration finished on a clean line break
            self.offset += self.buffer_size_in_bytes
        else:
            self.offset += self.buffer_size_in_bytes - len(last_line.encode("utf-8"))

        return buffer
