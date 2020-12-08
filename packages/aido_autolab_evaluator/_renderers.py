import math
import os
from typing import Optional


class ScreenTableRenderer:

    def __init__(self, rows: int, cols: int, cell_w: Optional[int] = None,
                 cell_h: Optional[int] = None):
        self._rows = rows
        self._cols = cols
        self._cell_w = cell_w
        self._cell_h = cell_h
        self._content = [["" for _j in range(self._cols)] for _i in range(self._rows)]

    def add_cell(self, i: int, j: int, content: str):
        if not (0 <= i < self._rows):
            raise ValueError(f"Out-of-bounds: 0 <= i < {self._rows}, {i} was given instead.")
        if not (0 <= j < self._cols):
            raise ValueError(f"Out-of-bounds: 0 <= j < {self._cols}, {j} was given instead.")
        # ---
        self._content[i][j] = content

    def render(self):
        tsize = os.get_terminal_size()
        cell_w = self._cell_w or int(math.floor(tsize.columns / self._cols) - 1)
        cell_h = self._cell_h or int(math.floor(tsize.lines / self._rows) - 1)
        screen_w = cell_w * self._cols + 1
        screen_h = cell_h * self._rows + 1
        buffer = [[" " for _j in range(screen_w)] for _i in range(screen_h)]

        # draw horizontal lines between cells
        for i in range(0, screen_h + 1, cell_h):
            for j in range(screen_w):
                buffer[i][j] = '-'
        # draw vertical lines between cells
        for j in range(0, screen_w + 1, cell_w):
            for i in range(screen_h):
                buffer[i][j] = '|'

        # draw content
        for r in range(self._rows):
            for c in range(self._cols):
                block = self._content[r][c]
                lines = block.split('\n')
                # cut the content vertically
                lines = lines[:cell_h - 1]
                # cut the content horizontally
                lines = [line[-(cell_w - 1):] for line in lines]
                # draw
                for _i, line in enumerate(lines):
                    for _j, char in enumerate(line):
                        i, j = r * cell_h + 1 + _i, c * cell_w + 1 + _j
                        buffer[i][j] = char

        s = ""
        for i in range(screen_h):
            for j in range(screen_w):
                s += buffer[i][j]
            s += '\n'

        print(s, end="")
