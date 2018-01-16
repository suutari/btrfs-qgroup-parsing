#!/usr/bin/env python3.6
"""
Parser for "btrfs qgroup" output.

Use this script like this:

    btrfs qgroup show --raw -p / > data.txt
    btrfs sub list  / >> data.txt
    parse_qgroups.py < data.txt

The output of "btrfs qgroup show --raw -p" is like this:

    qgroupid         rfer         excl parent
    --------         ----         ---- ------
    0/5             16384        16384 ---
    0/257     16320008192  16320008192 ---
    0/258     20404953088  20404953088 255/258
    0/318       134246400      9453568 1/300

And the output of "btrfs sub list /" is like this:

    ID 257 gen 1128091 top level 5 path @
    ID 258 gen 1128091 top level 5 path @home
    ID 318 gen 1123728 top level 257 path var/lib/docker/btrfs/...
"""

import sys
from typing import (
    Dict, Iterable, Iterator, List, NamedTuple, NewType, Optional, Tuple,
    Union)


Size = int
QGroupId = NewType('QGroupId', str)
SubvolId = NewType('SubvolId', int)


class ParseError(Exception):
    pass


class SubvolEntry(NamedTuple):
    id: SubvolId
    path: str

    @classmethod
    def from_line(cls, line: str) -> 'SubvolEntry':
        try:
            items = line.split()
            if items[0] != 'ID':
                raise ValueError()
            subvol_id = SubvolId(int(items[1]))
            path = items[8]
        except (IndexError, ValueError):
            raise ParseError(f'Not a valid subvol entry: {line}')
        return cls(subvol_id, path)

    def __str__(self) -> str:
        return f'[{self.id}]{self.path}'


class QGroupEntry(NamedTuple):
    qgroupid: QGroupId
    rfer: Size
    excl: Size
    parent: Optional[QGroupId]

    @classmethod
    def from_line(cls, line: str) -> 'QGroupEntry':
        try:
            (qgroupid_str, rfer_str, excl_str, parent_str) = line.split()
            qgroupid = QGroupId(qgroupid_str)
            rfer = int(rfer_str)
            excl = int(excl_str)
            parent = QGroupId(parent_str) if parent_str != '---' else None
        except ValueError:
            raise ParseError(f'Not a valid qgroup line: {line}')
        return cls(qgroupid, rfer, excl, parent)

    @property
    def id(self) -> QGroupId:
        return self.qgroupid

    @property
    def subvol_id(self) -> Optional[SubvolId]:
        (id1, id2, _str_id) = self.get_sort_key()
        if id1 == 0:
            return SubvolId(id2)
        return None

    def __str__(self) -> str:
        return (
            f'{self.id}: '
            f'rfer={self.rfer/1000000:.1f}MB '
            f'excl={self.excl/1000000:.1f}MB')

    def get_sort_key(self) -> Tuple[int, int, QGroupId]:
        splitted = self.id.split('/', 1)
        if len(splitted) == 2:
            (a, b) = splitted
            if a.isdigit() and b.isdigit():
                return (int(a), int(b), self.id)
        return (-1, 0, self.id)


QGroupTreeItem = NamedTuple('QGroupTreeItem', [
    ('entry', QGroupEntry),
    ('level', int),
])


class QGroupTree:
    def __init__(self, entries: Iterable[QGroupEntry]) -> None:
        self.entries = {x.id: x for x in entries}
        self._children_map = self._make_children_map(self.entries.values())
        self.roots = self._children_map.get(None, [])

    @classmethod
    def _make_children_map(
            cls, entries: Iterable[QGroupEntry]
    ) -> Dict[Optional[QGroupId], List[QGroupEntry]]:
        result: Dict[Optional[QGroupId], List[QGroupEntry]]
        result = {}
        for entry in entries:
            result.setdefault(entry.parent, []).append(entry)
        for (key, values) in list(result.items()):
            result[key] = sorted(values, key=QGroupEntry.get_sort_key)
        return result

    def get_children(self, entry: QGroupEntry) -> Iterable[QGroupEntry]:
        return self._children_map.get(entry.id, [])

    def _walk(
            self, root: QGroupEntry, level: int = 0
    ) -> Iterable[QGroupTreeItem]:
        yield QGroupTreeItem(root, level)
        for child in self.get_children(root):
            yield from self._walk(child, level + 1)

    def __iter__(self) -> Iterator[QGroupTreeItem]:
        for root in self.roots:
            yield from self._walk(root)


def main() -> None:
    entries = list(parse_lines(sys.stdin))
    qgroup_entries = (x for x in entries if isinstance(x, QGroupEntry))
    subvolmap = {x.id: x for x in entries if isinstance(x, SubvolEntry)}
    tree = QGroupTree(qgroup_entries)

    for item in tree:
        indent = item.level * '    '
        subvol_id = item.entry.subvol_id
        subvol = subvolmap.get(subvol_id) if subvol_id else None
        subvol_path = getattr(subvol, 'path', '')
        if item.entry.rfer != 0:
            print(f'{indent}{item.entry} path={subvol_path}')


def parse_lines(
        lines: Iterable[str]
) -> Iterable[Union[QGroupEntry, SubvolEntry]]:
    for (line_num, raw_line) in enumerate(lines, 1):
        line = raw_line.strip()
        if line_num == 1:
            if line.split() != list(QGroupEntry._fields):
                raise ParseError(f'Invalid qgroup data header: {line}')
        elif line_num == 2:
            assert set(line) == set(['-', ' '])  # Dash line
        else:
            if line.startswith('ID'):
                yield SubvolEntry.from_line(line)
            else:
                yield QGroupEntry.from_line(line)


if __name__ == '__main__':
    main()
