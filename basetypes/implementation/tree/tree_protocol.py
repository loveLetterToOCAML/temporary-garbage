from __future__ import annotations

from typing import Protocol, final


# this defines a list of properties a tree node must hold to be considered a tree node
class TreeNode(Protocol):

    @property
    def full_path(self) -> list:
        ...

    @property
    def root_node(self) -> TreeNode:
        ...

    @property
    def parent_node(self) -> TreeNode | None:
        ...

    @property
    def children_nodes(self) -> list[TreeNode]:
        ...

    @property
    def is_leaf(self) -> bool:
        ...

    @property
    @final
    def is_root(self) -> bool:
        return self.parent_node is None

    @property
    def depth(self) -> int:
        ...

    @property
    def data(self):
        ...


# mutable trees : functions to query and construct a partial tree
# searchable trees : functions to walk on a tree
# constraint & filter trees : functions to restrict walk
# visualization trees : function to easily grep and manipulate tree visualization
# mutable trees : function to easily mutate trees

class EnrichedTreeNode(TreeNode):

    def children_count(self):
        ...

    def children_nodes_gen(self):
        ...


class TreeNodeResolve(TreeNode):

    def resolve_data(self):
        ...

    def p(self):
        ...