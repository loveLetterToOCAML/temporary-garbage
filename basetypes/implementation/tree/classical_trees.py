from pydantic import BaseModel

from basetypes.a_root_params import RootSerial
from basetypes.implementation.basetypes_constraints import IntConstraintType
from basetypes.implementation.generics_match import SimpleTree, PartialTreeNode, IntIndexedString, IntIndexedData

from typing import TypeVar


StringTree = SimpleTree[str]
PartialStringTree = SimpleTree[PartialTreeNode[str]]


T = TypeVar('T')

PartiallyOrderedStringTree = SimpleTree[PartialTreeNode[IntIndexedString]]  # no need for OrderedStringTree since these are supposed to be complete
PartiallyOrderedTree = SimpleTree[PartialTreeNode[IntIndexedData[T]]]


# hash tree
# constraint tree


class AtomicIntSuperior(RootSerial):
    min: int
    equalsTolerated: bool

class AtomicIntInferior(RootSerial):
    max: int
    equalsTolerated: bool

class AtomicIntEquals(RootSerial):
    value: int

class OrConstraint(RootSerial, Generic[T]):
    constraints: list[Constraint[T] | AndConstraint[T]]

class AndConstraint(RootSerial, Generic[T]):
    constraints: list[Constraint[T] | OrConstraint[T]]

class NodeTreeConstraint(RootSerial):
    numberOfChildren: IntConstraint
    minDepth: IntConstraint
    maxDepth: IntConstraint
    fullPath: StringConstraint

class NodePartialTreeConstraint(NodeTreeConstraint):
    isComplete: BoolConstraint
    numberOfCompleteChildren: IntConstraint
    numberOfIncompleteChildren: IntConstraint
    totalRecursiveChildren: IntConstraint
    maximumDepthFromThere: IntConstraint


class SimpleTreeConstraint(RootSerial, Generic[T]):
    treeConstraint: NodeTreeConstraint
    perNodeConstraint: Constraint[T]


class SimplePartialTreeConstraint(RootSerial, Generic[T]):
    treeConstraint: NodePartialTreeConstraint
    perNodeConstraint: Constraint[T]


ConstraintTree = SimpleTree[SimpleTreeConstraint[T]]
ConstraintPartialTree = SimpleTree[SimplePartialTreeConstraint[T]]
