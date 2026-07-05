from baseimplems.persistence.model_utils.high_order_sqlalchemy_registry import register_sqlalchemy_type, \
    default_sqlalchemy_classname_keying
from baseimplems.persistence.model_utils.model_utils_common import TWithID, WithUniqueName, WithID
from baseimplems.persistence.model_utils.model_utils_time import CreatedModifiedAt
from baseimplems.persistence.mixins import BaseMixins

from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship, declared_attr, aliased, mapped_column


__objectname__ = "TREE"


@register_sqlalchemy_type(__objectname__, default_sqlalchemy_classname_keying)
def NAMED_DATE_METADATA(metadata_base_type: str):
    table_name = f'META_NAMED_DATE<{metadata_base_type}>'

    attrs = {
        "__tablename__": table_name,
    }

    bases = (WithID, WithUniqueName, CreatedModifiedAt, *BaseMixins)
    return type(attrs['__tablename__'], bases, attrs)


@register_sqlalchemy_type(__objectname__, default_sqlalchemy_classname_keying)
def TREE(NodeAlchemyBaseType: TWithID, LeafAlchemyBaseType: TWithID | None = None, MetadataType: TWithID | None = None,
         unique_children=False, unique_nodes=False):
    if not MetadataType:
        MetadataType = NAMED_DATE_METADATA(__objectname__)

    LeafType = LeafAlchemyBaseType if LeafAlchemyBaseType else NodeAlchemyBaseType

    entrytype_tablename = f"{__objectname__}<{MetadataType.__slug__ if hasattr(MetadataType, '__slug__') else MetadataType.__tablename__}," \
                          f"{','.join([NodeAlchemyBaseType.__tablename__, *([LeafAlchemyBaseType.__tablename__] if LeafAlchemyBaseType else [])])}>"

    class EntryTemplate(WithID):

        __tablename__ = entrytype_tablename
        __nodetype__ = NodeAlchemyBaseType
        __leaftype__ = LeafType

        @declared_attr
        def node_id(cls):
            return mapped_column(Integer, ForeignKey(NodeAlchemyBaseType.id))

        @declared_attr
        def leaf_id(cls):
            return mapped_column(Integer, ForeignKey(LeafType.id))

        @declared_attr
        def metadata_id(cls):
            return mapped_column(Integer, ForeignKey(MetadataType.id), nullable=False)

        if unique_nodes:
            @declared_attr
            def __table_args__(cls):
                return (UniqueConstraint('node_id', 'metadata_id',
                                         name=f"unique_nodes_"
                                              f"{NodeAlchemyBaseType.__tablename__}_{LeafType.__tablename__}"),
                        UniqueConstraint('leaf_id', 'metadata_id',
                                         name=f"unique_leafs_"
                                              f"{NodeAlchemyBaseType.__tablename__}_{LeafType.__tablename__}"),
                        )
        elif unique_children:
            @declared_attr
            def __table_args__(cls):
                return (UniqueConstraint('parent_id', 'node_id', 'metadata_id',
                                         name=f"unique_children_nodes_"
                                              f"{NodeAlchemyBaseType.__tablename__}_{LeafType.__tablename__}"),
                        UniqueConstraint('parent_id', 'leaf_id', 'metadata_id',
                                         name=f"unique_children_leafs_"
                                              f"{NodeAlchemyBaseType.__tablename__}_{LeafType.__tablename__}"),
                        )

        @declared_attr
        def parent_id(cls):
            return mapped_column(Integer, ForeignKey(cls.id), nullable=True)

        @declared_attr
        def parent(cls):
            return relationship(cls, remote_side=[cls.id])

        @declared_attr
        def node(cls):
            return relationship(NodeAlchemyBaseType, foreign_keys=[cls.node_id])

        @declared_attr
        def leaf(cls):
            return relationship(LeafType, foreign_keys=[cls.leaf_id])

        @declared_attr
        def metadata_instance(cls):
            return relationship(MetadataType, foreign_keys=[cls.metadata_id])

        def is_root(self):
            return self.parent_id == 0 or self.parent_id == self.id or self.parent_id is None

        def __repr__(self):
            if not self.node and not self.leaf:
                if self.is_root():
                    return f"Current node {self.id} is root -> {self.children()}"
                return f"Bad tree entry: No node nor leaf for current tree entry [{self.id}]"
            elif self.node and self.leaf:
                return f"Bad tree entry: Current tree entry [{self.id}] both node and leaf: [{self.node}], `{self.leaf}`"
            to_print = self.node if self.node else self.leaf
            if to_print:
                return f"{self.id} = {self.node} -> {self.children()}"

        def set_tree(self, tree):
            self.tree = tree

        def children(self):
            return self.tree.children_of(self)

        def add_child(self, node: NodeAlchemyBaseType):
            return self.tree.add_child(self, node)

        def add_leaf(self, leaf: LeafType):
            return self.tree.add_leaf(self, leaf)

        def delete_node_and_children(self, allow_root=False):
            return self.tree.delete_from(self, allow_root=allow_root)

    bases = (EntryTemplate, *BaseMixins)
    _TREE_ENTRY = type(entrytype_tablename, bases, {})

    class _TREE:

        __entrytype__ = _TREE_ENTRY
        __tablename__ = entrytype_tablename  # not a real SQLAlchemy table but to ease table creation
        __nodetype__ = NodeAlchemyBaseType
        __leaftype__ = LeafType

        def __init__(self, session, *args, **argv):
            if args:
                assert (type(args[0]) == self.__metadataclass__)  # comes from baseclass_for_metadata
                self.metadata = args[0]
            else:
                self.metadata = MetadataType.get_create(session, **argv)
            self.entries_initialized = False
            self._entries = []
            self._entries_per_id = {}
            self._roots = []
            self._tree = {}
            self._session_saved = session

        # lazy load list entries when needed, otherwise only the metadata suffices
        def load_fulltree(self, session=None):
            if not self.entries_initialized:
                self._entries = (session if session else self._session_saved) \
                    .query(_TREE_ENTRY, _TREE_ENTRY.node, _TREE_ENTRY.leaf, _TREE_ENTRY.parent) \
                    .outerjoin(NodeAlchemyBaseType, _TREE_ENTRY.node_id == NodeAlchemyBaseType.id)
                if LeafType:
                    leafalias = aliased(LeafType)
                    self._entries = self._entries.outerjoin(leafalias, _TREE_ENTRY.leaf_id == leafalias.id)
                self._entries = self._entries.filter(_TREE_ENTRY.metadataobj == self.metadata).all()
                for entry, _, _, _ in self._entries:
                    entry.set_tree(self)
                    self._tree.setdefault(entry.parent_id, []).append(entry)
                    self._tree.setdefault(entry.id, [])
                    self._entries_per_id[entry.id] = entry
                    if entry.is_root():
                        self._roots.append(entry)
                self.entries_initialized = True
            return self._tree

        def get_roots(self):
            self.load_fulltree()
            return self._roots

        def get_root(self, create=True):
            self.get_roots()
            if not self._roots and create:
                assert session or self._session_saved, f"Cannot get root for {self.metadata} nor create it as no session is provided"
                root = _TREE_ENTRY(metadataobj=self.metadata, parent=None)
                (session if session else self._session_saved).add(root)
                (session if session else self._session_saved).commit()
                root.set_tree(self)
                self._tree.setdefault(root.id, [])
                self._roots = [root]
            else:
                root = self._roots[0]
            assert len(self._roots) == 1, f"No (or multiple) root detected for tree {self.metadata}"
            return root

        def get_from(self, index):
            self.load_fulltree()
            return self._entries_per_id[index]

        def add_child(self, node: _TREE_ENTRY, child_node: NodeAlchemyBaseType):
            self.load_fulltree()
            child = _TREE_ENTRY(metadataobj=self.metadata, parent_id=node.id, node=child_node)
            (session if session else self._session_saved).add(child)
            (session if session else self._session_saved).commit()
            child.set_tree(self)
            self._tree.setdefault(node.id, []).append(child)
            self._tree.setdefault(child.id, [])
            return child

        def add_leaf(self, node: _TREE_ENTRY, child_leaf: LeafType):
            self.load_fulltree()
            child = _TREE_ENTRY(metadataobj=self.metadata, parent_id=node.id, leaf=child_leaf)
            (session if session else self._session_saved).add(child)
            (session if session else self._session_saved).commit()
            child.set_tree(self)
            self._tree.setdefault(node.id, []).append(child)
            self._tree.setdefault(child.id, [])
            return child

        def children_of(self, node: _TREE_ENTRY):
            return self._tree[node.id]

        def delete_from(self, node: _TREE_ENTRY, allow_root=False):
            cur_entries = [node]
            while cur_entries:
                entry = cur_entries.pop(0)
                session.delete(entry)
                cur_entries.extend(self._tree[entry.id])
                if not entry.is_root() or allow_root:
                    del self._tree[entry.id]
            if not entry.is_root() or allow_root:
                self._tree[node.parent_id] = [c for c in self._tree[node.parent_id] if c.id != node.id]
            else:
                self._tree[node.id] = []
            session.commit()

        def __repr__(self):
            roots = self.get_roots()
            return '\n'.join(map(repr, roots))

    return _TREE


if __name__ == "__main__":
    from ....persistent_to_disk import create_session
    from ..base_type import BasicEntity, STRING_SIZE

    from sqlalchemy import String

    columns = {
        "id": Column(Integer, primary_key=True),
        "value": Column(String(STRING_SIZE), unique=True),
        "ADD": Column(Integer, default=666),
    }

    Test = BasicEntity("bak2basics", columns)

    session = create_session()

    v1 = Test.GET_CREATE(session, value="bonjour1")
    v2 = Test.GET_CREATE(session, value="bonjour2")
    v3 = Test.GET_CREATE(session, value="bonjour3")
    v4 = Test.GET_CREATE(session, value="bonjour4")

    TREE_TYPE = TREE(Test)

    T = TREE_TYPE(session, name="firsttree")

    root = T.get_root()
    print(root)

    child = root.add_child(v1)
    print(child)

    child.add_leaf(v2)
    child2 = child.add_child(v3)

    child2.add_leaf(v4)
    child2.add_leaf(v1)

    print(root)

    T.delete_from(root)
    print(root)
