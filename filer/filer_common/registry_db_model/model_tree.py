from __future__ import annotations

from baseimplems.persistence.model_utils.high_order_sqlalchemy_registry import register_sqlalchemy_type, \
    default_sqlalchemy_classname_keying
from baseimplems.persistence.model_utils.model_utils_common import TWithID, WithUniqueName, WithID
from baseimplems.persistence.mixins import BaseMixins, commit_and_rollback_if_exception
from baseimplems.persistence.sqlalchemy_database import with_current_session_kwargs
from baseimplems.persistence.model_utils.model_utils_time import CreatedModifiedAt
from baseimplems.anyio_utils import NotInAsyncContextManager

from sqlalchemy.orm import relationship, declared_attr, aliased, mapped_column, Mapped
from sqlalchemy import Integer, ForeignKey, UniqueConstraint, select
from sqlalchemy.ext.asyncio import AsyncSession
from anyio import AsyncContextManagerMixin

from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import AsyncIterator


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
                return f"{self.id} = {to_print} -> {self.children()}"

        def set_tree(self, tree):
            self.tree = tree

        def children(self):
            return self.tree.children_of(self)

        async def add_child(self, node: NodeAlchemyBaseType):
            return await self.tree.add_child(self, node)

        async def add_leaf(self, leaf: LeafType):
            return await self.tree.add_leaf(self, leaf)

        async def delete_node_and_children(self, allow_root=False):
            return await self.tree.delete_from(self, allow_root=allow_root)

    bases = (EntryTemplate, *BaseMixins)
    _TREE_ENTRY = type(entrytype_tablename, bases, {})

    class _TREE(AsyncContextManagerMixin):

        __entrytype__ = _TREE_ENTRY
        __tablename__ = entrytype_tablename  # not a real SQLAlchemy table but to ease table creation
        __nodetype__ = NodeAlchemyBaseType
        __leaftype__ = LeafType

        def __init__(self, metadata: MetadataType | None = None, **kwargs):
            if metadata:
                self.metadata = metadata
                self._metadata_kwargs = None
            else:
                self.metadata = None
                self._metadata_kwargs = kwargs
            self._entries_initialized = False
            self._entries = []
            self._entries_per_id = {}
            self._roots = []
            self._tree = {}

        # lazy load list entries when needed, otherwise only the metadata suffices
        async def load_fulltree(self):
            if not self._entries_initialized:
                query_tree_statement = select(_TREE_ENTRY, _TREE_ENTRY.node, _TREE_ENTRY.leaf, _TREE_ENTRY.parent) \
                    .outerjoin(NodeAlchemyBaseType, _TREE_ENTRY.node_id == NodeAlchemyBaseType.id)
                if LeafType:
                    leafalias = aliased(LeafType)
                    query_tree_statement = query_tree_statement.outerjoin(leafalias, _TREE_ENTRY.leaf_id == leafalias.id)
                query_tree_statement = query_tree_statement.filter(_TREE_ENTRY.metadata_instance == self.metadata)
                self._entries = (await _TREE_ENTRY.execute_query(query_tree_statement)).scalars().all()
                for entry in self._entries:
                    entry.set_tree(self)
                    self._tree.setdefault(entry.parent_id, []).append(entry)
                    self._tree.setdefault(entry.id, [])
                    self._entries_per_id[entry.id] = entry
                    if entry.is_root():
                        self._roots.append(entry)
                self._entries_initialized = True
            return self._tree

        def check_initialized_entries(self):
            if not self._entries_initialized:
                raise NotInAsyncContextManager('check_initialized_entries', '_TREE')

        def get_roots(self):
            self.check_initialized_entries()
            return self._roots

        @with_current_session_kwargs
        async def get_root(self, create=True, *, session: AsyncSession):
            self.check_initialized_entries()
            if not self._roots and create:
                root = _TREE_ENTRY(metadata_id=self.metadata.id, parent=None)
                session.add(root)
                await commit_and_rollback_if_exception(session)  # populate root.id
                root.set_tree(self)
                self._tree.setdefault(root.id, [])
                self._roots = [root]
            else:
                root = self._roots[0]
            assert len(self._roots) == 1, f"No (or multiple) root detected for tree {self.metadata}"
            return root

        def get_from(self, index):
            self.check_initialized_entries()
            return self._entries_per_id[index]

        @with_current_session_kwargs
        async def add_child(self, node: _TREE_ENTRY, child_node: NodeAlchemyBaseType, *, session: AsyncSession):
            self.check_initialized_entries()
            child = _TREE_ENTRY(metadata_id=self.metadata.id, parent_id=node.id, node=child_node)
            session.add(child)
            await commit_and_rollback_if_exception(session)
            child.set_tree(self)
            self._tree.setdefault(node.id, []).append(child)
            self._tree.setdefault(child.id, [])
            return child

        @with_current_session_kwargs
        async def add_leaf(self, node: _TREE_ENTRY, child_leaf: LeafType, *, session: AsyncSession):
            self.check_initialized_entries()
            child = _TREE_ENTRY(metadata_id=self.metadata.id, parent_id=node.id, leaf=child_leaf)
            session.add(child)
            await commit_and_rollback_if_exception(session)
            child.set_tree(self)
            self._tree.setdefault(node.id, []).append(child)
            self._tree.setdefault(child.id, [])
            return child

        def children_of(self, node: _TREE_ENTRY):
            self.check_initialized_entries()
            return self._tree.get(node.id, [])

        @with_current_session_kwargs
        async def delete_from(self, node: _TREE_ENTRY, allow_root=False, *, session):
            self.check_initialized_entries()
            cur_entries = [node]
            while cur_entries:
                entry = cur_entries.pop(0)
                cur_entries.extend(self._tree[entry.id])
                if not entry.is_root() or allow_root:
                    await entry.delete()
                    del self._tree[entry.id]

            if not node.is_root() or allow_root:
                if node.parent_id in self._tree:
                    self._tree[node.parent_id] = [c for c in self._tree[node.parent_id] if c.id != node.id]
            else:
                self._tree[node.id] = []

        @asynccontextmanager
        async def __asynccontextmanager__(self) -> AsyncIterator[_TREE]:
            if not self.metadata:
                self.metadata = await MetadataType.get_create(**self._metadata_kwargs)
            await self.load_fulltree()
            try:
                prev = current_sqlalchemy_tree.set(self)
                yield self
            finally:
                current_sqlalchemy_tree.reset(prev)
                self._entries_initialized = False

        def __repr__(self):
            roots = self.get_roots()
            return '\n'.join(map(repr, roots))

    return _TREE


current_sqlalchemy_tree = ContextVar('current_sqlalchemy_tree')


if __name__ == "__main__":
    from baseimplems.persistence.sqlalchemy_persist import run_with_temporarily_persistent_mock_db_engine
    from baseimplems.persistence.sqlalchemy_database import run_within_sqlalchemy

    from sqlalchemy import String
    import anyio

    class Test(WithID, *BaseMixins):
        __tablename__ = 'M'
        value: Mapped[str] = mapped_column(String)

    async def main():
        async with (
            run_with_temporarily_persistent_mock_db_engine(),
            run_within_sqlalchemy() as db,
            db.session() as sess,
        ):
            v1 = await Test.get_create(value="bonjour1")
            v2 = await Test.get_create(value="bonjour2")
            v3 = await Test.get_create(value="bonjour3")
            v4 = await Test.get_create(value="bonjour4")

            TREE_TYPE = TREE(Test)
            await db.force_schema_update()
            T = TREE_TYPE(name="firsttree")

            async with T:
                await commit_and_rollback_if_exception(sess)
                root = await T.get_root()
                print(root)

                child = await root.add_child(v1)
                print(child)

                await child.add_leaf(v2)
                child2 = await child.add_child(v3)

                await child2.add_leaf(v4)
                await child2.add_leaf(v1)

                print(child2)

                print(root)

                await T.delete_from(child)
                print(root)

    anyio.run(main)
