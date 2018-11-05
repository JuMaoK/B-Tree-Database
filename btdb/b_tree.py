import pickle
import bisect
import math


from btdb.logical import LogicalBase, ValueRef


class BTreeNode:
    # TODO: __slots__

    def __init__(self, keys=None, val_refs=None, child=None):
        # keys' list
        self.keys = [] if keys is None else keys
        # value referents' list
        self.val_refs = [] if val_refs is None else val_refs
        # children's list
        self.child = [None] + [None] * \
            len(self.keys) if child is None else child

    def store_refs(self, storage):
        # store the value ref
        for vr in self.val_refs:
            vr.store(storage)
        # store the node ref in child list
        for c in self.child:
            if c:
                c.store(storage)


class BTreeNodeRef(ValueRef):

    def prepare_to_store(self, storage):
        # node ref would serialise children and valrefs before serialising itself.
        if self._referent:
            self._referent.store_refs(storage)

    @staticmethod
    def referent_to_string(referent):
        return pickle.dumps({
            'keys': referent.keys,
            'val_refs': referent.val_refs,
            'child': [c.address if c else c for c in referent.child]
        })

    @staticmethod
    def string_to_referent(string):
        d = pickle.loads(string)
        return BTreeNode(d['keys'], d['val_refs'],
                         [BTreeNodeRef(address=i) if i else None for i in d['child']])


class BTree(LogicalBase):
    # order of B-Tree
    _order = 256
    node_ref_class = BTreeNodeRef

    def search(self, node, key):
        '''traversing the tree searching for the giving key. returning:
        1. whether the key was found.
        2. the last node it searched.
        3. the position of the key should be found.
        4. every nodes were searched.
        '''

        # a stack recording parents and root.
        _hot = []

        while node is not None:
            if key in node.keys:
                target = node.keys.index(key)
                return (True, node, target, _hot)  
            pos = bisect.bisect_left(node.keys, key)
            if node.child[pos] is not None:
                _hot.append(node)
                node = self._follow(node.child[pos])
            else:
                return (False, node, pos, _hot)

    def _get(self, node, key):
        res = self.search(node, key)
        if res[0]:
            return self._follow(res[1].val_refs[res[2]])
        else:
            raise KeyError

    def ins_and_split(self, target, key, value_ref, _hot, child_pair=None):
        '''insert and split to solve overflow if needed'''

        bisect.insort(target.keys, key)
        pos = target.keys.index(key)
        target.val_refs.insert(pos, value_ref)
        if not child_pair:
            target.child.append(None)
        else:
            target.child[pos:pos + 1] = child_pair

        if len(target.child) <= self._order:
            if _hot:
                return _hot[0]
            else:
                return target
        else:
            # overflow
            if _hot:
                par = _hot.pop()
            else:
                par = BTreeNode()

            mid = len(target.keys) // 2
            left = self.node_ref_class(BTreeNode(
                target.keys[:mid], target.val_refs[:mid], target.child[:mid + 1]))
            right = self.node_ref_class(BTreeNode(
                target.keys[mid + 1:], target.val_refs[mid + 1:], target.child[mid + 1:]))
            child_pair = [left, right]
            return self.ins_and_split(par, target.keys[mid], target.val_refs[mid], _hot, child_pair)

    def _insert(self, node, key, value_ref):
        if node is None:
            node = BTreeNode()

        res = self.search(node, key)
        if res[0]:
            res[1].val_refs[res[2]] = value_ref
            if res[3]:
                root = res[3][0]
            else:
                root = node
        else:
            root = self.ins_and_split(res[1], key, value_ref, res[3])
        return self.node_ref_class(root)

    def succ(self, node, key):
        '''to find the leftmost successor of the giving node.'''
        if node.child[0] is None:
            # leaf has no successor
            raise KeyError
        pos = node.keys.index(key)
        node = self._follow(node.child[pos + 1])
        while node.child[0] is not None:
            node = self._follow(node.child[0])
        return node

    def check_sibling(self, parent, child):
        '''to check whether sibling(left & right) of the giving child exists.'''

        pos = [c._referent for c in parent.child].index(child)
        left = parent.child[pos - 1:pos]
        right = parent.child[pos + 1:pos + 2]
        return left, right, pos

    def rotate(self, target, sib, par, hot, pos, l_or_r='left'):
        if l_or_r == 'left':
            target.keys.insert(0, par.keys[pos - 1])
            target.val_refs.insert(0, par.val_refs[pos - 1])
            target.child.insert(0, sib.child.pop())
            par.keys[pos - 1] = sib.keys.pop()
            par.val_refs[pos - 1] = sib.val_refs.pop()

        elif l_or_r == 'right':
            target.keys.append(par.keys[pos])
            target.val_refs.append(par.val_refs[pos])
            target.child.append(sib.child.pop(0))
            par.keys[pos] = sib.keys.pop(0)
            par.val_refs[pos] = sib.val_refs.pop(0)
        if not hot:
            return par
        else:
            return hot[0]

    def merge(self, target, sib, par, hot, pos, l_or_r='left'):
        if l_or_r == 'left':
            sib.keys += [par.keys.pop(pos - 1)] + target.keys
            sib.val_refs += [par.val_refs.pop(pos - 1)] + target.val_refs
            sib.child += target.child
            par.child.pop(pos)
        if l_or_r == 'right':
            target.keys += [par.keys.pop(pos)] + sib.keys
            target.val_refs += [par.val_refs.pop(pos)] + sib.val_refs
            target.child += sib.child
            par.child.pop(pos + 1)
        if hot:
            return self.solveUnderflow(par, hot)
        elif par.keys:
            return par
        elif l_or_r == 'left':
            return sib
        else:
            return target

    def solveUnderflow(self, target, hot):
        if not hot:
            return target
        elif len(target.keys) > math.ceil(self._order / 2) - 2:
            return hot[0]

        par = hot.pop()
        left, right, pos = self.check_sibling(par, target)
        # rotate
        if left and len(self._follow(left[0]).keys) > math.ceil(self._order / 2) - 1:
            return self.rotate(target, self._follow(left[0]), par, hot, pos, l_or_r='left')
        elif right and len(self._follow(right[0]).keys) > math.ceil(self._order / 2) - 1:
            return self.rotate(target, self._follow(right[0]), par, hot, pos, l_or_r='right')
        # merge
        else:
            if left:
                return self.merge(target, self._follow(left[0]), par, hot, pos, l_or_r='left')
            elif right:
                return self.merge(target, self._follow(right[0]), par, hot, pos, l_or_r='right')

    def _delete(self, node, key):
        res = self.search(node, key)
        if not res[0]:
            print("key to be del not found")
            raise KeyError
        target, pos, hot = res[1:]
        # key was found at leaf node
        if target.child[0] is None:
            target.keys.pop(pos)
            target.val_refs.pop(pos)
            target.child.pop()
            new_node = self.solveUnderflow(target, hot)
            return self.node_ref_class(referent=new_node)
        # key was found at internal node
        else:
            successor = self.succ(target, key)
            hot = self.search(node, successor.keys[0])[3]
            target.keys[pos] = successor.keys.pop(0)
            target.val_refs[pos] = successor.val_refs.pop(0)
            successor.child.pop()
            new_node = self.solveUnderflow(successor, hot)
            return self.node_ref_class(referent=new_node)
