# coding=utf8
"""
测试不同的设计思路
---
I. hash-ring: 假设data均匀分配到各个node中, 求增加一个node需要移动的data个数
"""
from bisect import bisect_left
from hashlib import md5
from struct import unpack_from

NODE_COUNT = 10
NEW_NODE_COUNT = NODE_COUNT + 1
DATA_ID_COUNT = 10000
VNODE_COUNT = 100


def node():
    moved_ids = 0
    for data_id in range(DATA_ID_COUNT):
        hsh = id_hash(data_id)
        node_id = hsh % NODE_COUNT
        new_node_id = hsh % NEW_NODE_COUNT
        if node_id != new_node_id:
            moved_ids += 1
    percent_moved = 100.0 * moved_ids / DATA_ID_COUNT
    print('plain node model: %d ids moved, %.02f%%' % (moved_ids, percent_moved))


def node_ring():
    node_range_starts = [DATA_ID_COUNT / NODE_COUNT * node_id for node_id in range(NODE_COUNT)]
    new_node_range_starts = [DATA_ID_COUNT / NEW_NODE_COUNT * new_node_id for new_node_id in range(NEW_NODE_COUNT)]
    moved_ids = 0
    for data_id in range(DATA_ID_COUNT):
        hsh = id_hash(data_id)
        node_id = bisect_left(node_range_starts, hsh % DATA_ID_COUNT) % NODE_COUNT
        new_node_id = bisect_left(new_node_range_starts, hsh % DATA_ID_COUNT) % NEW_NODE_COUNT
        if node_id != new_node_id:
            moved_ids += 1
    percent_moved = 100.0 * moved_ids / DATA_ID_COUNT
    print('node-ring model: %d ids moved, %.02f%%' % (moved_ids, percent_moved))


def vnode_ring():
    """
    虚节点个数在集群的整个生命周期中是不会变化的，它与数据项的映射关系不会发生变化，改变的仅是vnode与node的映射关系
    """
    vnode2node = []
    for vnode_id in range(VNODE_COUNT):
        vnode2node.append(vnode_id % NODE_COUNT)
    new_vnode2node = list(vnode2node)
    new_node_id = NODE_COUNT
    vnodes_to_assign = VNODE_COUNT / NEW_NODE_COUNT
    while vnodes_to_assign > 0:
        for node_to_take_from in range(NODE_COUNT):
            for vnode_id, node_id in enumerate(vnode2node):
                if node_id == node_to_take_from:
                    vnode2node[vnode_id] = new_node_id
                    vnodes_to_assign -= 1
                    if vnodes_to_assign <= 0:
                        break
            if vnodes_to_assign <= 0:
                break
    moved_id = 0
    for data_id in range(DATA_ID_COUNT):
        hsh = id_hash(data_id)
        vnode_id = hsh % VNODE_COUNT  # (1)
        node_id = vnode2node[vnode_id]
        new_node_id = new_vnode2node[vnode_id]
        if node_id != new_node_id:
            moved_id += 1
    percent_moved = 100.0 * moved_id / DATA_ID_COUNT
    print('fixed node-ring model: %d ids moved, %.02f%%' % (moved_id, percent_moved))


def id_hash(data_id):
    """
    整数hash值
    """
    return unpack_from('>I', md5(str(data_id).encode('utf8')).digest())[0]


if __name__ == '__main__':
    # hash-ring advantage
    node()
    node_ring()
    vnode_ring()
