import math

class Node:
    def __init__(self, order):
        self.order = order
        self.key = []
        self.RID = []  #insert whatever values needed to be found here
        self.nextRID = None
        self.parent = None
        self.leaf_checker = False 

    # Inserting Node at the Leaf
    # Have to change this depending on whether or not keeping RID
    def inserting_in_leaf(self, leaf, key, RID):
        if (self.key):
            keyList = self.key
            for i in range(len(keyList)):
                if (key == keyList[i]):
                    self.RID[i].append(RID)
                    break
                elif (key < keyList[i]):
                    self.key = self.key[:i] + [key] + self.key[i:]
                    self.RID = self.RID[:i] + [[RID]] + self.RID[i:]
                    break
                elif (i + 1 == len(keyList)):
                    self.key.append(key)
                    self.RID.append([RID])
                    break
        else:
            self.key = [key]
            self.RID = [[RID]]


# initializing the b plus tree
class bPlusTree:
    def __init__(self, order):
        self.root = Node(order)
        self.root.leaf_checker = True

    # Insert
    def insert(self, key, RID):
        key = str(key)
        old_node = self.search(key)
        old_node.inserting_in_leaf(old_node, key, RID)

        if (len(old_node.key) == old_node.order):
            node1 = Node(old_node.order)
            node1.leaf_checker = True
            node1.parent = old_node.parent
            mid = int(math.ceil(old_node.order / 2)) - 1
            node1.key = old_node.key[mid + 1:]
            node1.RID = old_node.RID[mid + 1:]
            node1.nextRID = old_node.nextRID
            old_node.key = old_node.key[:mid + 1]
            old_node.RID = old_node.RID[:mid + 1]
            old_node.nextRID = node1
            self.insert_in_parent(old_node, node1.key[0], node1)

    # Search operation for different operations
    def search(self, key):
        current_node = self.root
        while(current_node.leaf_checker == False):
            temp2 = current_node.key
            for i in range(len(temp2)):
                if (key == temp2[i]):
                    current_node = current_node.RID[i + 1]
                    break
                elif (int(key) < int(temp2[i])):
                    current_node = current_node.RID[i]
                    break
                elif (i + 1 == len(current_node.key)):
                    current_node = current_node.RID[i + 1]
                    break
        return current_node

    #Search that returns RID
    def searchRID(self, key):
        current_node = self.root
        while(current_node.leaf_checker == False):
            temp2 = current_node.key
            for i in range(len(temp2)):
                if (key == temp2[i]):
                    current_node = current_node.RID[i + 1]
                    break
                elif (key < int(temp2[i])):
                    current_node = current_node.RID[i]
                    break
                elif (i + 1 == len(current_node.key)):
                    current_node = current_node.RID[i + 1]
                    break
        return current_node.RID

    # Find the node
    def find(self, key, RID):
        l = self.search(key)
        for i, item in enumerate(l.key):
            if item == key:
                if RID in l.RID[i]:
                    return True
                else:
                    return False
        return False

    # Inserting at the parent
    def insert_in_parent(self, n, key, ndash):
        if (self.root == n):
            rootNode = Node(n.order)
            rootNode.key = [key]
            rootNode.RID = [n, ndash]
            self.root = rootNode
            n.parent = rootNode
            ndash.parent = rootNode
            return

        parentNode = n.parent
        temp3 = parentNode.RID
        for i in range(len(temp3)):
            if (temp3[i] == n):
                parentNode.key = parentNode.key[:i] + \
                    [key] + parentNode.key[i:]
                parentNode.RID = parentNode.RID[:i +1] + [ndash] + parentNode.RID[i + 1:]
                if (len(parentNode.RID) > parentNode.order):
                    parentdash = Node(parentNode.order)
                    parentdash.parent = parentNode.parent
                    mid = int(math.ceil(parentNode.order / 2)) - 1
                    parentdash.key = parentNode.key[mid + 1:]
                    parentdash.RID = parentNode.RID[mid + 1:]
                    key_ = parentNode.key[mid]
                    if (mid == 0):
                        parentNode.key = parentNode.key[:mid + 1]
                    else:
                        parentNode.key = parentNode.key[:mid]
                    parentNode.RID = parentNode.RID[:mid + 1]
                    for j in parentNode.RID:
                        j.parent = parentNode
                    for j in parentdash.RID:
                        j.parent = parentdash
                    self.insert_in_parent(parentNode, key_, parentdash)

    # Delete a node
    def delete(self, key, RID):
        node_ = self.search(key)

        temp = 0
        for i, item in enumerate(node_.key):
            #print(item)
            #print(key)
            #print(" ")
            if int(item) == int(key):
                temp = 1
                #print("151")
                if RID in node_.RID[i]:
                    if len(node_.RID[i]) > 1:
                        node_.RID[i].pop(node_.RID[i].index(RID))
                        #print("deleted")
                    elif node_ == self.root:
                        node_.key.pop(i)
                        node_.RID.pop(i)
                        #print("deleted")
                    else:
                        node_.RID[i].pop(node_.RID[i].index(RID))
                        del node_.RID[i]
                        node_.key.pop(node_.key.index(str(key)))
                        self.deleteEntry(node_, key, RID)
                        #print("deleted")
                else:
                    print("key not in RID")
                    return
        if temp == 0:
            print("key not in Tree")
            return

    # Delete an entry
    def deleteEntry(self, node_, key, RID):
        
        if not node_.leaf_checker:
            for i, item in enumerate(node_.RID):
                if item == RID:
                    node_.RID.pop(i)
                    break
            for i, item in enumerate(node_.key):
                if item == key:
                    node_.key.pop(i)
                    break

        if self.root == node_ and len(node_.RID) == 1:
            self.root = node_.RID[0]
            node_.RID[0].parent = None
            del node_
            return
        elif (len(node_.RID) < int(math.ceil(node_.order / 2)) and node_.leaf_checker == False) or (len(node_.key) < int(math.ceil((node_.order - 1) / 2)) and node_.leaf_checker == True):

            is_predecessor = 0
            parentNode = node_.parent
            PrevNode = -1
            NextNode = -1
            PrevK = -1
            PostK = -1
            for i, item in enumerate(parentNode.RID):

                if item == node_:
                    if i > 0:
                        PrevNode = parentNode.RID[i - 1]
                        PrevK = parentNode.key[i - 1]

                    if i < len(parentNode.RID) - 1:
                        NextNode = parentNode.RID[i + 1]
                        PostK = parentNode.key[i]

            if PrevNode == -1:
                ndash = NextNode
                key_ = PostK
            elif NextNode == -1:
                is_predecessor = 1
                ndash = PrevNode
                key_ = PrevK
            else:
                if len(node_.key) + len(NextNode.key) < node_.order:
                    ndash = NextNode
                    key_ = PostK
                else:
                    is_predecessor = 1
                    ndash = PrevNode
                    key_ = PrevK

            if len(node_.key) + len(ndash.key) < node_.order:
                if is_predecessor == 0:
                    node_, ndash = ndash, node_
                ndash.RID += node_.RID
                if not node_.leaf_checker:
                    ndash.key.append(key_)
                else:
                    ndash.nextRID = node_.nextRID
                ndash.key += node_.key

                if not ndash.leaf_checker:
                    for j in ndash.RID:
                        j.parent = ndash

                self.deleteEntry(node_.parent, key_, node_)
                del node_
            else:
                if is_predecessor == 1:
                    if not node_.leaf_checker:
                        ndashpm = ndash.RID.pop(-1)
                        ndashkm_1 = ndash.key.pop(-1)
                        node_.RID = [ndashpm] + node_.RID
                        node_.key = [key_] + node_.key
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.key):
                            if item == key_:
                                parentNode.key[i] = ndashkm_1  #was just p.key[i]   i think it needs to be parentNode.key[i]
                                break
                    else:
                        ndashpm = ndash.RID.pop(-1)
                        ndashkm = ndash.key.pop(-1)
                        node_.RID = [ndashpm] + node_.RID
                        node_.key = [ndashkm] + node_.key
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.key):  #was also just p for no reason. changed to parentNode
                            if item == key_:
                                parentNode.key[i] = ndashkm
                                break
                else:
                    if not node_.leaf_checker:
                        ndashp0 = ndash.RID.pop(0)
                        ndashk0 = ndash.key.pop(0)
                        node_.RID = node_.RID + [ndashp0]
                        node_.key = node_.key + [key_]
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.key):
                            if item == key_:
                                parentNode.key[i] = ndashk0
                                break
                    else:
                        ndashp0 = ndash.RID.pop(0)
                        ndashk0 = ndash.key.pop(0)
                        node_.RID = node_.RID + [ndashp0]
                        node_.key = node_.key + [ndashk0]
                        parentNode = node_.parent
                        for i, item in enumerate(parentNode.key):
                            if item == key_:
                                parentNode.key[i] = ndash.key[0]
                                break

                if not ndash.leaf_checker:
                    for j in ndash.RID:
                        j.parent = ndash
                if not node_.leaf_checker:
                    for j in node_.RID:
                        j.parent = node_
                if not parentNode.leaf_checker:
                    for j in parentNode.RID:
                        j.parent = parentNode
