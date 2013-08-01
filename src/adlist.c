/* adlist.c - A generic doubly linked list implementation
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include <stdlib.h>
#include "adlist.h"
#include "zmalloc.h"

/* Create a new list. The created list can be freed with
 * AlFreeList(), but private value of every node need to be freed
 * by the user before to call AlFreeList().
 *
 * On error, NULL is returned. Otherwise the pointer to the new list. */
/*
 * 创建一个新的链表
 *
 * 创建成功返回链表，失败返回 NULL 。
 *
 * T = O(1)
 */
list *listCreate(void)
{
    struct list *list;

    // 分配内存
    if ((list = zmalloc(sizeof(*list))) == NULL)
        return NULL;

    // 初始化属性
    list->head = list->tail = NULL;
    list->len = 0;
    list->dup = NULL;
    list->free = NULL;
    list->match = NULL;

    return list;
}

/* Free the whole list.
 *
 * This function can't fail. */
/*
 * 释放整个链表，以及链表中所有节点
 *
 * T = O(N)
 */
void listRelease(list *list)
{
    unsigned long len;
    listNode *current, *next;

    // 指向头指针
    current = list->head;
    // 遍历整个链表
    len = list->len;
    while(len--) {
        next = current->next;

        // 如果有设置值释放函数，那么调用它
        if (list->free) list->free(current->value);

        // 释放节点结构
        zfree(current);

        current = next;
    }

    // 释放链表结构
    zfree(list);
}

/* Add a new node to the list, to head, contaning the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
/*
 * 将一个包含有给定值指针 value 的新节点添加到链表的表头
 *
 * 如果为新节点分配内存出错，那么不执行任何动作，仅返回 NULL
 *
 * 如果执行成功，返回传入的链表指针
 *
 * T = O(1)
 */
list *listAddNodeHead(list *list, void *value)
{
    listNode *node;

    // 为节点分配内存
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    // 保存值指针
    node->value = value;

    // 添加节点到空链表
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    // 添加节点到非空链表
    } else {
        node->prev = NULL;
        node->next = list->head;
        list->head->prev = node;
        list->head = node;
    }

    // 更新链表节点数
    list->len++;

    return list;
}

/* Add a new node to the list, to tail, containing the specified 'value'
 * pointer as value.
 *
 * On error, NULL is returned and no operation is performed (i.e. the
 * list remains unaltered).
 * On success the 'list' pointer you pass to the function is returned. */
/*
 * 将一个包含有给定值指针 value 的新节点添加到链表的表尾
 *
 * 如果为新节点分配内存出错，那么不执行任何动作，仅返回 NULL
 *
 * 如果执行成功，返回传入的链表指针
 *
 * T = O(1)
 */
list *listAddNodeTail(list *list, void *value)
{
    listNode *node;

    // 为新节点分配内存
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    // 保存值指针
    node->value = value;

    // 目标链表为空
    if (list->len == 0) {
        list->head = list->tail = node;
        node->prev = node->next = NULL;
    // 目标链表非空
    } else {
        node->prev = list->tail;
        node->next = NULL;
        list->tail->next = node;
        list->tail = node;
    }

    // 更新链表节点数
    list->len++;

    return list;
}

/*
 * 创建一个包含值 value 的新节点，并将它插入到 old_node 的之前或之后
 *
 * 如果 after 为 0 ，将新节点插入到 old_node 之前。
 * 如果 after 为 1 ，将新节点插入到 old_node 之后。
 *
 * T = O(1)
 */
list *listInsertNode(list *list, listNode *old_node, void *value, int after) {
    listNode *node;

    // 创建新节点
    if ((node = zmalloc(sizeof(*node))) == NULL)
        return NULL;

    // 保存值
    node->value = value;

    // 将新节点添加到给定节点之后
    if (after) {
        node->prev = old_node;
        node->next = old_node->next;
        // 给定节点是原表尾节点
        if (list->tail == old_node) {
            list->tail = node;
        }
    // 将新节点添加到给定节点之前
    } else {
        node->next = old_node;
        node->prev = old_node->prev;
        // 给定节点是原表头节点
        if (list->head == old_node) {
            list->head = node;
        }
    }

    // 更新新节点的前置指针
    if (node->prev != NULL) {
        node->prev->next = node;
    }
    // 更新新节点的后置指针
    if (node->next != NULL) {
        node->next->prev = node;
    }

    // 更新链表节点数
    list->len++;

    return list;
}

/* Remove the specified node from the specified list.
 * It's up to the caller to free the private value of the node.
 *
 * This function can't fail. */
/*
 * 从链表 list 中删除给定节点 node 
 * 
 * 对节点私有值(private value of the node)的释放工作由调用者进行。
 *
 * T = O(1)
 */
void listDelNode(list *list, listNode *node)
{
    // 调整前置节点的指针
    if (node->prev)
        node->prev->next = node->next;
    else
        list->head = node->next;

    // 调整后置节点的指针
    if (node->next)
        node->next->prev = node->prev;
    else
        list->tail = node->prev;

    // 释放值
    if (list->free) list->free(node->value);

    // 释放节点
    zfree(node);

    // 链表数减一
    list->len--;
}

/* Returns a list iterator 'iter'. After the initialization every
 * call to listNext() will return the next element of the list.
 *
 * This function can't fail. */
/*
 * 为给定链表创建一个迭代器，
 * 之后每次对这个迭代器调用 listNext 都返回被迭代到的链表节点
 *
 * direction 参数决定了迭代器的迭代方向：
 *  AL_START_HEAD ：从表头向表尾迭代
 *  AL_START_TAIL ：从表尾想表头迭代
 *
 * T = O(1)
 */
listIter *listGetIterator(list *list, int direction)
{
    // 为迭代器分配内存
    listIter *iter;
    if ((iter = zmalloc(sizeof(*iter))) == NULL) return NULL;

    // 根据迭代方向，设置迭代器的起始节点
    if (direction == AL_START_HEAD)
        iter->next = list->head;
    else
        iter->next = list->tail;

    // 记录迭代方向
    iter->direction = direction;

    return iter;
}

/* Release the iterator memory */
/*
 * 释放迭代器
 *
 * T = O(1)
 */
void listReleaseIterator(listIter *iter) {
    zfree(iter);
}

/* Create an iterator in the list private iterator structure */
/*
 * 将迭代器的方向设置为 AL_START_HEAD ，
 * 并将迭代指针重新指向表头节点。
 *
 * T = O(1)
 */
void listRewind(list *list, listIter *li) {
    li->next = list->head;
    li->direction = AL_START_HEAD;
}

/*
 * 将迭代器的方向设置为 AL_START_TAIL ，
 * 并将迭代指针重新指向表尾节点。
 *
 * T = O(1)
 */
void listRewindTail(list *list, listIter *li) {
    li->next = list->tail;
    li->direction = AL_START_TAIL;
}

/* Return the next element of an iterator.
 * It's valid to remove the currently returned element using
 * listDelNode(), but not to remove other elements.
 *
 * The function returns a pointer to the next element of the list,
 * or NULL if there are no more elements, so the classical usage patter
 * is:
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 * */
/*
 * 返回迭代器当前所指向的节点。
 *
 * 删除当前节点是允许的，但不能修改链表里的其他节点。
 *
 * 函数要么返回一个节点，要么返回 NULL ，常见的用法是：
 *
 * iter = listGetIterator(list,<direction>);
 * while ((node = listNext(iter)) != NULL) {
 *     doSomethingWith(listNodeValue(node));
 * }
 *
 * T = O(1)
 */
listNode *listNext(listIter *iter)
{
    listNode *current = iter->next;

    if (current != NULL) {
        // 根据方向选择下一个节点
        if (iter->direction == AL_START_HEAD)
            // 保存下一个节点，防止当前节点被删除而造成指针丢失
            iter->next = current->next;
        else
            // 保存下一个节点，防止当前节点被删除而造成指针丢失
            iter->next = current->prev;
    }

    return current;
}

/* Duplicate the whole list. On out of memory NULL is returned.
 * On success a copy of the original list is returned.
 *
 * The 'Dup' method set with listSetDupMethod() function is used
 * to copy the node value. Otherwise the same pointer value of
 * the original node is used as value of the copied node.
 *
 * The original list both on success or error is never modified. */
/*
 * 复制整个链表。
 *
 * 复制成功返回输入链表的副本，
 * 如果因为内存不足而造成复制失败，返回 NULL 。
 *
 * 如果链表有设置值复制函数 dup ，那么对值的复制将使用复制函数进行，
 * 否则，新节点将和旧节点共享同一个指针。
 *
 * 无论复制是成功还是失败，输入节点都不会修改。
 *
 * T = O(N)
 */
list *listDup(list *orig)
{
    list *copy;
    listIter *iter;
    listNode *node;

    // 创建新链表
    if ((copy = listCreate()) == NULL)
        return NULL;

    // 设置节点值处理函数
    copy->dup = orig->dup;
    copy->free = orig->free;
    copy->match = orig->match;

    // 迭代整个输入链表
    iter = listGetIterator(orig, AL_START_HEAD);
    while((node = listNext(iter)) != NULL) {
        void *value;

        // 复制节点值到新节点
        if (copy->dup) {
            value = copy->dup(node->value);
            if (value == NULL) {
                listRelease(copy);
                listReleaseIterator(iter);
                return NULL;
            }
        } else
            value = node->value;

        // 将节点添加到链表
        if (listAddNodeTail(copy, value) == NULL) {
            listRelease(copy);
            listReleaseIterator(iter);
            return NULL;
        }
    }

    // 释放迭代器
    listReleaseIterator(iter);

    // 返回副本
    return copy;
}

/* Search the list for a node matching a given key.
 * The match is performed using the 'match' method
 * set with listSetMatchMethod(). If no 'match' method
 * is set, the 'value' pointer of every node is directly
 * compared with the 'key' pointer.
 *
 * On success the first matching node pointer is returned
 * (search starts from head). If no matching node exists
 * NULL is returned. */
/* 
 * 查找链表 list 中值和 key 匹配的节点。
 * 
 * 对比操作由链表的 match 函数负责进行，
 * 如果没有设置 match 函数，
 * 那么直接通过对比值的指针来决定是否匹配。
 *
 * 如果匹配成功，那么第一个匹配的节点会被返回。
 * 如果没有匹配任何节点，那么返回 NULL 。
 *
 * T = O(N)
 */
listNode *listSearchKey(list *list, void *key)
{
    listIter *iter;
    listNode *node;

    // 迭代整个链表
    iter = listGetIterator(list, AL_START_HEAD);
    while((node = listNext(iter)) != NULL) {
        
        // 对比
        if (list->match) {
            if (list->match(node->value, key)) {
                listReleaseIterator(iter);
                // 找到
                return node;
            }
        } else {
            if (key == node->value) {
                listReleaseIterator(iter);
                // 找到
                return node;
            }
        }
    }
    
    listReleaseIterator(iter);

    // 未找到
    return NULL;
}

/* Return the element at the specified zero-based index
 * where 0 is the head, 1 is the element next to head
 * and so on. Negative integers are used in order to count
 * from the tail, -1 is the last element, -2 the penultimate
 * and so on. If the index is out of range NULL is returned. */
/*
 * 返回链表在给定索引上的值。
 *
 * 索引以 0 为起始，也可以是负数， -1 表示链表最后一个节点，诸如此类。
 *
 * 如果索引超出范围（out of range），返回 NULL 。
 *
 * T = O(N)
 */
listNode *listIndex(list *list, long index) {
    listNode *n;

    // 如果索引为负数，从表尾开始查找
    if (index < 0) {
        index = (-index)-1;
        n = list->tail;
        while(index-- && n) n = n->prev;
    // 如果索引为正数，从表头开始查找
    } else {
        n = list->head;
        while(index-- && n) n = n->next;
    }

    return n;
}

/* Rotate the list removing the tail node and inserting it to the head. */
/*
 * 取出链表的表尾节点，并将它移动到表头，成为新的表头节点。
 *
 * T = O(1)
 */
void listRotate(list *list) {
    listNode *tail = list->tail;

    if (listLength(list) <= 1) return;

    /* Detach current tail */
    // 取出表尾节点
    list->tail = tail->prev;
    list->tail->next = NULL;

    /* Move it as head */
    // 插入到表头
    list->head->prev = tail;
    tail->prev = NULL;
    tail->next = list->head;
    list->head = tail;
}
