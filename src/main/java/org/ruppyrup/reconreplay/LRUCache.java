package org.ruppyrup.reconreplay;

import java.util.HashMap;
import java.util.LinkedList;

public class LRUCache {
    // Hash map to store the cache data
    // key as Integer and Value as String
    HashMap<Integer, Integer> data = new HashMap<>();
    // Linked list to store the order of cache access
    LinkedList<Integer> order = new LinkedList<>();
    // size of the cache
    int capacity;

    LRUCache(int capacity) {
        this.capacity = capacity; // assigning the size of the cache while creating
    }

    // put - for adding new cache
    synchronized void put(int key) {

        // Check if the cache is full by comparing the size of the list with capacity
        if (order.size() >= capacity) {
            // if the cache is full, then remove the last element from the order list and
            // also from the data
            // so we will get room for new cache
            System.out.println("LRU Cache is full when adding key :: " + key);
            int keyRemoved = order.removeLast(); // remove from order
            data.remove(keyRemoved);// remove from data
        }

        // add the new cache to top of the list and also to data
        order.addFirst(key);// add to top of the list
        data.put(key, key);// add to data

    }

    // get
    synchronized Integer get(int key) {
        Integer res = data.get(key); //get the value from map using the key
        if(res != null) {
            //if the data is present then we need to update the access order
            //move the current key to top of the access list
            //to move to top of the list first remove it and re-add it
            order.remove((Object) key);
            order.addFirst(key);
        }else {
            //if value is not present in the cache then its a cache miss
            //and we will return null
            res = null;
        }

        return res;

    }

    // display method
    public void display() {

        for (int i = 0; i < order.size(); i++) {
            int key = order.get(i);
            System.out.println(key + " => " + data.get(key));
        }
    }

    public static void main(String[] args) {
        LRUCache cache = new LRUCache(3);
        cache.put(1);
        cache.put(2);
        cache.put(3);
        cache.put(4); // putting new cache when full. 1 will be removed
        cache.get(3); // accessing 3, it will be moved to top
        cache.get(2);// accessing 2, it will be moved to top
        cache.put(1); // putting new cache when full. 4 will be removed
        cache.get(3);// accessing 3, it will be moved to top
        cache.get(1);// accessing 1, it will be moved to top
        cache.display(); // 1 should be on top
        /*
         * output
         * 1 => one
         * 3 => three
         * 2 => two
         *
         **/
    }

}
